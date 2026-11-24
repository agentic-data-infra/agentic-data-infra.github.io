---
layout: post
title: "Stoke: A Distributed Dataloader for PyTorch"
author: "Patrick Stuedi"
date: 2026-11-24 00:00:00 -0800
description: "Stoke streams training batches to PyTorch from a running Ember job: preprocessing gets a full DataFrame API, distributed execution and fault tolerance from Ember, and the dataloader itself is just a batch-aware sink and a queue."
image: /assets/images/social/introducing-stoke.png
---

In our [last post]({{ '/blog/2026/11/17/introducing-ember/' | relative_url }})
we introduced Ember, the distributed data processing framework at the center of our
data layer. This post is about
[Stoke](https://github.com/agentic-data-infra/stoke), a distributed dataloader
for PyTorch. Stoke runs data preprocessing as an Ember job and streams its
output to the trainers while the job is still running. We explain when that
kind of online preprocessing makes sense, what a dataloader for it needs, and
why we built it on top of a data processing framework instead of next to
one.

## Why preprocess online, and why it matters for agentic workflows

The common way to prepare training data is offline: a batch job filters,
tokenizes and packs a corpus once, writes the result to storage, and every
training run reads the same files. That works well when many runs share one
fixed dataset.

It works less well when they don't. Some situations call for preprocessing
as part of data loading, while the data is fed to the trainers or the model:

- **Many jobs need slightly different pieces of the data.** A quality
  threshold, a language filter or a mixture weight changes from one run to the
  next. Materializing every variant costs a full pass and a full copy each
  time.
- **Jobs apply different custom transformations.** Tokenizers, packing
  schemes and augmentations differ between models and experiments.
- **Data is joined with other sources.** Quality scores, labels or embeddings
  computed by an earlier job are joined with the corpus before training.

Batch inference has the same shape: a model consumes a stream of inputs that
are selected, joined and transformed on the way in.

Iterative agentic workflows are a prime example. In an exploration loop many
concurrent jobs analyze the same or related datasets in different ways, and
many of the hypotheses being tested *are* changes to the data. Preprocessing
online lets each candidate train on its own variant without first writing a
copy of the dataset.

## What a dataloader for online preprocessing needs

Once preprocessing moves into the data path, the dataloader has to do the
job of a data processing system. We think it needs at least these five properties:

1. **An expressive API.** Filters, joins, user-defined functions, model
   calls: the API should allow arbitrary transformations of the data, not a
   fixed menu.
2. **Performance.** Loading and preprocessing must never stall the trainers'
   GPUs. An idle accelerator is the most expensive resource in the job.
3. **Distributed execution.** Preprocessing has to scale out, both to feed
   distributed training and to keep up when a single host's CPUs cannot
   produce batches as fast as the trainers consume them.
4. **Fault tolerance.** The crash of one preprocessing worker must not fail
   the training job.
5. **Continuous streaming.** Batches of a fixed size flow to the trainers as
   they are needed, while preprocessing is still running, at the trainers'
   pace.

## Dataloaders are data processing frameworks in disguise

Most dataloaders are built as standalone frameworks or libraries. Yet the
first four properties are core parts of every data processing framework: a
transformation API, efficient parallel execution, distribution across
machines, and task retries and recovery.

Because they stand apart, dataloaders either implement those pieces from
scratch or fall short on some of them. Popular loaders such as PyTorch's
DataLoader, Grain and Mosaic Streaming run preprocessing on the trainer's own
host, in worker processes, with transformations written as Python `map` and
`filter` functions. That is flexible for per-record transforms, but a join
with another dataset is hard to express, and preprocessing can never use more
CPUs than the trainer host has. Disaggregated services such as
tf.data service and Meta's DPP move preprocessing to separate machines, and
had to build their own dispatching, scaling and recovery to do it.

The fifth property is different. Data processing frameworks are built to
write results to storage, not to feed a running consumer, and streaming to
trainers adds constraints they don't handle:

- **Fixed batch sizes.** Trainers consume tensors of a fixed shape, for
  example 32 sequences of 4,096 tokens. A framework produces rows in
  whatever partitions and vectors its operators happen to emit. Something has
  to pack rows into exact batches.
- **Backpressure from the trainers.** Trainers set the pace, and it changes
  with the model, the step time and restarts. When they fall behind,
  producers have to slow down and memory has to stay bounded, without failing
  the job. A framework's sink writes as fast as it can.
- **Delivery while the job runs.** A framework's output becomes visible when
  the job commits. Trainers need each batch as soon as it is ready.
- **Many independent consumers.** Each batch should go to exactly one of
  hundreds or thousands of ranks, which start, restart and stop on their own
  schedule, and a retried producer task must not deliver its batches twice.

## Stoke: a dataloader on top of a data processing framework

Stoke was designed from day one together with Ember. It takes the first four
properties from Ember: the DataFrame API, native execution, distributed
execution and fault tolerance, plus the RDMA integration through Kiln. It
then adds the fifth, streaming fixed-size batches to trainers under their
backpressure, with three pieces:

- **A batch-aware sink**, a Stoke connector in Ember, that packs rows into
  fixed-shape training batches.
- **Kiln chunks** that hold the batches, so batch bytes move only between
  producers, Kiln and trainers.
- **Stoke shards**, a small messaging service that queues chunk ids and
  makes producers wait when trainers fall behind.

The core insight is that this is all a distributed dataloader needs on top of
a data processing framework. A Stoke feed is an ordinary Ember program whose
last step writes to a Stoke stream instead of a table:

```python
from ember import Session
from ember.functions import col

ember = Session.builder.master("ember://driver:7077").getOrCreate()
docs = ember.read.parquet("corpus/fineweb-edu/")
scores = ember.read.parquet("runs/r42/quality-scores/")  # from an earlier job

(docs.join(scores, on="doc_id")
     .filter((col("quality") > 0.7) & (col("language") == "en"))
     .select("text")
     .write.format("stoke")
     .option("batch", 32).option("sequence", 4096)
     .save("r42-train"))
```

The sink packs rows into int32 tensors of 32 sequences with 4,096 tokens
each, and writes them into immutable chunks of tens of megabytes in
[Kiln](https://github.com/agentic-data-infra/kiln), the store we use for
shuffle data. When a task commits, the sink hands each chunk's id, 16 bytes,
to a Stoke shard. On the training side, each rank runs the Stoke loader, a
PyTorch `IterableDataset`:

```python
import stoke

dataset = stoke.StokeDataset("r42-train", job=f"r42-rank{rank}")
for batch in dataset:   # int32 [32, 4097] tensors, zero-copy
    ...                 # a training step
```

The loader takes chunk ids from any shard, reads each chunk from Kiln in one
read, and hands out its batches as tensors without copying them. Batch bytes
never pass through the shards, and the trainer never talks to Ember. Ember
knows only a stream name, and the trainer knows only the loader.

<figure>
  <img src="{{ '/assets/images/posts/stoke-architecture.svg' | relative_url }}" alt="Stoke's architecture: Ember workers run the preprocessing program and write batches into chunks in Kiln, passing chunk ids to Stoke shards; each trainer rank runs a Stoke loader that takes chunk ids from the shards and reads the chunks from Kiln. Shards make producers wait when the trainers fall behind.">
  <figcaption>Figure 1. Stoke's data path. Ember workers write batches into Kiln chunks and pass only chunk ids (16 bytes) to the Stoke shards. Each rank's loader takes ids and reads whole chunks from Kiln. Blue boxes are our systems.</figcaption>
</figure>

## What comes from Ember, and what Stoke adds

From Ember, unchanged:

- **Expressive API.** A feed is any Ember DataFrame program: joins, filters,
  SQL expressions, user-defined functions, and model calls with
  `ai.generate`. Mixing sources is a query too, for example a sampled
  `unionAll` of two corpora.
- **Performance.** Preprocessing runs in Ember's native C++ engine, on
  Velox's vectorized operators across all cores, with no per-record Python.
  The trainer host does no preprocessing at all.
- **Distributed execution.** One Ember job, on any number of workers, feeds
  every rank.
- **Fault tolerance.** When a worker fails, Ember retries its tasks.

From Stoke, for streaming:

- **Fixed-size batches.** The sink packs rows across row boundaries into
  batches of one shape and frames them straight into pooled Kiln buffers.
- **Delivery while the job runs.** A task's chunks become available to the
  trainers as soon as the task commits, not when the job ends.
- **Backpressure.** A per-stream task budget bounds how many unconsumed
  chunks each producer task may have. A task that reaches it waits, so the
  feed runs at the trainers' pace and memory stays bounded.
- **Many consumers.** Each chunk goes to one rank. Producers and consumers
  scale independently because the queue is sharded and the bytes live in
  Kiln. A retried task writes the same bytes and therefore the same chunk
  id, so the shard drops the duplicate, and a chunk that a rank took but
  never acknowledged goes back into the queue.
- **Locality.** Each chunk stays on one Kiln server, and loaders take chunks
  from their own host first, reading them over shared memory. Across hosts,
  Kiln reads chunks with one-sided RDMA where the network supports it.

Stoke's own code is small: the sink, the shards and the loader. Everything
else is Ember, and every improvement to Ember is an improvement to the
dataloader.

## First numbers

We compared Stoke with [Grain](https://github.com/google/grain) on one
16-vCPU Google Cloud VM at equal CPU budgets, each loader pinned to the same cores. We report only
relative numbers. Three setups:

- **Pre-tokenized:** a pure loading benchmark. Each loader reads pre-built batches of tokenized FineWeb-Edu and hands them to
  one consumer.
- **Filter + pack, one host:** a live pipeline over tokenized FineWeb-Edu
  (filter on quality and language scores, pack documents into 4,097-token
  sequences, batch by 32) feeding four synthetic trainers. We measure the
  highest rate at which every trainer waits for data less than 1% of the
  time.
- **Filter + pack, 4 nodes:** the same, on a four-node Kubernetes cluster
  with one trainer per node, with and without local writes, where producers
  write chunks to their own node's Kiln server.

<figure>
  <img src="{{ '/assets/images/posts/stoke-vs-grain.svg' | relative_url }}" alt="Bar chart of Stoke's throughput relative to Grain at 4, 8 and 12 CPUs: pre-tokenized on one host 2.1x, 2.3x, 2.8x; filter and pack on one host with 4 trainers 3.7x, 4.2x, 4.6x; filter and pack on 4 nodes 2.4x, 2.3x, 2.7x; with local writes 2.9x, 2.5x, 3.4x.">
  <figcaption>Figure 2. Stoke's throughput relative to Grain at equal CPU budgets, median of three runs. Pre-tokenized: samples per second. Filter + pack: highest per-trainer rate with every trainer waiting less than 1%. The four Kubernetes nodes share one VM.</figcaption>
</figure>

Stoke delivered 2.1x to 2.8x Grain's throughput on pre-tokenized data, and
its lead grows with the core budget: it runs parallel C++ threads in one
process and uses nearly all of its CPUs, while Grain levels off at about six
to seven cores. With real work in the pipeline the gap widens to 3.7x to
4.6x, because filtering and packing run in Ember's engine instead of in
Python worker processes per trainer. On four nodes, where three quarters of
the chunks cross the network, Stoke keeps a 2.3x to 2.7x lead, and local
writes bring it back to 2.5x to 3.4x.

As with Ember, these are early signals from one machine, with synthetic
trainers instead of real models. The rates are far above what a single GPU
consumes; they show how much feed each loader produces per CPU. The
Kubernetes nodes share one host's cores and a loopback network, so RDMA
between real hosts is not part of these numbers. Stoke also holds more
memory than Grain, mostly Kiln DRAM for chunks in flight. Next, we will
share numbers at scale, from clusters with accelerators and RDMA networks
running real training workloads.

## Where Stoke stands

Stoke is early. This is what works today and what is planned. The
[README](https://github.com/agentic-data-infra/stoke#overview) keeps the
current list.

| | Today | Planned |
| --- | --- | --- |
| Preprocessing | <span class="item" markdown="span">Any Ember DataFrame program as the feed</span><span class="item" markdown="span">Tokenizing and packing into fixed-shape batches in the sink</span><span class="item" markdown="span">Seeded split-level shuffle, mixing as a query</span> | <span class="item" markdown="span">Per-sample shuffle</span><span class="item" markdown="span">Weighted mixing within each batch</span> |
| Delivery | <span class="item" markdown="span">Sharded queue of chunk ids, batches in Kiln</span><span class="item" markdown="span">Zero-copy PyTorch tensors</span><span class="item" markdown="span">Backpressure through a per-task budget</span><span class="item" markdown="span">Local-first reads and local writes</span> | <span class="item" markdown="span">GPUDirect: chunks from Kiln straight into GPU memory</span><span class="item" markdown="span">RDMA for the shard protocol</span> |
| Resilience | <span class="item" markdown="span">Ember task retry, duplicate chunks dropped by the shards</span><span class="item" markdown="span">Unacknowledged chunks returned to the queue</span> | <span class="item" markdown="span">Exact resume from a trainer checkpoint</span><span class="item" markdown="span">Durable shard state</span> |
| Operations | Local mode in the trainer's process, Kubernetes clusters, Python wheels | Scaling the feed job from the trainers' wait time |
{: .compare .lead}

The code is on [GitHub](https://github.com/agentic-data-infra/stoke) under
the Apache 2.0 license, including a local mode that runs the feed, Kiln and a
shard inside the training process with a plain `pip install`. In the next
post we will introduce Kiln.
