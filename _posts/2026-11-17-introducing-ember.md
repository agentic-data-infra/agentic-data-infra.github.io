---
layout: post
title: "Introducing Ember"
author: "Patrick Stuedi"
date: 2026-11-17 10:30:00 -0800
description: "A distributed DataFrame engine for agents: content-hashed stages, model calls as expressions, and shuffle that outlives its workers."
---

In our [first post]({{ '/blog/2026/11/17/data-infrastructure-for-the-agentic-loop/' | relative_url }})
we argued that agents running exploration loops need a data layer that treats
the whole loop as one slowly evolving computation and pays only for what
changes between rounds. [Ember](https://github.com/agentic-data-infra/ember)
is the engine at the center of that layer. This post explains what it is, what
sets it apart, and how it does on its first benchmarks.

## What Ember is

Ember is a distributed framework, written in C++20, for processing large
datasets on a cluster. Programs are written against a DataFrame API in
Python or C++. Ember uses
[Axiom](https://github.com/facebookincubator/axiom) for query optimization and
distributed planning,
and [Velox](https://github.com/facebookincubator/velox) for query execution. A
driver schedules the stages, retries tasks, and recomputes lost outputs from
their lineage. Results come back as Arrow tables.

```python
from ember import Session
from ember.functions import avg, col, countAll

ember = Session.builder.master("local[*]").getOrCreate()
runs = ember.read.parquet("runs.parquet")
(runs.filter(col("loss") < 0.8)
     .groupBy("model")
     .agg(countAll().alias("runs"), avg("loss"))
     .show())
```

Ember's main target is a cluster: many workers on Kubernetes, processing
datasets far larger than one machine. It also runs on a single node. In local
mode the driver and an executor live inside the caller's process, so a small
query from an agent, or a quick test on a laptop, needs no cluster at all. The
same program moves from one to the other without changes.

## Pay for the deltas

Most jobs in an exploration loop are small variations of jobs that ran a minute
earlier: the same scoring pass with a different threshold, the same corpus with
a slightly different filter. Ember is built to notice.

Every stage of a plan gets a content key: a hash of its plan, everything
upstream of it, the files it reads, the configuration that can change its
result, and the program's build. When a later job in the same session contains
a stage with the same key, that stage does not run again; its consumers read
the output stored the first time. `explain()` shows the keys and marks which
stages were reused.

For a threshold sweep this means the expensive scoring stage runs once, and
each new threshold only re-runs the cheap filter on top of it. No `cache()`
calls are needed, although they are available. When shuffle data lives in
[Kiln](https://github.com/agentic-data-infra/kiln), stage outputs are also
recorded in Kiln's catalog, so a later run of the same program build can reuse what
an earlier one computed.

## Model calls as expressions

Agents filter, label and grade large experiment outputs with models all the
time. In Ember a model call is an ordinary DataFrame expression:

```python
from ember import ai
from ember.functions import col, concat, lit

judged = reviews.withColumn("judgment", ai.generate(
    concat(lit("Rate the sentiment of this review from 0 to 1: "), col("text")),
    endpoint="small",
    output={"positive": "boolean", "sentiment": "double"}))
```

`ai.generate` sends one request per row to any OpenAI-compatible endpoint, such
as vLLM, SGLang or a hosted API, and returns a typed result. Three properties
make it fit an agentic loop:

- **Calls only where needed.** Calls run asynchronously on Velox's RPC
  operator, and only on rows that pass the filters before them.
- **Failures are data.** A failed row gets a status instead of failing the job,
  so one bad request does not cost a thousand good ones.
- **Reuse covers model calls too.** A stage with model calls is reused like any
  other, so rerunning an unchanged program makes no calls at all.
  `session.stats()` reports calls, failures and latency.

## Shuffle that outlives its workers

Data moves between stages through a shuffle. By default map outputs are files
on the workers that wrote them. With Kiln as the shuffle store, each map output
is an immutable chunk in Kiln's DRAM or local-disk tier, read with one-sided
RDMA where the network supports it. Outputs then survive the loss of the worker
that wrote them, which matters on the preemptible capacity that long-running
loops run on. When an output is lost anyway, the driver recomputes it from its
inputs.

## First numbers

We started with a 20 GB sort: simple, shuffle-heavy, and easy to validate. We
compared Ember with Spark 3.5 (DataFrame), Spark with Gluten 1.7 (Velox
native) and Ray Data 2.60, each reading and writing Parquet, at equal memory
budgets. The cluster is four Kubernetes nodes with four cores each, running
on a single 16-vCPU Google Cloud VM.

<figure>
  <img src="{{ '/assets/images/posts/ember-sort-kind.svg' | relative_url }}" alt="Bar chart of 20 GB sort job times on four Kubernetes nodes: in memory Ember 76 s, Spark with Gluten 146 s, Spark DataFrame 167 s, Ray Data 207 s; spilling with 16 partitions Ember 230 s, Gluten 282 s, Spark 315 s, Ray did not complete; spilling with 128 partitions Ember 155 s, Gluten 211 s, Spark 240 s, Ray did not complete.">
  <figcaption>Figure 1. 20 GB sort, Parquet to Parquet, on four Kubernetes nodes sharing one 16-vCPU VM. Faster of two runs; every output validated. Ray Data's processes were killed for running out of memory in both spilling cells.</figcaption>
</figure>

Ember finished first in every cell: 1.9x faster than the fastest alternative in
memory, and 1.23x and 1.36x faster when the sort spills to disk. When spilling,
every system is bound by the VM's disk, and Ember wins mostly by moving fewer
bytes.

We read these runs as correctness validation and a performance sanity check,
not as a performance claim. All four nodes share one machine's cores and disk,
so they exercise the distributed code paths but not scaling across hosts. There
is no real RDMA network and no GPU in this setup, and a sort is not an
exploration loop. We are now running cluster-wide benchmarks of agentic loop
workloads (many concurrent, near-identical jobs with reuse), which is where we
expect Ember's design to matter most, and we will share those numbers soon.

## Where Ember stands

Ember is early. This is what works today and what is planned, grouped by the
properties an exploration loop needs. The
[README](https://github.com/agentic-data-infra/ember#overview) keeps the
current list.

| | Today | Planned |
| --- | --- | --- |
| Reuse | <span class="item" markdown="span">Content-keyed stages, shown by `explain()`</span><span class="item" markdown="span">Reuse within a session, and across runs of the same build through Kiln's catalog</span><span class="item" markdown="span">`cache()` and `persist()`</span> | <span class="item" markdown="span">Reuse across sessions and agents, through a persistent driver and catalog</span><span class="item" markdown="span">Deciding what to keep from predicted reuse</span> |
| Execution | <span class="item" markdown="span">Task retry and recompute from lineage</span><span class="item" markdown="span">Shuffle and datasets in Kiln, over one-sided RDMA</span> | <span class="item" markdown="span">Shared scans across concurrent queries</span><span class="item" markdown="span">Differential execution from where a candidate diverges from its baseline</span><span class="item" markdown="span">Progressive results with tightening confidence intervals</span> |
| Models | `ai.generate` with typed outputs, called only on rows that reach it, reused like any stage | Models next to the data: inference on the job's own GPUs |
| Operations | Local mode, Kubernetes clusters, a Python wheel | <span class="item" markdown="span">Many hosts on real RDMA networks</span><span class="item" markdown="span">GPU execution through Velox's cuDF backend</span><span class="item" markdown="span">Elastic scale-out and scale-in</span><span class="item" markdown="span">Lineage and guardrails as queryable tables</span> |
{: .compare .lead}

The code is on [GitHub](https://github.com/agentic-data-infra/ember) under the
Apache 2.0 license, including a Linux wheel for Python and a local mode that
needs no cluster. In the next posts we will introduce Kiln and Stoke.
