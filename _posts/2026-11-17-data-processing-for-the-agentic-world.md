---
layout: post
title: "Data processing for the agentic world"
author: "Patrick Stuedi"
description: "When agents launch the jobs and agents read the results, the data layer sets the pace of discovery. Why we are building new infrastructure for it."
---

Data systems were built around a human loop: a person writes a query, waits,
reads the result and decides what to do next. That loop is changing. More and
more jobs are issued by agents, and their results are read by agents.

The most demanding version of this is *continuous exploration*: AI systems that
propose a hypothesis, run an experiment, evaluate it, refine the hypothesis and
repeat, thousands of times in parallel. Efforts to automate the scientific loop
this way are under way in ML research, materials, biology, chip design and
systems optimization. They share a loop with a measurable objective and a very
high iteration count. The faster and cheaper each iteration, the more
hypotheses get tested.

This post explains why we think the data layer becomes the bottleneck of that
loop, and what a data system built for it should look like. It is the idea
behind Agentic Data Infra.

## What a discovery loop looks like

Take a concrete objective: at a fixed compute budget, lower the validation loss
of a 1B-parameter language model pretraining recipe without regressing
downstream evaluations.

A discovery loop for this is a small set of roles around shared state. A human
writes the objective, the constraints and the budget. An orchestrator agent
keeps the hypothesis tree and the experiment ledger. Proposer agents read
papers and past results and suggest changes. Coding agents implement them on
isolated branches. A runner schedules jobs on elastic, often preemptible
accelerators. A frozen evaluation harness scores each run across seeds and
slices. An analyst agent interprets the results and writes notes that inform
the next round. Humans approve the expensive steps.

Candidates climb a ladder. On night one, 200 hypotheses are proposed and 140
pass smoke tests and run at 20M parameters, about 15 minutes each. On day two,
the top 20 run at 150M parameters with three seeds. On day four, the top three
run at 1B parameters. On day six, one survivor becomes the new baseline, and
the loop continues.

The skeleton generalizes. In chip design the runner is an EDA flow; in drug or
materials discovery it is a simulation or a robotic lab. Only the runner and
the evaluator change.

## Data processing is everywhere in the loop

Once proposing and implementing hypotheses is automated, the speed of the loop
is bounded by everything else, and much of everything else is large-scale data
processing:

- **The experiment itself.** In modern ML, many hypotheses are data changes: a
  new quality filter, a dedup threshold, a reweighted mixture, synthetic data,
  a tokenizer change. Running one means rewriting a corpus of trillions of
  tokens, and there are hundreds of variants per week.
- **Training and simulation feeds.** Every run streams, decodes, shuffles and
  transforms data fast enough to keep accelerators busy. Thousands of
  concurrent runs read different mixtures and versions.
- **Evaluation.** Profiling what a filter removed, checking every training feed
  against every eval set for contamination, grading millions of samples with a
  model, replaying billions of logged impressions, re-running genome-wide
  analyses, scoring against petabytes of climate reanalysis data. Evaluation
  scales with candidates times slices.
- **Generation-heavy experiments.** RL rollouts, synthetic data and simulation
  output are batch inference followed by bulk filtering, scoring, dedup and
  aggregation: a data pipeline in disguise.
- **Telemetry and the ledger.** Thousands of runs produce metrics, logs,
  traces and checkpoints. Analyst agents ask questions across all of them, such
  as which runs had loss spikes and what their configs shared.

For data-centric experiments the line between preparation and evaluation
disappears. The same engine creates a candidate dataset, profiles it, checks it
for contamination, feeds training and computes per-slice deltas. If that
engine treats every experiment as an independent full pass over the data, the
loop runs at the speed of the data layer.

## A new kind of workload

When the client of a data system changes from a person to an agent, almost
every assumption behind its design changes too:

- **Massive, redundant concurrency.** Thousands of jobs are in flight per loop,
  and most are small variations of each other: one threshold changed, a few
  mixture weights adjusted, one checkpoint swapped.
- **Latency stalls reasoning.** An agent waiting on a result is an idle
  reasoning step. Time to first signal matters as much as throughput.
- **Statistical answers.** The question is rarely an exact number. It is
  whether the candidate beats the baseline by more than noise, overall and per
  slice.
- **Mixed CPU and GPU work.** Joins, dedup and aggregation are interleaved with
  classifiers, embedders and model-based graders.
- **Two consumption modes.** Batch analysis, and streaming feeds into trainers
  at accelerator speed.
- **Machine consumers.** Results are read by models, so they must be compact,
  structured and self-describing, and errors must be something a model can act
  on.
- **Tireless, fast and occasionally wrong clients.** No human notices nonsense
  along the way. Agents will exploit any leak at scale, from eval
  contamination to seed luck, and will sometimes launch runaway plans.

## What the data layer should look like

Our goal is to make the whole exploration loop behave like one large, slowly
evolving computation, and to pay only for the deltas. Three ideas carry most of
the leverage:

1. **Hashed plan DAGs with global memoization.** Every job compiles to a
   deterministic plan, and every operator is keyed by its parameters, code
   version, model weights and input snapshot. Anything already computed,
   anywhere in the loop, is reused. A candidate filter that only changes a
   threshold reuses the expensive classifier scores and recomputes only the
   cheap filter. Concurrent jobs that read the same data share one scan: a
   fan-out of 200 variants becomes one pass instead of 200.
2. **Differential and progressive execution as native operators.** The engine
   understands "candidate versus baseline". It finds where two plans diverge,
   computes only what changes downstream, and reports per-slice deltas with
   confidence intervals. Jobs run on stratified samples first and stop as soon
   as the statistics allow, so clear losers are discarded after a small
   fraction of the data, while the false-discovery rate is controlled across
   thousands of candidates.
3. **Lazy datasets served directly to consumers.** The same plan runs as a
   streaming source for trainers and simulators, with deterministic shuffling
   and exact resume after preemption. A data mixture is a sampling plan over
   base tables, not a materialized corpus, so most data experiments never need
   to be written out at all.

Around these sit the rest of the system: model inference as a first-class,
batched operator; CPU and GPU pools that scale separately on preemptible
capacity; a fast ephemeral tier for shuffle, cached intermediates and training
feeds, over the RDMA fabric AI clusters already have; lineage for every number;
and an agent-native interface with dry runs, cost estimates, quotas and
guardrails such as automatic contamination checks.

Today these capabilities are spread across batch engines, warehouses, ML data
frameworks and training-feed services that share neither an optimizer nor a
cache. Many of the individual techniques have strong prior work, from computation
reuse to semantic operators and online aggregation. What we think is new is
combining them for discovery loops, where data jobs are the experiments
themselves.

## What we are building

We are building this as open source, under Apache 2.0, on top of open engines
such as [Velox](https://github.com/facebookincubator/velox) and
[Axiom](https://github.com/facebookincubator/axiom). The first building blocks
are already on GitHub:

- **[Ember](https://github.com/agentic-data-infra/ember)**, a distributed C++
  DataFrame engine on Velox.
- **[Kiln](https://github.com/agentic-data-infra/kiln)**, a tiered store for
  ephemeral data, such as shuffle data, cached results and training-feed
  chunks, with one-sided RDMA reads and writes.
- **[Stoke](https://github.com/agentic-data-infra/stoke)**, a distributed
  PyTorch dataloader that runs preprocessing as an Ember job and feeds trainers
  through Kiln.

Many hard problems are still open: keeping memoization correct when operators
and GPUs are not deterministic, choosing what to keep when future reuse depends
on what the agents propose next, keeping statistics valid under adaptive search
across thousands of candidates, and making global shuffles deterministic,
resumable and elastic at the same time. We will write about them as we go.

Next up: a closer look at Ember.
