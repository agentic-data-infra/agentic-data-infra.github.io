---
layout: post
title: "Data infrastructure for the agentic loop"
author: "Patrick Stuedi"
description: "Why agents that run experiments need a new kind of data layer, and what we are building for it."
image: /assets/images/social/data-infrastructure-for-the-agentic-loop.png
---

Data systems were built around a human loop. A person writes a query, waits,
reads the result and decides what to do next. That loop has shaped almost
every design decision in today's engines, from how jobs are scheduled to how
errors are reported.

That loop is changing. More and more jobs are now issued by AI agents, and
their results are read by agents too. The most demanding version of this is
what we call *continuous exploration*: AI systems that propose a hypothesis,
run an experiment, evaluate it, refine the hypothesis and repeat, thousands of
times in parallel, to attack hard problems in science and engineering.

We started an initiative to build open-source data infrastructure for exactly
this setting. This post explains why we think it needs new infrastructure, and
what we are building.

## The exploration loop

An exploration loop is the scientific method, automated. Scientists set the
objective and approve the expensive steps. Agents do the rest, round after
round:

1. **Propose.** Agents form new hypotheses, often by mining the results of
   earlier rounds.
2. **Implement.** Agents turn each hypothesis into a change to code or data,
   for example a candidate training dataset.
3. **Run.** The candidates are trained, simulated or sent to a lab, small runs
   first, with only the best moving on to larger scales.
4. **Evaluate.** Every candidate is scored against the current baseline, per
   slice and across seeds, and the results go into a ledger the next round
   learns from.

<figure>
  <img src="{{ '/assets/images/posts/exploration-loop.svg' | relative_url }}" alt="An exploration loop: propose, implement, run and evaluate, each step reading and writing data through one shared data layer.">
  <figcaption>Figure 1. An exploration loop. Scientists set the objective; agents run each step as hundreds of parallel variants. Every step processes data (blue), all through one shared data layer.</figcaption>
</figure>

The loop has the same shape whether the experiment is an ML training recipe, a
genome-wide analysis or a materials simulation. What changes is the runner and
the evaluator. What stays the same is that every step reads or writes large
amounts of data.

## Why the data layer sets the pace

Once agents propose and implement hypotheses, the speed of the loop is bounded
by everything else: preparing inputs, feeding training and judging outcomes.
In many domains those steps are dominated by large-scale data processing.

This is especially true in data-centric ML, where many hypotheses *are* data
changes: a new quality filter, a different dedup threshold, a reweighted
mixture. If the data layer treats every one of those experiments as an
independent full pass over the corpus, the whole loop runs at the speed of the
data layer.

## What changes when agents are the users

When the client of a data system changes from a person to an agent, almost
every assumption behind its design changes with it.

| | Human-driven | Agent-driven |
| --- | --- | --- |
| Who issues jobs | Engineers and analysts | Planner, implementer and analyst agents |
| Job rate | Tens to hundreds per day per team | Thousands to millions per day per loop |
| Job shape | Mostly independent | Mostly small variations of earlier jobs |
| Tolerated latency | Minutes to hours | Every second of waiting is an idle reasoning step |
| Result consumer | Dashboards and notebooks | Models reading compact, structured results |
| Failure handling | A human reads the error | Errors must be actionable by a model |
{: .compare}

Five properties of this workload drive our design:

- **Massive, redundant concurrency.** A proposer fans out hundreds of variants
  at once, and most differ from each other by one threshold or a few mixture
  weights. An engine should pay for the differences, not the totals.
- **Latency stalls reasoning.** Time to first result matters as much as
  throughput, so fixed overheads and stragglers matter as much as raw
  bandwidth.
- **Long-running loops.** A loop runs for days and keeps coming back to the
  same baselines, scores and embeddings, so results must outlive a single job.
- **Mixed CPU and GPU compute.** Joins, dedup and aggregation run on CPUs,
  interleaved with classifiers, embedders and model-based graders on GPUs.
- **Two ways to consume data.** The same data is analysed in batch and
  streamed into trainers at accelerator speed.

Today these needs are spread across systems that share neither an optimizer
nor a cache: batch engines with little reuse across jobs, warehouses that are
awkward for model-in-the-loop transforms, and training dataloaders that live
apart from analytics and evaluation.

## What we want the data layer to be

We think the right design treats a whole exploration loop as one large, slowly
evolving computation, and pays only for what changes between rounds. Concretely:

- **One engine for the whole loop.** Data preparation, model inference,
  evaluation and training feeds share one plan representation and one cache,
  so work done for one experiment is reused by every later one.
- **Built for agents.** Fast enough to keep up with an agent's reasoning step,
  safe under thousands of concurrent and occasionally wrong clients, with
  results and errors a model can act on.
- **At the scale of science.** The same loop that tunes an ML data recipe also
  drives genome-wide analyses, climate scoring, compound screening and
  particle-physics event selection, on CPUs, GPUs and RDMA fabrics, elastic on
  preemptible capacity.
- **Open and portable.** Apache 2.0, built on open engines
  ([Velox](https://github.com/facebookincubator/velox) and
  [Axiom](https://github.com/facebookincubator/axiom)), and deployable on any
  Kubernetes cluster.

## The first building blocks

We have built the first three pieces of this layer, all open source:

- **[Ember](https://github.com/agentic-data-infra/ember)** is a distributed C++
  DataFrame engine on Velox and Axiom. Every stage of a job gets a content
  hash, so a stage that an earlier job already computed is reused instead of
  recomputed, and calls to language models are ordinary DataFrame expressions.
- **[Kiln](https://github.com/agentic-data-infra/kiln)** is a tiered store for
  ephemeral data: shuffle data, cached results and training-feed chunks. Chunks
  are immutable and content-addressed, so two agents that produce the same
  result share one copy, and clients read and write them with one-sided RDMA.
- **[Stoke](https://github.com/agentic-data-infra/stoke)** is a distributed
  PyTorch dataloader. Preprocessing runs as an Ember job, and Stoke streams its
  output to trainers through Kiln while the job is still running.

An agent submits DataFrame jobs to Ember. Every intermediate result that should
outlive a task lives in Kiln, so it survives worker loss and later jobs can
reuse it. Stoke turns Ember's output into training batches.

<figure>
  <img src="{{ '/assets/images/posts/architecture.svg' | relative_url }}" alt="Architecture: agents submit DataFrame jobs to Ember, which calls model endpoints and passes chunk ids to Stoke; Stoke feeds trainers; Kiln holds every intermediate result in DRAM, local disk and object-store tiers.">
  <figcaption>Figure 2. How the pieces fit together. Blue boxes are the systems we built.</figcaption>
</figure>

These are early systems, tested so far on a single host. Next we are taking
them to clusters with GPUs and real RDMA networks.

## What's next

In the next post we will introduce Ember in more detail: how it reuses work
across near-identical jobs, how it runs model calls inside a query, and how it
compares with established engines. If you are building agentic or scientific
workflows and want to try any of this, the code is on
[GitHub](https://github.com/agentic-data-infra), and we would love to hear from
you.
