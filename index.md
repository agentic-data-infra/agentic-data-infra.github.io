---
layout: default
title: Open data infrastructure for agentic and scientific workflows
---

# Open data infrastructure for agentic and scientific workflows

We are building open-source data infrastructure for large-scale agentic and
scientific workflows: the shared data layer that AI agents and scientists use
to prepare data, call models, evaluate experiments and feed training, across
thousands of concurrent experiments, without paying twice for the same work.

Scientists set the objective; agents propose, implement, run and evaluate
experiments, round after round. Every step reads or writes large amounts of
data, so the data layer sets the pace of the whole loop, whether the experiment
is an ML training recipe, a genome-wide analysis or a materials simulation.

When we succeed, that layer looks like this:

- **One engine for the whole loop.** Data preparation, model inference,
  evaluation and training feeds share one plan representation and one cache,
  so work done for one experiment is reused by every later one.
- **Built for agents.** Fast enough to keep up with an agent's reasoning step,
  safe under thousands of concurrent and occasionally wrong clients, with
  results and errors a model can act on.
- **At the scale of science.** The same loop that tunes an ML data recipe also
  drives genome-wide analyses, climate reanalysis scoring, compound and protein
  screening, and particle-physics event selection, on CPUs, GPUs, TPUs and RDMA
  fabrics, elastic on preemptible capacity.
- **Open and portable.** Apache 2.0, built on open engines
  ([Velox](https://github.com/facebookincubator/velox),
  [Axiom](https://github.com/facebookincubator/axiom)), and deployable on any
  Kubernetes cluster.

## Building blocks

- **[Ember](https://github.com/agentic-data-infra/ember)**, a distributed C++
  DataFrame engine on Velox.
- **[Kiln](https://github.com/agentic-data-infra/kiln)**, a tiered store for
  ephemeral data (shuffle, cached results, training-feed chunks) with one-sided
  RDMA reads and writes.
- **[Stoke](https://github.com/agentic-data-infra/stoke)**, a distributed
  PyTorch dataloader that runs preprocessing as an Ember job and feeds trainers
  through Kiln.

## Blog

{% include post-list.html %}
