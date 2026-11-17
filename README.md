# agentic-data-infra.github.io

Source of https://agentic-data-infra.github.io, built by GitHub Pages with Jekyll.

## Adding a blog post

Add a Markdown file to `_posts/` named `YYYY-MM-DD-short-title.md`:

```markdown
---
layout: post
title: "Post title"
author: "Author name"
description: "One-line summary shown in the post list."
---

Post body in Markdown.
```

It appears on the home page and on `/blog/` once merged to `main`.

## Previewing locally

```sh
bundle install
bundle exec jekyll serve
```

Then open http://localhost:4000.
