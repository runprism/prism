<p align="center">
  <img src="https://github.com/runprism/prism/blob/main/.github/prism_logo_light.png" alt="prism logo" width="350"/>
</p>
<p align="center">
    <a href="https://github.com/runprism/prism/actions/workflows/ci-linux.yml">
        <img src="https://github.com/runprism/prism/actions/workflows/ci-linux.yml/badge.svg" alt="linux tests badge">
    </a>
    <a href="https://github.com/runprism/prism/actions/workflows/ci-macos.yml">
        <img src="https://github.com/runprism/prism/actions/workflows/ci-macos.yml/badge.svg" alt="macos tests badge">
    </a>
    <a href="https://github.com/runprism/prism/actions/workflows/ci-windows.yml">
        <img src="https://github.com/runprism/prism/actions/workflows/ci-windows.yml/badge.svg" alt="windows tests badge">
    </a>
        <a href="https://github.com/runprism/prism/actions/workflows/style.yml">
        <img src="https://github.com/runprism/prism/actions/workflows/style.yml/badge.svg" alt="style tests badge">
    </a>
</p>
<p align="center">
    <i>Prism is currently in <b>alpha</b></i>.
</p>

# :wave: Welcome to Prism!
[Prism](https://docs.runprism.com) is the easiest way to create data pipelines in Python.

## Introduction
Data projects often require multiple steps that need to be executed in sequence (think extract-transform-load, data wrangling, etc.). With Prism, users can break down their project into modular tasks, manage dependencies, and execute complex computations in sequence.

Here are some of Prism's main features:
- **Modules as tasks**: Unlike other orchestration platforms, Prism allows tasks to live within their own modules. This not only helps with readability and QC, but also enables users to build powerful, complex pipelines that scale alongside their project.
- **Real-time dependency declaration**: With Prism, analysts can declare dependencies using a simple function call. No need to explicitly keep track of the pipeline order — at runtime, Prism automatically parses the function calls and builds the dependency graph.
- **Flexible CLI**: Analysts can instantiate, compile, and run projects using a simple command-line interface.
- **Integrations**: Prism integrates with several tools that are popular in the data community, including dbt, Snowflake, PySpark and Google BigQuery. We're adding more integrations every day, so let us know what you'd like to see!

## Getting Started

Prism can be installed via ```pip```. Prism requires Python >= 3.7.

```
pip install prism-ds
```

To get started with Prism projects, check out our [documentation](https://docs.runprism.com). Some sections of interest include:

- :key: [Fundamentals](https://docs.runprism.com/fundamentals)
- :seedling: [CLI](https://docs.runprism.com/cli)
- :electric_plug: [Integrations](https://docs.runprism.com/integrations)
- :bulb: [Use Cases](https://docs.runprism.com/use-cases)

In addition, check out some [example projects](https://github.com/mtrivedi50/prism_examples).


## Product Roadmap

We're always looking to improve our product. Here's what we're working on at the moment:

- **DAG visualizer**: a clean UI for visualizing the data flow between tasks
- **Python API**: an API that packages projects into a single class that can be called in other programs
- **Additional adapters**: Postgres, Redshift, and more!
- **Cloud deployment**: deploying projects on Amazon EMR clusters, Docker containers, Databricks clusters, and more!

Let us know if you'd like to see another feature!