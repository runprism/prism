<p align="center">
  <img src="https://github.com/runprism/prism/raw/main/.github/prism_logo_light.png" alt="prism logo" width="350"/>
</p>
<p align="center">
    <a href="https://pypi.python.org/pypi/prism-ds/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prism-ds?color=2081c1&labelColor=090422"></a>
    <a href="https://pepy.tech/badge/prism-ds/" alt="Downloads">
        <img src="https://static.pepy.tech/personalized-badge/prism-ds?period=total&units=international_system&left_color=black&right_color=blue&left_text=Downloads" /></a>
</p>
<div align="center">

[![CI Linux](https://github.com/runprism/prism/actions/workflows/ci-linux.yml/badge.svg)](https://github.com/runprism/prism/actions/workflows/ci-linux.yml)
[![CI MacOS](https://github.com/runprism/prism/actions/workflows/ci-macos.yml/badge.svg)](https://github.com/runprism/prism/actions/workflows/ci-macos.yml)
[![CI Windows](https://github.com/runprism/prism/actions/workflows/ci-windows.yml/badge.svg)](https://github.com/runprism/prism/actions/workflows/ci-windows.yml)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)


</div>
<p align="center">
    <i>Prism is currently in <b>alpha</b></i>.
</p>

# :wave: Welcome to Prism!
[Prism](https://docs.runprism.com) is the easiest way to create data pipelines in Python.

## Introduction
Data projects often require multiple steps that need to be executed in sequence (think extract-transform-load, data wrangling, etc.). With Prism, users can break down their project into modular tasks, manage dependencies, and execute complex computations in sequence.

Here are some of Prism's main features:
- **Real-time dependency declaration**: With Prism, analysts can declare dependencies using a simple function call. No need to explicitly keep track of the pipeline order — at runtime, Prism automatically parses the function calls and builds the dependency graph.
- **Intuitive logging**: Prism automatically logs events for parsing the configuration files, compiling the tasks and creating the DAG, and executing the tasks. No configuration is required.
- **Flexible CLI**: Users can instantiate, compile, and run projects using a simple, but powerful command-line interface.
- **“Batteries included”**: Prism comes with all the essentials needed to get up and running quickly. Users can create and run their first DAG in less than 2 minutes. 
- **Integrations**: Prism integrates with several tools that are popular in the data community, including Snowflake, Google BigQuery, Redshift, PySpark, and dbt. We're adding more integrations every day, so let us know what you'd like to see!


## Getting Started

Prism can be installed via ```pip```. Prism requires Python >= 3.7.

```
pip install --upgrade pip
pip install prism-ds
```

Prism also supports several adapters! They can be installed with the following commands:
| Adapter      | Command |
| ------------ | ----------- |
| **dbt** | ```pip install "prism-ds[dbt]"``` |
| **Google BigQuery** | ```pip install "prism-ds[bigquery]"``` |
| **Postgres** | ```pip install "prism-ds[postgres]"``` |
| **PySpark** | ```pip install "prism-ds[pyspark]"``` |
| **Redshift** | ```pip install "prism-ds[redshift]"``` |
| **Snowflake** | ```pip install "prism-ds[snowflake]"``` |
| **Trino** | ```pip install "prism-ds[trino]"``` |

To get started with Prism projects, check out our [documentation](https://docs.runprism.com). Some sections of interest include:

- :key: [Fundamentals](https://docs.runprism.com/fundamentals)
- :seedling: [CLI](https://docs.runprism.com/cli)
- :electric_plug: [Integrations](https://docs.runprism.com/integrations)
- :bulb: [Use Cases](https://docs.runprism.com/use-cases)

In addition, check out some [example projects](https://github.com/runprism/prism_examples).


## Product Roadmap

We're always looking to improve our product. Here's what we're working on at the moment:

- **Triggers**: trigger function calls on the success and/or failure of your project
- **Additional adapters**: Celery, Dask, Docker, and more!
- **Cloud deployment**: managed orchestration platform to deploy Prism projects in the cloud

Let us know if you'd like to see another feature!