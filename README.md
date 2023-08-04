<p align="center">
  <img src="https://github.com/runprism/prism/raw/main/.github/Logo.png" alt="prism logo" height="100"/>
</p>
<p align="center">
    <a href="https://pypi.python.org/pypi/prism-ds/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prism-ds?color=2081c1&labelColor=090422"></a>
    <a href="https://pepy.tech/badge/prism-ds/" alt="Downloads">
        <img src="https://static.pepy.tech/personalized-badge/prism-ds?period=total&units=international_system&left_color=black&right_color=blue&left_text=Downloads"/>
    </a>
</p>
<div align="center">

[![CI Linux](https://github.com/runprism/prism/actions/workflows/ci-linux.yml/badge.svg)](https://github.com/runprism/prism/actions/workflows/ci-linux.yml)
[![CI MacOS](https://github.com/runprism/prism/actions/workflows/ci-macos.yml/badge.svg)](https://github.com/runprism/prism/actions/workflows/ci-macos.yml)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)


</div>

# :wave: Welcome to Prism!
[Prism](https://www.runprism.com/) is the easiest way to create data pipelines in Python.

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

To get started with Prism projects, check out our [documentation](https://docs.runprism.com). Some sections of interest include:

- :key: [Fundamentals](https://docs.runprism.com/fundamentals)
- :seedling: [CLI](https://docs.runprism.com/cli)
- :electric_plug: [Integrations](https://docs.runprism.com/integrations)
- :bulb: [Use Cases](https://docs.runprism.com/use-cases)

In addition, check out some [example projects](https://github.com/runprism/prism_examples).


## Integrations
Prism integrates with a wide variety of third-party developer tools There are two kinds of integrations that Prism supports: adapters, and agents.

### Adapters
Adapters allow users to connect to data warehouses or analytics engines. Prism currently supports the following adapters:
| Adapter      | Command |
| ------------ | ----------- |
| **dbt** | ```pip install "prism-ds[dbt]"``` |
| **Google BigQuery** | ```pip install "prism-ds[bigquery]"``` |
| **Postgres** | ```pip install "prism-ds[postgres]"``` |
| **Presto** | ```pip install "prism-ds[presto]"``` |
| **PySpark** | ```pip install "prism-ds[pyspark]"``` |
| **Redshift** | ```pip install "prism-ds[redshift]"``` |
| **Snowflake** | ```pip install "prism-ds[snowflake]"``` |
| **Trino** | ```pip install "prism-ds[trino]"``` |

### Agents
Agents allow users to run their projects on external computing environments, e.g., Docker containers, EC2 instances, EMR clusters, and more. Prism currently supports the following agents:
| Agent      | Command |
| ------------ | ----------- |
| **docker** | ```pip install "prism-ds[docker]"``` |
| **ec2** | N/A - comes with base Prism |


## Product Roadmap

We're always looking to improve our product. Here's what we're working on at the moment:

- **Additional Agents**: EMR clusters, Databricks clusters, and more!
- **Additional adapters**: Celery, Dask, MySQL, Presto, and more!
- **Cloud deployment**: Managed orchestration platform to deploy Prism projects in the cloud

Let us know if you'd like to see another feature!