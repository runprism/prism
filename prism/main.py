import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Literal, Optional

import rich
import rich_click as click

# Prism imports
import prism.constants
import prism.exceptions
import prism.logging.loggers
from prism.cli.init import initialize_project
from prism.client.client import PrismProject

# Use markdown for --help
click.rich_click.USE_MARKDOWN = True


@click.group()
def cli():
    pass


@cli.command()
@click.option("--project-name", type=str, help="""Project name""", required=False)
@click.option(
    "--log-level",
    "-l",
    type=click.Choice(["info", "warning", "error", "debug", "critical"]),
    default="info",
    help="""Set the log level""",
    required=False,
)
def init(project_name, log_level):
    """Initialize a Prism project."""
    try:
        initialize_project(project_name, log_level)
    except Exception:
        prism.logging.loggers.CONSOLE.print_exception(
            show_locals=False, suppress=[prism], width=120
        )
        sys.exit(1)


@cli.command()
@click.option(
    "--project-id",
    type=str,
    help="Project ID.",
    multiple=False,
)
@click.option(
    "--project-name",
    type=str,
    help="Project name.",
    multiple=False,
)
@click.option(
    "--project-version",
    type=str,
    help="Project version.",
    multiple=False,
)
@click.option(
    "--connector",
    type=str,
    help="""Import path to the connector instances to use for your project. These can be
            accessed at runtime `CurrentRun.conn(...)`.""",
    multiple=True,
)
@click.option(
    "--concurrency",
    type=int,
    help="""Number of threads to use when running tasks. Default is `1` (i.e.,
            single-threaded)""",
    default=1,
)
@click.option(
    "--tasks-dir",
    type=str,
    help="""Directory containing tasks. Default is the "tasks" folder in the current
            directory.""",
    multiple=False,
    default=Path.cwd() / "tasks",
    required=True,
)
@click.option(
    "--package-lookups",
    type=str,
    help="""Additional directories / modules to look within when importing modules and
            functions in your code. The `tasks_dir` and its parent are automatically
            added to this list.""",
    multiple=True,
)
@click.option(
    "--task",
    "-t",
    type=str,
    help="""Task ID to run. If not specified, then all tasks are run.
            Tasks are retrieved from the `tasks_dir` path specified in the
            `PrismProject` instance. You can specify multiple tasks with as
            follows: `-t <your_first_task> -t <your_second_task>`.""",
    multiple=True,
)
@click.option(
    "--runtime-ctx",
    "-c",
    type=str,
    help="""Runtime context as a dictionary. Must be a valid JSON. These overwrite
            variables in the `PrismProject` context""",
    default="{}",
)
@click.option(
    "--all-tasks-upstream",
    "-u",
    is_flag=True,
    type=bool,
    help="Execute all tasks upstream of tasks specified with `--task`",
)
@click.option(
    "--all-tasks-downstream",
    "-d",
    is_flag=True,
    help="Execute all tasks downstream of tasks specified with `--task`",
)
@click.option(
    "--on-success",
    "-s",
    multiple=True,
    type=str,
    help="""Callback to run if the job succeeds. Use import paths to specify the
            callback. You can specify multiple callbacks as follows: `-s <callback1> -s
            <callback2>`""",
)
@click.option(
    "--on-failure",
    "-f",
    multiple=True,
    type=str,
    help="""Callback to run if the job fails. Use import paths to specify the callback.
            You can specify multiple callbacks as follows: `-f <callback1> -f
            <callback2>`""",
)
@click.option(
    "--full-refresh",
    is_flag=True,
    default=False,
    type=bool,
    help="Run all the tasks, regardless of whether or not they are already `done`",
)
@click.option(
    "--log-level",
    "-l",
    type=click.Choice(["info", "warning", "error", "debug", "critical"]),
    default="info",
    help="""Set the log level""",
    required=False,
)
@click.option(
    "--disable-rich-logging",
    is_flag=True,
    help="""Disable default logging behavior, whereby logs are beautified in the console
            with the `rich` package.""",
    default=False,
)
@click.option(
    "--log-file",
    type=str,
    help="""File in which to save the logs. Default is None.""",
    required=False,
)
def run(
    project_id: Optional[str],
    project_name: Optional[str],
    project_version: Optional[str],
    connector: Iterable[str],
    concurrency: int,
    tasks_dir: str,
    package_lookups: Iterable[str],
    task: Iterable[str],
    runtime_ctx: str,
    all_tasks_upstream: bool,
    all_tasks_downstream: bool,
    on_success: Iterable[str],
    on_failure: Iterable[str],
    full_refresh: bool,
    log_level: Literal["info", "warning", "error", "debug", "critical"],
    disable_rich_logging: bool,
    log_file: Optional[str],
):
    """Run your Prism project. This is the CLI equivalent of calling
    `PrismProject.run(...)` in your Python script.

    <br>Examples:
    - prism run
    - prism run -t task01.Task01 -t task02.Task02
    - prism run -t task01.Task01 --all-downstream
    - prism run --runtime-ctx '{"hi": 1}'
    """
    project_dir = Path.cwd()
    try:
        runtime_ctx_json: Dict[str, Any] = json.loads(runtime_ctx)
    except Exception:
        if disable_rich_logging:
            raise
        else:
            console = rich.console.Console()
            console.print_exception()
            sys.exit(1)

    version = project_version if project_version else "1.0"
    prism_project = PrismProject(
        id=project_id if project_id else f"{project_dir.name.lower()}-{version}",
        name=project_name if project_name else f"{project_dir.name.lower()}",
        version=version,
        connectors=list(connector),
        concurrency=concurrency,
        tasks_dir=tasks_dir,
        package_lookups=list(package_lookups),
    )
    prism_project.run(
        run_id=None,
        task_ids=list(task),
        runtime_ctx=runtime_ctx_json,
        all_tasks_downstream=all_tasks_downstream,
        all_tasks_upstream=all_tasks_upstream,
        on_success=list(on_success) if list(on_success) else None,
        on_failure=list(on_failure) if list(on_failure) else None,
        full_refresh=full_refresh,
        log_level=log_level,
        rich_logging=False if disable_rich_logging else True,
        log_file=log_file,
    )


@cli.command()
@click.option(
    "--project-id",
    type=str,
    help="Project ID.",
    multiple=False,
)
@click.option(
    "--project-name",
    type=str,
    help="Project name.",
    multiple=False,
)
@click.option(
    "--project-version",
    type=str,
    help="Project version.",
    multiple=False,
)
# @click.option(
#     "--connector",
#     type=str,
#     help="""Import path to the connector instances to use for your project. These can be
#             accessed at runtime `CurrentRun.conn(...)`.""",
#     multiple=True,
# )
# @click.option(
#     "--concurrency",
#     type=int,
#     help="""Number of threads to use when running tasks. Default is `1` (i.e.,
#             single-threaded)""",
#     default=1,
# )
@click.option(
    "--tasks-dir",
    type=str,
    help="""Directory containing tasks. Default is the "tasks" folder in the current
            directory.""",
    multiple=False,
    default=Path.cwd() / "tasks",
    required=True,
)
# @click.option(
#     "--package-lookups",
#     type=str,
#     help="""Additional directories / modules to look within when importing modules and
#             functions in your code. The `tasks_dir` and its parent are automatically
#             added to this list.""",
#     multiple=True,
# )
@click.option(
    "--port",
    "-p",
    type=int,
    help="""Port used by the webserver for launching the UI. Default is 8000.""",
    default=8000,
)
@click.option(
    "--open-window",
    is_flag=True,
    type=bool,
    help="Open the visualizer UI in a new window in your default web browswer.",
    default=True,
)
@click.option(
    "--hot-reload",
    is_flag=True,
    help="Update the project's graph after each local change",
    default=True,
)
@click.option(
    "--log-level",
    "-l",
    type=click.Choice(["info", "warning", "error", "debug", "critical"]),
    default="info",
    help="""Set the log level""",
    required=False,
)
def graph(
    project_id: Optional[str],
    project_name: Optional[str],
    project_version: Optional[str],
    tasks_dir: str,
    port: int,
    open_window: bool,
    hot_reload: bool,
    log_level: Literal["info", "warning", "error", "debug", "critical"],
):
    """Launch the Prism Visualizer UI to view your project as a graph. This is the CLI
    equivalent of calling `PrismProject.graph(...)` in your Python script.
    """
    project_dir = Path.cwd()
    version = project_version if project_version else "1.0"
    prism_project = PrismProject(
        id=project_id if project_id else f"{project_dir.name.lower()}-{version}",
        name=project_name if project_name else f"{project_dir.name.lower()}",
        version=version,
        tasks_dir=tasks_dir,
    )
    prism_project.graph(
        port=port,
        open_window=open_window,
        hot_reload=hot_reload,
        log_level=log_level,
    )


if __name__ == "__main__":
    cli()
