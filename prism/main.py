"""
Argument parser

Table of Contents
- Imports
- Util functions
- Arg parser
"""

###########
# Imports #
###########

import rich_click as click
import argparse
import re
import json
import sys
from typing import Any, Optional, List

# Prism imports
import prism.constants
import prism.cli.init
import prism.cli.run
import prism.cli.compile
import prism.cli.connect
import prism.cli.create_agent
import prism.cli.create_task
import prism.cli.create_trigger
import prism.cli.graph
import prism.cli.agent
import prism.cli.spark_submit
import prism.exceptions
from prism.ui import (
    RED,
    YELLOW,
    RESET
)


##################
# Util functions #
##################

def _check_vars_format(inputted_vars):
    for v in inputted_vars:
        if not re.findall(r'^(.)+=(.)+$', v):
            raise prism.exceptions.ArgumentException(
                message=f"invalid argument -v `{v}`... should be formatted as a key-value pair like VAR=VALUE"  # noqa: E501
            )
    vars_dict = {
        v.split("=")[0]: v.split("=")[1] for v in inputted_vars
    }
    return vars_dict


def _check_context(inputted_vars, inputted_context):
    # Context must be a valid JSON
    _ = json.loads(inputted_context)
    if len(inputted_vars) > 0 and inputted_context != '{}':
        raise prism.exceptions.ArgumentException(
            message="`vars` and `context` are mutually exclusive arguments"
        )


def _process_modules(inputted_modules) -> Optional[List[Any]]:
    processed_modules = []
    for m in list(inputted_modules):
        processed = m

        # If the user adds modules/ at the beginning of their modules (for auto-fill
        # purposes), then just remove that prefix.
        if len(re.findall(r'^modules\/', processed)) > 0:
            click.echo(
                f"{RED}ArgumentError: remove `modules/` from your --module argument `{m}`{RESET}"  # noqa: E501
            )
            sys.exit(1)

        # If the user wants to run a specific module and puts .py at the end, fire a
        # warning.
        if len(re.findall(r'\.py$', processed)) > 0:
            click.echo(
                f'{YELLOW}ArgumentWarning: `.py` in --module arguments will be an error in a future version of Prism.{RESET}'  # noqa: E501
            )

        # If the user wants to run a specific module but doesn't put .py at the end,
        # then add it in.
        if (
            len(re.findall(r'\.py$', processed)) == 0
            and len(re.findall(r'\/\*$', processed)) == 0  # noqa: W503
        ):
            processed = f'{processed}.py'
        processed_modules.append(processed)

    if len(processed_modules) > 0:
        return processed_modules
    return None


##############
# Arg parser #
##############

# Use markdown for --help
click.rich_click.USE_MARKDOWN = True


@click.group()
def cli():
    pass


def invoke(args: Optional[List[str]] = None, bool_return: bool = False):
    """
    Thin wrapper around `main`
    """
    res = cli(args, standalone_mode=False)
    # If the result is an exit code, then exit (this happens with `--help`)
    if isinstance(res, int):
        sys.exit(res)

    # Otherwise, if we want to return our result, then return it
    if bool_return:
        return res

    # If we don't want to return the result, then exit with an error code if we
    # encountered an error in the process of executing our argument.
    if res.has_error:
        sys.exit(1)
    else:
        sys.exit(0)


@cli.command()
@click.option(
    "--project-name",
    type=str,
    help="""Project name""",
    required=False
)
@click.option(
    "--minimal",
    is_flag=True,
    help="""Create minimal project (just `prism_project.py` and `modules`)""",
    default=False,
    required=False
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
def init(project_name, minimal, log_level):
    """Initialize a Prism project.
    """
    # Namespace
    ns = argparse.Namespace()
    ns.project_name = project_name
    ns.minimal = minimal
    ns.log_level = log_level

    # Instantiate and run task
    task = prism.cli.init.InitTask(ns)
    result = task.run()
    return result


@cli.command()
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
def compile(
    log_level,
    full_tb,
):
    """Parse the tasks.ref(...) calls, construct the DAG, and generate the manifest.

    <br>Examples:
    - prism compile
    - prism compile -l debug
    """
    # Namespace
    ns = argparse.Namespace()
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.which = 'compile'

    # Instantiate and run task
    task = prism.cli.compile.CompileTask(ns)
    result = task.run()
    return result


@cli.command()
@click.option(
    '--type', '-t',
    type=click.Choice(prism.constants.VALID_ADAPTERS),
    help="Connection type",
    required=True,
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
@click.option(
    '--vars', '-v',
    help="Variables as key value pairs. These overwrite variables in prism_project.py. All values are intepreted as strings.",  # noqa: E501
    multiple=True
)
@click.option(
    '--context',
    type=str,
    help="Context as a dictionary. Must be a valid JSON. These overwrite variables in prism_project.py",  # noqa: E501
    default='{}'
)
def connect(
    type,
    log_level,
    full_tb,
    vars,
    context,
):
    """Connect your Prism project to third-party adapters (e.g., Snowflake, BigQuery,
    PySpark, etc.).

    <br>Examples:
    - prism connect --type snowflake
    - prism connect --type snowflake -v PROFILE_YML_PATH=/Users/.prism/profile.yml
    - prism connect --type snowflake --context '{"PROFILE_YML_PATH", "/Users/.prism/profile.yml"}'
    """  # noqa
    # Check `vars` and `context`
    vars_dict = _check_vars_format(vars)
    _check_context(vars, context)

    # Namespace
    ns = argparse.Namespace()
    ns.type = type
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.vars = vars_dict
    ns.context = context
    ns.which = 'connect'

    # Instantiate and run task
    task = prism.cli.connect.ConnectTask(ns)
    result = task.run()
    return result


@cli.command()
@click.option(
    '--module', '-m',
    type=str,
    help="Modules to execute. You can specify multiple modules with as follows: `-m <your_first_module> -m <your_second_module>`.",  # noqa: E501
    multiple=True,
    default=[]
)
@click.option(
    '--all-downstream',
    is_flag=True,
    help="Execute all tasks downstream of modules specified with `--module`"
)
@click.option(
    '--all-upstream',
    is_flag=True,
    type=bool,
    help="Execute all tasks upstream of modules specified with `--module`"
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
@click.option(
    '--vars', '-v',
    help="Variables as key value pairs. These overwrite variables in prism_project.py. All values are intepreted as strings.",  # noqa: E501
    multiple=True
)
@click.option(
    '--context',
    type=str,
    help="Context as a dictionary. Must be a valid JSON. These overwrite variables in prism_project.py",  # noqa: E501
    default='{}'
)
def run(
    module,
    all_downstream,
    all_upstream,
    log_level,
    full_tb,
    vars,
    context
):
    """Execute your Prism project.

    <br>Examples:
    - prism run
    - prism run -m module01.py -m module02.py
    - prism run -m module01 --all-downstream
    - prism run -v VAR1=VALUE1 -v VAR2=VALUE2
    - prism run --context '{"hi": 1}'
    """
    # Convert tuple of modules to list
    modules_list: Optional[List[Any]] = _process_modules(module)

    # Check `vars` and `context`
    vars_dict = _check_vars_format(vars)
    _check_context(vars, context)

    # Namespace
    ns = argparse.Namespace()
    ns.modules = modules_list
    ns.all_upstream = all_upstream
    ns.all_downstream = all_downstream
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.vars = vars_dict
    ns.context = context
    ns.which = 'run'

    # Instantiate and run task
    task = prism.cli.run.RunTask(ns)
    result = task.run()
    return result


@cli.group()
def create():
    """Create and/or update project components (e.g., tasks, triggers, agents).
    """
    pass


@create.command('agent')
@click.option(
    '--type', '-t',
    type=click.Choice(prism.constants.VALID_AGENTS),
    required=True,
    help="Agent type"
)
@click.option(
    '--file', '-f',
    type=str,
    required=False,
    help="File path for agent YML configuration. Default is `./agent.yml`.",
    default="./agent.yml",
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
def create_agent(
    type,
    file,
    log_level,
    full_tb,
):
    """Create an agent YML configuration file

    <br>Examples:
    - prism create agent --type docker
    - prism create agent --type ec2 --file ./agents/ec2.yml
    """

    # Namespace
    ns = argparse.Namespace()
    ns.type = type
    ns.file = file
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.which = 'agent'

    # Need these for the base task to run
    ns.vars = None
    ns.context = '{}'

    # Instantiate and run task
    task = prism.cli.create_agent.CreateAgentTask(ns)
    result = task.run()
    return result


@create.command('task')
@click.option(
    '--type', '-t',
    type=click.Choice(prism.constants.VALID_TASK_TYPES),
    required=False,
    help="Task type. Default is `python`",
    default="python",
)
@click.option(
    '--decorated',
    is_flag=True,
    required=False,
    help="If specified, the task will be a decorated function",
    default=False,
)
@click.option(
    '--number',
    type=int,
    required=False,
    default=1,
    help="""Number of tasks to create. Default is 1.""",
)
@click.option(
    '--name',
    type=str,
    required=False,
    default="new_task",
    help="""
    Task name. If only a single task is requested, then the task will be named
    `<task_name>.py`. If multiple tasks are requested, then the tasks will be named
    `<task_name>_<number>.py`. Tasks should have short, all-lowercase names. Underscores
    can be used in the module name if it improves readability.
    """
)
@click.option(
    '--dir',
    type=str,
    required=False,
    default="",
    help="""
    Folder within the `modules` directory in which the new tasks should live. If not
        specified, then new tasks will be dumpted into `modules/`
    """
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
def create_task(
    type,
    decorated,
    number,
    name,
    dir,
    log_level,
    full_tb
):
    """Create new tasks for your project.

    <br>Examples:
    - prism create task --type python
    - prism create task --n 3 --name extract_step
    """

    # Check the task name
    if len(re.findall(r'^[a-z0-9\_]+$', name)) == 0:
        raise prism.exceptions.ArgumentException(
            message=f"invalid task name `{name}`...the task name can only contain lowercase alpha-numeric characters and underscores"  # noqa: E501
        )

    # Namespace
    ns = argparse.Namespace()
    ns.type = type
    ns.decorated = decorated
    ns.number = number
    ns.name = name
    ns.dir = dir
    ns.log_level = log_level
    ns.full_tb = full_tb
    ns.which = 'task'

    # Need these for the base task to run
    ns.vars = None
    ns.context = '{}'

    # Instantiate and run task
    task = prism.cli.create_task.CreateTaskTask(ns)
    result = task.run()
    return result


@create.command('trigger')
@click.option(
    '--type', '-t',
    type=click.Choice(["function"]),
    required=False,
    help="Trigger type. Default is `function`",
    default="function",
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
@click.option(
    '--vars', '-v',
    help="Variables as key value pairs. These overwrite variables in prism_project.py. All values are intepreted as strings.",  # noqa: E501
    multiple=True
)
@click.option(
    '--context',
    type=str,
    help="Context as a dictionary. Must be a valid JSON. These overwrite variables in prism_project.py",  # noqa: E501
    default='{}'
)
def create_trigger(
    type,
    log_level,
    full_tb,
    vars,
    context
):
    """Create a triggers YML file at the TRIGGERS_YML_PATH in `prism_project.py`.

    <br>Examples:
    - prism create trigger
    - prism create trigger -v TRIGGERS_YML_PATH=/Users/.prism/triggers.yml
    - prism create trigger --context '{"TRIGGERS_YML_PATH": "/Users/.prism/triggers.yml"}'
    """  # noqa
    # Check `vars` and `context`
    vars_dict = _check_vars_format(vars)
    _check_context(vars, context)

    # Namespace
    ns = argparse.Namespace()
    ns.type = type
    ns.log_level = log_level
    ns.full_tb = full_tb
    ns.which = 'trigger'
    ns.vars = vars_dict
    ns.context = context

    # Instantiate and run task
    task = prism.cli.create_trigger.CreateTriggerTask(ns)
    result = task.run()
    return result


@cli.command()
@click.option(
    "--port", "-p",
    type=int,
    default=8080,
    required=False,
    help="Port to use for HTTP request; default is 8080",
)
@click.option(
    "--no-browser",
    is_flag=True,
    default=False,
    help="""
    Overwrite default behavior and do not open a tab in the default web browser with
    the docs UI.
    """
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
def graph(
    port,
    no_browser,
    log_level,
    full_tb,
):
    """Visualize your Prism project as a DAG in an interactive UI.

    <br>Examples:
    - prism graph
    - prism graph --no-browser --port 3000
    """
    # Namespace
    ns = argparse.Namespace()
    ns.port = port
    ns.no_browser = no_browser
    ns.log_level = log_level
    ns.full_tb = full_tb
    ns.which = 'graph'

    # Need these for the compile subclass task to run
    ns.all_downstream = True
    ns.all_upstream = True

    # Need these for the base task to run
    ns.vars = None
    ns.context = '{}'

    # Instantiate and run task
    task = prism.cli.graph.GraphTask(ns)
    result = task.run()
    return result


@cli.group()
def agent():
    """Execute your Prism using third-party agents, (e.g., Docker containers, EC2
    instances, etc.).
    """
    pass


@agent.command('apply')
@click.option(
    "--file", "-f",
    required=True,
    type=str,
    help="Path to agent configuration YML"
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
def agent_apply(
    file,
    log_level,
    full_tb,
):
    """Build your agent using a configuration YML.

    <br>Examples:
    - prism agent apply -f ./ec2.yml
    - prism agent apply -f /Users/docker.yml
    """
    # Namespace
    ns = argparse.Namespace()
    ns.file = file
    ns.log_level = log_level
    ns.full_tb = full_tb
    ns.which = 'agent-apply'

    # Need these for the base task to run
    ns.vars = None
    ns.context = '{}'

    # Instantiate and run task
    task = prism.cli.agent.AgentTask(ns)
    result = task.run()
    return result


@agent.command('run')
@click.option(
    "--file", "-f",
    required=True,
    type=str,
    help="Path to agent configuration YML"
)
@click.option(
    '--module', '-m',
    type=str,
    help="Modules to execute. You can specify multiple modules with as follows: `-m <your_first_module> -m <your_second_module>`.",  # noqa: E501
    multiple=True,
    default=[]
)
@click.option(
    '--all-downstream',
    is_flag=True,
    help="Execute all tasks downstream of modules specified with `--module`"
)
@click.option(
    '--all-upstream',
    is_flag=True,
    type=bool,
    help="Execute all tasks upstream of modules specified with `--module`"
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
@click.option(
    '--vars', '-v',
    help="Variables as key value pairs. These overwrite variables in prism_project.py. All values are intepreted as strings.",  # noqa: E501
    multiple=True
)
@click.option(
    '--context',
    type=str,
    help="Context as a dictionary. Must be a valid JSON. These overwrite variables in prism_project.py",  # noqa: E501
    default='{}'
)
def agent_run(
    file,
    module,
    all_downstream,
    all_upstream,
    log_level,
    full_tb,
    vars,
    context
):
    """Run your project using an agent.

    <br>Examples:
    - prism agent run -f ./ec2.yml --module module01.py --module module02.py
    - prism agent run -f ./docker.yml --m module01
    - prism agent run -f /Users/docker.yml --vars VAR1=VALUE1
    """
    # Namespace
    ns = argparse.Namespace()

    # Convert tuple of modules to list
    modules_list: Optional[List[Any]] = _process_modules(module)

    # Check `vars` and `context`
    vars_dict = _check_vars_format(vars)
    _check_context(vars, context)

    # Namespace
    ns = argparse.Namespace()
    ns.file = file
    ns.modules = modules_list
    ns.all_upstream = all_upstream
    ns.all_downstream = all_downstream
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.vars = vars_dict
    ns.context = context
    ns.which = 'agent-run'

    # Instantiate and run task
    task = prism.cli.agent.AgentTask(ns)
    result = task.run()
    return result


@agent.command('build')
@click.option(
    "--file", "-f",
    required=True,
    type=str,
    help="Path to agent configuration YML"
)
@click.option(
    '--module', '-m',
    type=str,
    help="Modules to execute. You can specify multiple modules with as follows: `-m <your_first_module> -m <your_second_module>`.",  # noqa: E501
    multiple=True,
    default=[]
)
@click.option(
    '--all-downstream',
    is_flag=True,
    help="Execute all tasks downstream of modules specified with `--module`"
)
@click.option(
    '--all-upstream',
    is_flag=True,
    type=bool,
    help="Execute all tasks upstream of modules specified with `--module`"
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
@click.option(
    '--vars', '-v',
    help="Variables as key value pairs. These overwrite variables in prism_project.py. All values are intepreted as strings.",  # noqa: E501
    multiple=True
)
@click.option(
    '--context',
    type=str,
    help="Context as a dictionary. Must be a valid JSON. These overwrite variables in prism_project.py",  # noqa: E501
    default='{}'
)
def agent_build(
    file,
    module,
    all_downstream,
    all_upstream,
    log_level,
    full_tb,
    vars,
    context
):
    """Build your agent using a configuration YML and then run your project on the newly
    created agent.

    <br>Examples:
    - prism agent build -f ./ec2.yml
    - prism agent build -f /Users/docker.yml --context '{"HI": 1}'
    """
    # Namespace
    ns = argparse.Namespace()

    # Convert tuple of modules to list
    modules_list: Optional[List[Any]] = _process_modules(module)

    # Check `vars` and `context`
    vars_dict = _check_vars_format(vars)
    _check_context(vars, context)

    # Namespace
    ns = argparse.Namespace()
    ns.file = file
    ns.modules = modules_list
    ns.all_upstream = all_upstream
    ns.all_downstream = all_downstream
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.vars = vars_dict
    ns.context = context
    ns.which = 'agent-build'

    # Instantiate and run task
    task = prism.cli.agent.AgentTask(ns)
    result = task.run()
    return result


@agent.command('delete')
@click.option(
    "--file", "-f",
    required=True,
    type=str,
    help="Path to agent configuration YML"
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs"
)
def agent_delete(
    file,
    log_level,
    full_tb,
):
    """Delete your agent.

    <br>Examples:
    - prism agent delete -f ./ec2.yml
    - prism graph delete -f /Users/agents.yml
    """
    # Namespace
    ns = argparse.Namespace()
    ns.file = file
    ns.log_level = log_level
    ns.full_tb = full_tb
    ns.which = 'agent-delete'

    # Need these for the base task to run
    ns.vars = None
    ns.context = '{}'

    # Instantiate and run task
    task = prism.cli.agent.AgentTask(ns)
    result = task.run()
    return result


@cli.command('spark-submit')
@click.option(
    '--module', '-m',
    type=str,
    help="Modules to execute. You can specify multiple modules with as follows: `-m <your_first_module> -m <your_second_module>`.",  # noqa: E501
    multiple=True,
    default=[]
)
@click.option(
    '--all-downstream',
    is_flag=True,
    help="Execute all tasks downstream of modules specified with `--module`."
)
@click.option(
    '--all-upstream',
    is_flag=True,
    type=bool,
    help="Execute all tasks upstream of modules specified with `--module`."
)
@click.option(
    '--log-level', '-l',
    type=click.Choice(['info', 'warn', 'error', 'debug']),
    default="info",
    help="""Set the log level.""",
    required=False
)
@click.option(
    '--full-tb',
    is_flag=True,
    type=bool,
    help="Show the full traceback when an error occurs."
)
@click.option(
    '--vars', '-v',
    help="Variables as key value pairs. These overwrite variables in prism_project.py. All values are intepreted as strings.",  # noqa: E501
    multiple=True
)
@click.option(
    '--context',
    type=str,
    help="Context as a dictionary. Must be a valid JSON. These overwrite variables in prism_project.py.",  # noqa: E501
    default='{}'
)
def spark_submit(
    module,
    all_downstream,
    all_upstream,
    log_level,
    full_tb,
    vars,
    context
):
    """Execute your Prism project as a PySpark job.

    <br>Examples:
    - prism spark-submit
    - prism spark-submit -m module01.py -m module02.py
    - prism spark-submit -m module01 --all-downstream
    - prism spark-submit -v VAR1=VALUE1 -v VAR2=VALUE2
    - prism spark-submit --context '{"hi": 1}'
    """
    # Convert tuple of modules to list
    modules_list: Optional[List[Any]] = _process_modules(module)

    # Check `vars` and `context`
    vars_dict = _check_vars_format(vars)
    _check_context(vars, context)

    # Namespace
    ns = argparse.Namespace()
    ns.modules = modules_list
    ns.all_upstream = all_upstream
    ns.all_downstream = all_downstream
    ns.full_tb = full_tb
    ns.log_level = log_level
    ns.vars = vars_dict
    ns.context = context
    ns.which = 'spark-submit'

    # Instantiate and run task
    task = prism.cli.spark_submit.SparkSubmitTask(ns)
    result = task.run()
    return result


if __name__ == "__main__":
    cli()
