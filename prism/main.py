"""
Main

Table of Contents
- Imports
- Functions / utils
- Main
"""

###########
# Imports #
###########

import argparse
from rich_argparse import RichHelpFormatter
import prism.constants
from prism.cli import (
    base,
    connect,
    init,
    run,
    compile,
    spark_submit,
    graph,
    create_agent,
    create_task,
    create_trigger,
    agent
)
import prism.exceptions
from typing import List
import sys


##############
# Arg parser #
##############

# Styles
RichHelpFormatter.styles['argparse.groups'] = 'color(139)'


# Key-value class
class KeyValue(argparse.Action):
    """
    Allow users to specify key-value pairs in their arguments. These will overwrite any
    variables in `prism_project.py`
    """

    def __call__(self,
        parser,
        namespace,
        values,
        option_string=None
    ):
        setattr(namespace, self.dest, dict())

        # Iterate through values, split each into k and value and assign to dictionary.
        for value in values:
            key, value = value.split('=')
            getattr(namespace, self.dest)[key] = value


def add_other_option_arguments(
    parser,
    options: List[str] = ["full-tb", "log-level", "vars", "context"]
):
    """
    Add other arguments to the inputted parser. These other options are shared across a
    bunch of different commands.

    args:
        parser: parser to which to add options
        options: list of option names to add
    returns:
        parser with options added
    """
    if "full-tb" in options:
        parser.add_argument(
            '--full-tb',
            required=False,
            action='store_true',
            help="""
            Display the full traceback for errors in the project; default is False
            """
        )

    if "log-level" in options:
        parser.add_argument(
            '-l',
            '--log-level',
            required=False,
            metavar="",
            type=str,
            default='info',
            help="""
            Log level, must be one of `info`, `warn`, `error`, or `debug`. Default is
            `info`
            """
        )

    # Key-value variables
    if "vars" in options:
        parser.add_argument(
            '--vars',
            required=False,
            action=KeyValue,
            metavar="A=a B=b",
            nargs='*',
            help="""
            Prism variables as key-value pairs `key=value`. These overwrite any variable
            definitions in `prism_project.py`. All values are read as strings.
            """
        )

    # Context JSON (as a string)
    if "context" in options:
        parser.add_argument(
            '--context',
            required=False,
            type=str,
            metavar="'{}'",
            default='{}',
            help="""
            Prism variables as JSON. Cannot co-exist with --vars. These overwrite any
            variable definitions in `prism_project.py`.
            """
        )
    return parser


def build_init_subparser(sub):
    """
    Build subparser for init command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    init_sub = sub.add_parser(
        'init',
        formatter_class=RichHelpFormatter,
        help="""
        Initialize a Prism project
        """,
    )

    # Sub-command options
    command_options = init_sub.add_argument_group("Command Options")

    # Add argument for project name
    command_options.add_argument(
        '--project-name',
        required=False,
        metavar="NAME",
        type=str,
        help="""
        Project name
        """
    )

    # Add argument for whether to create minimal project
    command_options.add_argument(
        '--minimal',
        required=False,
        action='store_true',
        help="""
        Create minimal project (just `prism_project.py` and `modules`). Default is False
        """
    )

    # Other options
    general_options = init_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options, ["log-level"])

    # Set default class argument to InitTask()
    init_sub.set_defaults(cls=init.InitTask, which='init')


def build_connect_subparser(sub):
    """
    Build subparser for connect command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = """
    Connect your Prism project to third-party adapters (e.g., Snowflake, Redshift, dbt,
    etc.)
    """
    connect_sub = sub.add_parser(
        'connect',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Connect-task command_options arguments
    command_options = connect_sub.add_argument_group("Command Options")

    # Add argument for connection type
    valid_connections_str = ','.join([f'`{k}`' for k in prism.constants.VALID_ADAPTERS])
    command_options.add_argument(
        '--type',
        type=str,
        required=True,
        help=f"""
        Connection type. One of {valid_connections_str}
        """
    )

    general_options = connect_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options)

    # Set default class argument to RunTask()
    connect_sub.set_defaults(cls=connect.ConnectTask, which='connect')


def build_compile_subparser(sub):
    """
    Build subparser for compile command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = """
    Parse the tasks.ref(...) calls, construct the DAG, and generate the manifest
    """
    compile_sub = sub.add_parser(
        'compile',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter,
    )

    # General options
    general_options = compile_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(
        general_options, ["full-tb", "log-level"]
    )

    # Set default class argument to RunTask()
    compile_sub.set_defaults(
        cls=compile.CompileTask,
        modules=None,
        which='compile',
        vars=None,
        context='{}'
    )


def build_create_subparser(sub):
    """
    Build subparser for `create` command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = "Create and/or update project components (e.g., tasks, triggers, agents)"
    create_parser = sub.add_parser(
        'create',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )
    create_sub = create_parser.add_subparsers(
        title="Sub-commands",
    )

    # ----------------------------------------------------------------------------------
    # Create agent parser

    desc = "Create an agent YML configuration file"
    create_agent_sub = create_sub.add_parser(
        'agent',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Subcommand options
    subcommand_options = create_agent_sub.add_argument_group("Subcommand Options")

    # Add argument for agent type
    valid_agents_str = ",".join(["`" + t + "`" for t in prism.constants.VALID_AGENTS])
    subcommand_options.add_argument(
        '--type',
        type=str,
        required=True,
        help=f"""
        Agent type. Accepted types are {valid_agents_str}
        """
    )

    # File name
    subcommand_options.add_argument(
        '-f',
        '--file',
        default="./agent.yml",
        type=str,
        required=False,
        help="""
        File path for agent YML configuration. Default is `./agent.yml`.
        """
    )

    # General options
    general_options = create_agent_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(
        general_options, ["full-tb", "log-level"]
    )

    # Set default class argument to RunTask()
    create_agent_sub.set_defaults(
        cls=create_agent.CreateAgentTask,
        which='agent',
        vars=None,
        context='{}'
    )

    # ----------------------------------------------------------------------------------
    # Create task parser

    desc = "Create new tasks for your project"
    create_task_sub = create_sub.add_parser(
        'task',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Subcommand options
    subcommand_options = create_task_sub.add_argument_group("Subcommand Options")

    # Add argument for task type
    valid_tasks_str = ','.join([f'`{k}`' for k in prism.constants.VALID_TASK_TYPES])
    subcommand_options.add_argument(
        '--type',
        type=str,
        required=False,
        default="python",
        help=f"""
        Task type. One of {valid_tasks_str}. Default is `python`
        """
    )

    # Add argument for number of tasks to create
    subcommand_options.add_argument(
        '-n',
        '--number',
        type=int,
        required=False,
        metavar="N",
        default=1,
        help="""
        Number of tasks to create. Default is 1.
        """
    )

    # Add argument for the task name
    subcommand_options.add_argument(
        '--name',
        type=str,
        required=False,
        default="new_task",
        help="""
        Task name. If only a single task is requested, then the task will be named
        `<task_name>.py`. If multiple tasks are requested, then the tasks will be named
        `<task_name>_<number>.py`. Tasks should have short, all-lowercase names.
        Underscores can be used in the module name if it improves readability.
        """
    )

    # Add argument for the directory in which the new tasks should live
    subcommand_options.add_argument(
        '--dir',
        type=str,
        required=False,
        default="",
        help="""
        Folder within the `modules` directory in which the new tasks should live. If not
        specified, then new tasks will be dumpted into `modules/`
        """
    )

    # General options
    general_options = create_task_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(
        general_options, ["full-tb", "log-level"]
    )

    # Set default class argument to RunTask()
    create_task_sub.set_defaults(
        cls=create_task.CreateTaskTask,
        which='task',
        vars=None,
        context='{}'
    )

    # ----------------------------------------------------------------------------------
    # Create trigger parser

    desc = """
    Create a triggers YML file at the TRIGGERS_YML_PATH in `prism_project.py`.
    """
    trigger_sub = create_sub.add_parser(
        'trigger',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Subcommand options
    subcommand_options = trigger_sub.add_argument_group("Subcommand Options")

    # Add argument for triggerion type
    subcommand_options.add_argument(
        '--type',
        type=str,
        required=True,
        help="""
        Trigger type. As of now, only acceptable value is `function`
        """
    )

    # General options
    general_options = trigger_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(
        general_options, ["full-tb", "log-level"]
    )

    # Set default class argument to RunTask()
    trigger_sub.set_defaults(
        cls=create_trigger.CreateTriggerTask,
        which='trigger',
        vars=None,
        context='{}'
    )


def _exec_project_args(sub):
    """
    Arguments to add for commands that actually execute a project
    """

    # Add argument for which script(s) to run
    sub.add_argument(
        '--modules',
        type=str,
        nargs='*',
        metavar="X.py Y.py",
        required=False,
        help="""
        Path to script(s) that you want to run; if not specified, all modules in
        project are run
        """
    )

    # Add argument for whether to run all upstream modules
    sub.add_argument(
        '--all-upstream',
        required=False,
        action='store_true',
        help="""
        Run all modules upstream of --modules
        """
    )

    # Add argument for whether to run all upstream modules
    sub.add_argument(
        '--all-downstream',
        required=False,
        action='store_true',
        help="""
        Run all modules downstream of --modules
        """
    )


def build_run_subparser(sub):
    """
    Build subparser for run command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = "Execute your Prism project"
    run_sub = sub.add_parser(
        'run',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Command options
    command_options = run_sub.add_argument_group("Command Options")
    _exec_project_args(command_options)

    # General options
    general_options = run_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options)

    # Set default class argument to RunTask()
    run_sub.set_defaults(cls=run.RunTask, which='run')


def build_spark_submit_subparser(sub):
    """
    Build subparser for spark-submit command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = "Execute your Prism project as a PySpark job"
    spark_submit_sub = sub.add_parser(
        'spark-submit',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Command options
    command_options = spark_submit_sub.add_argument_group("Command Options")
    _exec_project_args(command_options)

    # General options
    general_options = spark_submit_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options)

    # Set default class argument to RunTask()
    spark_submit_sub.set_defaults(
        cls=spark_submit.SparkSubmitTask, which='spark-submit'
    )


def build_graph_subparser(sub):
    """
    Build subparser for graph command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = "Visualize your Prism project as a DAG in an interactive UI"
    graph_sub = sub.add_parser(
        'graph',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    # Add argument for the port to use
    command_options = graph_sub.add_argument_group("Command Options")
    command_options.add_argument(
        '--port',
        type=int,
        default=8080,
        help="""
        Port to use for HTTP request; default is 8080
        """
    )

    # Add argument for whether Prism should open a webbrowser with the dos
    command_options.add_argument(
        '--no-browser',
        action='store_true',
        help="""
        Overwrite default behavior and do not open a tab in the default web browser with
        the docs UI
        """
    )

    # General options
    general_options = graph_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(
        general_options, ["full-tb", "log-level"]
    )

    # Set default class argument to RunTask()
    graph_sub.set_defaults(
        cls=graph.GraphTask,
        all_downstream=True,
        which='graph',
        vars=None,
        context='{}'
    )


def build_agent_parser(sub):
    """
    Build subparser for graph command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
    returns:
        None
    """
    desc = """
    Execute your Prism using third-party agents, (e.g., Docker containers, EC2
    instances, EMR clusters, and more).
    """
    agent_parser = sub.add_parser(
        'agent',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    agent_sub = agent_parser.add_subparsers(
        title="Subcommands",
        description=desc,
    )

    # ----------------------------------------------------------------------------------
    # Add a subparser for the "apply" command. This command builds the agent.

    desc = """
    Build your agent using a configuration YML
    """
    agent_apply_sub = agent_sub.add_parser(
        'apply',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    subcommand_options = agent_apply_sub.add_argument_group('Subcommand Options')
    subcommand_options.add_argument(
        "-f",
        "--file",
        required=True,
        help="""
        Path to agent configuration YML
        """
    )

    # General options
    general_options = agent_apply_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(
        general_options, ["full-tb", "log-level"]
    )

    # Set defaults
    agent_apply_sub.set_defaults(
        cls=agent.AgentTask,
        which='agent-apply',
        vars=None,
        context='{}'
    )

    # ----------------------------------------------------------------------------------
    # Add a subparser for the "run" command. This command runs the project on the agent.

    desc = """
    Run your project using an agent
    """
    agent_run_sub = agent_sub.add_parser(
        'run',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    subcommand_options = agent_run_sub.add_argument_group('Subcommand Options')
    subcommand_options.add_argument(
        "-f",
        "--file",
        required=True,
        help="""
        Path to agent configuration YML
        """
    )

    # Add the `run` arguments to this subparser
    run_options = agent_run_sub.add_argument_group('Run Command Options')
    _exec_project_args(run_options)

    # General options
    general_options = agent_run_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options)

    # Set defaults
    agent_run_sub.set_defaults(
        cls=agent.AgentTask,
        which='agent-run'
    )

    # ----------------------------------------------------------------------------------
    # Add a subparser for the "build" command. This command combines the `apply` and
    # `run` commands (i.e., it re-builds the agent and then runs the project on it.)

    desc = """
    Build your agent using a configuration YML and then run your project on the
    newly-built agent
    """
    agent_build_sub = agent_sub.add_parser(
        'build',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    subcommand_options = agent_build_sub.add_argument_group('Subcommand Options')
    subcommand_options.add_argument(
        "-f",
        "--file",
        required=True,
        help="""
        Path to agent configuration YML.
        """
    )

    # Add the `run` arguments to this subparser
    run_options = agent_build_sub.add_argument_group('Run Command Options')
    _exec_project_args(run_options)

    # General options
    general_options = agent_build_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options)

    # Set defaults
    agent_build_sub.set_defaults(cls=agent.AgentTask, which='agent-build')

    # ----------------------------------------------------------------------------------
    # Add a subparser for the "run" command. This command runs the project on the agent.

    desc = """
    Delete your agent
    """
    agent_delete_sub = agent_sub.add_parser(
        'delete',
        help=desc,
        description=desc,
        formatter_class=RichHelpFormatter
    )

    subcommand_options = agent_delete_sub.add_argument_group('Subcommand Options')
    subcommand_options.add_argument(
        "-f",
        "--file",
        required=True,
        help="""
        Path to agent configuration YML
        """
    )

    # General options
    general_options = agent_delete_sub.add_argument_group("General Options")
    general_options = add_other_option_arguments(general_options)

    # Set defaults
    agent_delete_sub.set_defaults(
        cls=agent.AgentTask,
        which='agent-delete'
    )


def build_full_arg_parser() -> argparse.ArgumentParser:
    """
    Build full argument parser
    """
    # Base parser
    base_parser = argparse.ArgumentParser(formatter_class=RichHelpFormatter)
    subparser = base_parser.add_subparsers(
        title="Commands"
    )

    # Add subparsers
    build_init_subparser(subparser)
    build_compile_subparser(subparser)
    build_connect_subparser(subparser)
    build_create_subparser(subparser)
    build_run_subparser(subparser)
    build_spark_submit_subparser(subparser)
    build_graph_subparser(subparser)
    build_agent_parser(subparser)

    # Return base parser
    return base_parser


########
# Main #
########

def main(argv=None, bool_return: bool = False):
    """
    Parse argument, extract task, and execute
    """
    # Base parser
    base_parser = build_full_arg_parser()

    # Parse args
    parsed = base_parser.parse_args(argv)

    # If `vars` and `context` are both specified, raise an error
    if hasattr(parsed, "vars") and hasattr(parsed, "context"):
        if parsed.vars is not None and parsed.context != '{}':
            raise prism.exceptions.RuntimeException(
                message="`vars` and `context` are mutually exclusive arguments"
            )
    task = parsed.cls.task_from_args(parsed)
    result: base.TaskRunReturnResult = task.run()

    # Return result
    if bool_return:
        return result

    # Exit gracefully
    if result.has_error:
        sys.exit(1)
    else:
        sys.exit(0)
