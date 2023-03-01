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
import prism.constants
from prism.cli import (
    connect,
    init,
    run,
    compile,
    spark_submit,
    graph,
    create_task,
    create_trigger,
)


##############
# Arg parser #
##############

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


def build_common_arguments_parser() -> argparse.ArgumentParser:
    """
    Build base subparser for common arguments across all commands.

    args:
        None
    returns:
        common_arguments_parser: instantiated ArgumentParser class with arguments for
        base subparser
    """
    common_arguments_parser = argparse.ArgumentParser(add_help=False)

    common_arguments_parser.add_argument(
        '--full-tb',
        required=False,
        action='store_true',
        help="""
        Display full traceback if errors arise at any stage of the pipeline
        """
    )

    common_arguments_parser.add_argument(
        '--log-level',
        required=False,
        type=str,
        default='info',
        help="""
        Set the log level; must be one of `info`, `warn`, `error`, or `critical`
        """
    )

    # Key-value variables
    common_arguments_parser.add_argument(
        '--vars',
        required=False,
        action=KeyValue,
        nargs='*',
        help="""
        Prism variables as key-value pairs `key=value`. These overwrite any variable
        definitions in `prism_project.py`. All values are read as strings.
        """
    )

    # Context JSON (as a string)
    common_arguments_parser.add_argument(
        '--context',
        required=False,
        type=str,
        default='{}',
        help="""
        Prism variables as JSON. Cannot co-exist with --vars. These overwrite any
        variable definitions in `prism_project.py`.
        """
    )
    return common_arguments_parser


def build_init_subparser(sub):
    """
    Build subparser for init command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    init_sub = sub.add_parser(
        'init',
        help="""
        Initialize a pipeline
        """
    )

    # Add argument for project name
    init_sub.add_argument(
        '--project-name',
        type=str,
        help="""
        Project name
        """
    )

    # Add argument for whether to create minimal project
    init_sub.add_argument(
        '--minimal',
        required=False,
        action='store_true',
        help="""
        Create minimal project (just `prism_project.py` and `modules`)
        """
    )

    init_sub.add_argument(
        '--log-level',
        required=False,
        type=str,
        default='info',
        help="""
        Set the log level; must be one of `info`, `warn`, `error`, or `critical`
        """
    )

    # Set default class argument to InitTask()
    init_sub.set_defaults(cls=init.InitTask, which='init')


def build_connect_subparser(sub, common_arguments_parser):
    """
    Build subparser for connect command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    connect_sub = sub.add_parser(
        'connect',
        parents=[common_arguments_parser],
        help="""
        Create a connection to a data warehouse or data processing system.
        At this time, prism supports connecting to Snowflake, PySpark, and Dbt.
        """
    )

    # Add argument for connection type
    valid_connections_str = ','.join([f'`{k}`' for k in prism.constants.VALID_ADAPTERS])
    connect_sub.add_argument(
        '--type',
        type=str,
        required=True,
        help=f"""
        Connection type. One of {valid_connections_str}
        """
    )

    # Set default class argument to RunTask()
    connect_sub.set_defaults(cls=connect.ConnectTask, which='connect')


def build_compile_subparser(sub, common_arguments_parser):
    """
    Build subparser for compile command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    compile_sub = sub.add_parser(
        'compile',
        parents=[common_arguments_parser],
        help="""
        Parse pipeline inputs and outputs and create a DAG
        """
    )

    # Set default class argument to RunTask()
    compile_sub.set_defaults(cls=compile.CompileTask, modules=None, which='compile')


def build_create_subparser(sub, common_arguments_parser):
    """
    Build subparser for `create` command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    create_parser = sub.add_parser(
        'create',
        parents=[common_arguments_parser],
    )
    create_sub = create_parser.add_subparsers(
        help="""
        Create project components
        """
    )

    # ----------------------------------------------------------------------------------
    # Create task parser

    create_task_sub = create_sub.add_parser(
        'task',
        help="""
        Create new tasks for your project
        """
    )

    # Add argument for task type
    valid_tasks_str = ','.join([f'`{k}`' for k in prism.constants.VALID_TASK_TYPES])
    create_task_sub.add_argument(
        '--type',
        type=str,
        required=False,
        default="python",
        help=f"""
        Task type. One of {valid_tasks_str}
        """
    )

    # Add argument for number of tasks to create
    create_task_sub.add_argument(
        '-n',
        '--number',
        type=int,
        required=False,
        default=1,
        help="""
        Number of tasks to create. Default is 1.
        """
    )

    # Add argument for the task name
    create_task_sub.add_argument(
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
    create_task_sub.add_argument(
        '--dir',
        type=str,
        required=False,
        default="",
        help="""
        Folder within the `modules` directory in which the new tasks should live. If not
        specified, then new tasks will be dumpted into `modules/`
        """
    )

    # Set default class argument to RunTask()
    create_task_sub.set_defaults(cls=create_task.CreateTaskTask, which='task')

    # ----------------------------------------------------------------------------------
    # Create trigger parser

    trigger_sub = create_sub.add_parser(
        'trigger',
        help="""
        Create a triggers.yml file
        """
    )

    # Add argument for triggerion type
    valid_triggers_str = ','.join(
        [f'`{k}`' for k in prism.constants.VALID_TRIGGER_TYPES]
    )
    trigger_sub.add_argument(
        '--type',
        type=str,
        required=True,
        help=f"""
        Trigger type. One of {valid_triggers_str}
        """
    )

    # Set default class argument to RunTask()
    trigger_sub.set_defaults(cls=create_trigger.CreateTriggerTask, which='trigger')


def build_run_subparser(sub, common_arguments_parser):
    """
    Build subparser for run command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    run_sub = sub.add_parser(
        'run',
        parents=[common_arguments_parser],
        help="""
        Run a pipeline
        """
    )

    # Add argument for which script(s) to run
    run_sub.add_argument(
        '--modules',
        type=str,
        nargs='+',
        help="""
        Path to script(s) that you want to run; if not specified, all modules in
        pipeline are run
        """
    )

    # Add argument for whether to run all upstream modules
    run_sub.add_argument(
        '--all-upstream',
        required=False,
        action='store_true',
        help="""
        Run all modules upstream of explicit run set
        """
    )

    # Add argument for whether to run all upstream modules
    run_sub.add_argument(
        '--all-downstream',
        required=False,
        action='store_true',
        help="""
        Run all modules downstream of explicit run set
        """
    )

    # Set default class argument to RunTask()
    run_sub.set_defaults(cls=run.RunTask, which='run')


def build_spark_submit_subparser(sub, common_arguments_parser):
    """
    Build subparser for spark-submit command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    spark_submit_sub = sub.add_parser(
        'spark-submit',
        parents=[common_arguments_parser],
        help="""
        Run a pipeline as a PySpark job
        """
    )

    # Add argument for which notebook to run
    spark_submit_sub.add_argument(
        '--modules',
        type=str,
        nargs='+',
        help="""
        Path to script(s) that you want to run; if not specified, all modules in
        pipeline are run
        """
    )

    # Add argument for whether to run all upstream modules
    spark_submit_sub.add_argument(
        '--all-upstream',
        required=False,
        action='store_true',
        help="""
        Run all modules upstream of explicit run set
        """
    )

    # Add argument for whether to run all downstream modules
    spark_submit_sub.add_argument(
        '--all-downstream',
        required=False,
        action='store_true',
        help="""
        Run all modules downstream of explicit run set
        """
    )

    # Set default class argument to RunTask()
    spark_submit_sub.set_defaults(
        cls=spark_submit.SparkSubmitTask, which='spark-submit'
    )


def build_graph_subparser(sub, common_arguments_parser):
    """
    Build subparser for graph command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments
    returns:
        None
    """
    graph_sub = sub.add_parser(
        'graph',
        parents=[common_arguments_parser],
        help="""
        Generate the DAG UI vizualizer
        """
    )

    # Add argument for the port to use
    graph_sub.add_argument(
        '--port',
        type=int,
        default=8080,
        help="""
        Port to use for HTTP request; default is 8080
        """
    )

    # Add argument for whether Prism should open a webbrowser with the dos
    graph_sub.add_argument(
        '--no-browser',
        action='store_true',
        help="""
        Overwrite default behavior and do not open a tab in the default web browser with
        the docs UI
        """
    )

    # Set default class argument to RunTask()
    graph_sub.set_defaults(cls=graph.GraphTask, all_downstream=True, which='graph')


def build_full_arg_parser() -> argparse.ArgumentParser:
    """
    Build full argument parser
    """
    # Common arguments parser
    common_arguments_parser = build_common_arguments_parser()

    # Base parser
    base_parser = argparse.ArgumentParser()
    subparser = base_parser.add_subparsers()

    # Add subparsers
    build_init_subparser(subparser)
    build_run_subparser(subparser, common_arguments_parser)
    build_compile_subparser(subparser, common_arguments_parser)
    build_connect_subparser(subparser, common_arguments_parser)
    build_create_subparser(subparser, common_arguments_parser)
    build_spark_submit_subparser(subparser, common_arguments_parser)
    build_graph_subparser(subparser, common_arguments_parser)

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
    task = parsed.cls.task_from_args(parsed)
    result = task.run()

    # Return result
    if bool_return:
        return result
