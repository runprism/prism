"""
Main

Table of Contents
- Imports
- Functions / utils
- Main
"""

#############
## Imports ##
#############

import argparse
from prism.cli import connect, init, run, compile, spark_submit


#######################
## Functions / utils ##
#######################

def build_common_arguments_parser() -> argparse.ArgumentParser:
    """
    Build base subparser for common arguments across all commands.

    args:
        None
    returns:
        common_arguments_parser: instantiated ArgumentParser class with arguments for base subparser
    """
    common_arguments_parser = argparse.ArgumentParser(add_help=False)
    common_arguments_parser.add_argument(
        '--profiles-dir',
        required=False,
        type=str,
        help="""
        Path to the profiles directory to use for the project. Default is working project directory.
        """
    )

    common_arguments_parser.add_argument(
        '--full-tb',
        required=False,
        action='store_true',
        help="""
        Display full traceback if errors arise at any stage of the pipeline
        """
    )
    return common_arguments_parser


def build_init_subparser(sub):
    """
    Build subparser for init command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments to be included in subparser
    returns:
        None
    """
    init_sub = sub.add_parser(
        'init',
        help = """
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

    # Set default class argument to InitTask()
    init_sub.set_defaults(cls=init.InitTask, which='init')


def build_connect_subparser(sub, common_arguments_parser):
    """
    Build subparser for connect command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments to be included in subparser
    returns:
        None
    """
    connect_sub = sub.add_parser(
        'connect',
        parents=[common_arguments_parser],
        help = """
        Create a connection to a data warehouse or data processing system.
        At this time, prism supports connecting to Snowflake, PySpark, and Dbt.
        """
    )

    # Add argument for connection type
    connect_sub.add_argument(
        '--type',
        type=str,
        required=True,
        help="""
        Connection type. One of "snowflake", "pyspark", or "dbt""
        """
    )

    # Set default class argument to RunTask()
    connect_sub.set_defaults(cls=connect.ConnectTask, which='connect')


def build_compile_subparser(sub, common_arguments_parser):
    """
    Build subparser for compile command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments to be included in subparser
    returns:
        None
    """
    compile_sub = sub.add_parser(
        'compile',
        parents=[common_arguments_parser],
        help = """
        Parse pipeline inputs and outputs and create a DAG
        """
    )

    # Add argument for which script(s) to compile
    compile_sub.add_argument(
        '--modules',
        type=str,
        nargs='+',
        help="""
        Path to script(s) that you want to compile; if not specified, all modules in pipeline are compiled
        """
    )

    # Set default class argument to RunTask()
    compile_sub.set_defaults(cls=compile.CompileTask, which='compile')


def build_run_subparser(sub, common_arguments_parser):
    """
    Build subparser for run command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments to be included in subparser
    returns:
        None
    """
    run_sub = sub.add_parser(
        'run',
        parents=[common_arguments_parser],
        help = """
        Run a pipeline
        """
    )

    # Add argument for which script(s) to run
    run_sub.add_argument(
        '--modules',
        type=str,
        nargs='+',
        help="""
        Path to script(s) that you want to run; if not specified, all modules in pipeline are run
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

    # Set default class argument to RunTask()
    run_sub.set_defaults(cls=run.RunTask, which='run')


def build_spark_submit_subparser(sub, common_arguments_parser):
    """
    Build subparser for spark-submit command line argument.

    args:
        sub: special-action object (see argparse docs) to add subparsers to
        common_arguments_parser: parser with common arguments to be included in subparser
    returns:
        None
    """
    spark_submit_sub = sub.add_parser(
        'spark-submit',
        parents=[common_arguments_parser],
        help = """
        Run a pipeline as a PySpark job
        """
    )

    # Add argument for which notebook to run
    spark_submit_sub.add_argument(
        '--modules',
        type=str,
        nargs='+',
        help="""
        Path to script(s) that you want to run; if not specified, all modules in pipeline are run
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

    # Set default class argument to RunTask()
    spark_submit_sub.set_defaults(cls=spark_submit.SparkSubmitTask, which='spark-submit')


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
    build_spark_submit_subparser(subparser, common_arguments_parser)

    # Return base parser
    return base_parser


##########
## Main ##
##########

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


# EOF