"""
Mixin classes for compile task

Table of Contents
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import re
import argparse
from pathlib import Path
from typing import List, Optional

# Prism-specific imports
import prism.cli.base
import prism.exceptions
import prism.constants
from prism.infra import compiler
from prism.infra.project import PrismProject
from prism.parsers.ast_parser import AstParser


####################
# Class definition #
####################

class CompileMixin():
    """
    Mixin for compile task
    """

    def is_valid_script_name(self,
        script: str
    ) -> bool:
        """
        Check if script name is valid. Script must start with [A-Za-z] character to be
        valid.

        args:
            script: script name
        returns:
            prism.exceptions.CompileException
        """
        if len(re.findall(r'\.py$', script)) == 0:
            pass
        else:
            if len(re.findall(r'(?i)^[a-z]', script)) > 0:
                return True
        raise prism.exceptions.CompileException(
            message=f'`{script}` is not valid name; script must start with [a-zA-Z] to be compiled correctly'  # noqa: E501
        )

    def get_tasks_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Get directory containing tasks

        args:
            project_dir: project directory
        returns:
            tasks directory
        """
        tasks_dir = project_dir / 'tasks'
        modules_dir = project_dir / 'modules'
        if not tasks_dir.is_dir():

            # If the `modules` directory is found, then raise a warning
            if modules_dir.is_dir():
                prism.prism_logging.fire_console_event(
                    prism.prism_logging.ModulesFolderDeprecated(),
                    log_level='warn'
                )
                return modules_dir

            # Otherwise, raise a warning
            raise prism.exceptions.CompileException(
                message=f'`tasks` directory not found in `{str(project_dir)}`'
            )
        return tasks_dir

    def get_modules(self,
        tasks_dir: Path,
        prefix: Path = Path('')
    ) -> List[Path]:
        """
        Get all Python modules from `tasks_dir`

        args:
            tasks_dir: tasks directory
            prefix: prefix to use for directories in tasks directory
        returns:
            list of tasks in directory (as pathlib Path objects)
        """
        modules = []
        for path in tasks_dir.iterdir():

            # If object is a directory...
            if path.is_dir():

                # If parent directory is the tasks folder, set the prefix to the name
                # of the directory. For example, if the tasks folder has a directory
                # called "extraction", then the tasks within "extraction" should be
                # stored as "extraction/..."
                if str(prefix) == '.':
                    modules.extend(self.get_modules(path, prefix=Path(path.name)))

                # If parent directory is not the tasks folder, set the prefix to the
                # parent prefix + the name of the directory. Using the above example, if
                # the "extraction" folder has a directory called "step1", then the
                # tasks within "step1" should be stored as "extraction/step1/..."
                else:
                    modules.extend(
                        self.get_modules(path, prefix=Path(prefix) / path.name)
                    )

            # If object is a file
            elif path.is_file():

                # If file is a python file...
                if len(re.findall(r'\.py$', path.name)) > 0:

                    # If parent directory is the tasks folder, then just add the
                    # python file name
                    if str(prefix) == '.':
                        modules += [Path(path.name)]

                    # If parent directory is not the tasks folder, then add prefix to
                    # python file name
                    else:
                        modules += [Path(prefix) / Path(path.name)]

        return modules

    def parse_all_tasks(self,
        all_modules: List[Path],
        tasks_dir: Path,
    ) -> List[AstParser]:
        """
        Parse all the tasks in `tasks_dir`

        args:
            tasks_dir: directory containing all tasks
        returns:
            list of AstParsers
        """
        parser_list = []
        for _m in all_modules:
            parser = AstParser(Path(_m), tasks_dir)
            parser_list.append(parser)

        return parser_list

    def get_task_names(self,
        parsed_tasks: List[AstParser]
    ) -> List[str]:
        all_task_names = []
        for _p in parsed_tasks:
            module_name_no_py = re.sub(r'\.py$', '', str(_p.task_relative_path))
            all_task_names += [
                f"{module_name_no_py}.{_n.name}" for _n in _p.prism_task_nodes
            ]
        return all_task_names

    def user_arg_tasks(self,
        args: argparse.Namespace,
        tasks_dir: Path,
        all_parsed_tasks: List[AstParser],
    ) -> List[str]:
        """
        Process user arg tasks

        args:
            args: user arguments
            tasks_dir: tasks directory
        returns:
            list of processed user arg tasks
        """
        # Get the tasks argument
        try:
            raw_tasks = args.tasks

            # If the user didn't specify any tasks, return all of the tasks
            if raw_tasks is None:
                return self.get_task_names(all_parsed_tasks)

            # Otherwise, iterate through the user arguments
            else:
                processed_tasks: List[str] = []
                for m in raw_tasks:
                    curr_m_tasks: List[str] = []

                    # Check if * is used. If so, get the tasks contained with all
                    # modules in the parent path (i.e., if `extract/*` is used, get the
                    # tasks for all modules in `extract/`).
                    m_split = m.split('/')
                    if m_split[-1] == '*':
                        parent = '/'.join([m_comp for m_comp in m_split[:-1]])

                        # Parsed subset
                        parsed_subset = [
                            _p for _p in all_parsed_tasks if _p.task_relative_path.parent == Path(parent)  # noqa: E501
                        ]
                        curr_m_tasks = self.get_task_names(parsed_subset)

                    # Check if path is a directory
                    elif Path(tasks_dir / m).is_dir():
                        raise prism.exceptions.CompileException(
                            message=f'invalid --task argument `{m}`...--module must either by `<module_name>` or `<module_name>.<task_name>`'  # noqa: E501
                        )

                    # Otherwise, the task must be either a module or a module.task
                    else:
                        task_arg_regex = r'(?i)^[a-z0-9\_\-\/\*]+\.?(?:[a-z0-9\_\-\/\*]+|py)?$'  # noqa: E501
                        if len(re.findall(task_arg_regex, m)) == 0:  # noqa: E501
                            raise prism.exceptions.ParserException(
                                f'invalid --task argument `{m}`...--module must either by `<module_name>` or `<module_name>.<task_name>`'  # noqa: E501
                            )

                        # If the task contains `.py` at the end, remove it. We already
                        # throw a warning with click.
                        if len(re.findall(r'\.py$', m)) > 0:
                            m = re.sub(r'\.py$', '', m)

                        # Check format of the task argument
                        m_split = m.split(".")
                        if not Path(tasks_dir / f'{m_split[0]}.py').is_file():
                            raise prism.exceptions.CompileException(
                                f'could not find module `{m_split[0]}` in `{tasks_dir.name}/`, so could not parse the corresponding tasks'  # noqa: E501
                            )

                        # Otherwise, get the parsed task and task names
                        parsed_subset = [
                            _p for _p in all_parsed_tasks if _p.task_relative_path == Path(f"{m_split[0]}.py")  # noqa: E501
                        ]
                        curr_m_tasks = self.get_task_names(parsed_subset)

                        # If the user wants a specific task within the module, then
                        # update `processed_tasks`
                        if len(m_split) > 1:
                            curr_m_tasks = [
                                p for p in curr_m_tasks if p.split(".")[1] == m_split[1]
                            ]
                    processed_tasks += curr_m_tasks

            return processed_tasks

        # If --task argument not specified, then get all tasks
        except AttributeError:
            return self.get_task_names(all_parsed_tasks)

    def get_compiled_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Get directory containing compiled tasks

        args:
            project_dir: project directory
        returns:
            compiled_dir: directory with compiled tasks
        """
        compiled_dir = project_dir / '.compiled'
        return compiled_dir

    def create_compiled_dir(self,
        project_dir: Path
    ):
        """
        Create folder for compiled tasks in project directory

        args:
            project_dir: project directory
            tasks: list of tasks to compile
        returns:
            None
        """

        # Create compiled directory
        compiled_tasks_path = self.get_compiled_dir(project_dir)
        if not compiled_tasks_path.is_dir():
            compiled_tasks_path.mkdir(parents=True, exist_ok=True)
        return compiled_tasks_path

    def compile_dag(self,
        project_dir: Path,
        tasks_dir: Path,
        compiled_dir: Path,
        all_parsed_tasks: List[AstParser],
        user_arg_tasks: List[str],
        user_arg_all_downstream: bool = True,
        project: Optional[PrismProject] = None
    ) -> compiler.CompiledDag:
        """
        Wrapper for the `compile` method in the DagCompiler class

        args:
            project_dir: project directory
            tasks_dir: directory containing all tasks
            compiled_dir: directory in which to place manifest
            all_parsed_tasks: parsed tasks in `tasks_dir`
            user_arg_tasks: tasks passed in user argument
            user_arg_all_downstream: boolean indicating whether the user wants to run
                all tasks downstream of inputted args
            project: PrismProject
        returns:
            CompiledDag object
        """
        # All tasks
        all_tasks = self.get_task_names(all_parsed_tasks)

        dag_compiler = compiler.DagCompiler(
            project_dir,
            tasks_dir,
            compiled_dir,
            all_tasks,
            all_parsed_tasks,
            user_arg_tasks,
            user_arg_all_downstream,
            project
        )
        compiled_dag = dag_compiler.compile()

        # Check that all tasks in topological sort are in project
        for m in compiled_dag.topological_sort:
            if m not in all_tasks:
                raise prism.exceptions.CompileException(
                    message=f'task `{m}` not found in project'
                )

        # Otherwise, return
        return compiled_dag
