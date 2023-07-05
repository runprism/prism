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

    def get_models_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Get directory containing models

        args:
            project_dir: project directory
        returns:
            models directory
        """
        models_dir = project_dir / 'models'
        modules_dir = project_dir / 'modules'
        if not models_dir.is_dir():

            # If the `modules` directory is found, then raise a warning
            if modules_dir.is_dir():
                prism.prism_logging.fire_console_event(
                    prism.prism_logging.ModulesFolderDeprecated(),
                    log_level='warn'
                )
                return modules_dir

            # Otherwise, raise a warning
            raise prism.exceptions.CompileException(
                message=f'`models` directory not found in `{str(project_dir)}`'
            )
        return models_dir

    def get_modules(self,
        models_dir: Path,
        prefix: Path = Path('')
    ) -> List[Path]:
        """
        Get all Python modules from `models_dir`

        args:
            models_dir: models directory
            prefix: prefix to use for directories in models directory
        returns:
            list of models in directory (as pathlib Path objects)
        """
        modules = []
        for path in models_dir.iterdir():

            # If object is a directory...
            if path.is_dir():

                # If parent directory is the models folder, set the prefix to the name
                # of the directory. For example, if the models folder has a directory
                # called "extraction", then the models within "extraction" should be
                # stored as "extraction/..."
                if str(prefix) == '.':
                    modules.extend(self.get_modules(path, prefix=Path(path.name)))

                # If parent directory is not the models folder, set the prefix to the
                # parent prefix + the name of the directory. Using the above example, if
                # the "extraction" folder has a directory called "step1", then the
                # models within "step1" should be stored as "extraction/step1/..."
                else:
                    modules.extend(
                        self.get_modules(path, prefix=Path(prefix) / path.name)
                    )

            # If object is a file
            elif path.is_file():

                # If file is a python file...
                if len(re.findall(r'\.py$', path.name)) > 0:

                    # If parent directory is the models folder, then just add the
                    # python file name
                    if str(prefix) == '.':
                        modules += [Path(path.name)]

                    # If parent directory is not the models folder, then add prefix to
                    # python file name
                    else:
                        modules += [Path(prefix) / Path(path.name)]

        return modules

    def parse_all_models(self,
        all_modules: List[Path],
        models_dir: Path,
    ) -> List[AstParser]:
        """
        Parse all the models in `models_dir`

        args:
            models_dir: directory containing all models
        returns:
            list of AstParsers
        """
        parser_list = []
        for _m in all_modules:
            parser = AstParser(Path(_m), models_dir)
            parser_list.append(parser)

        return parser_list

    def get_model_names(self,
        parsed_models: List[AstParser]
    ):
        all_model_names = []
        for _p in parsed_models:
            module_name_no_py = re.sub(r'\.py$', '', str(_p.model_relative_path))
            all_model_names += [
                f"{module_name_no_py}.{_n.name}" for _n in _p.prism_task_nodes
            ]
        return all_model_names

    def user_arg_models(self,
        args: argparse.Namespace,
        models_dir: Path,
        all_parsed_models: List[AstParser],
    ) -> List[Path]:
        """
        Process user arg models

        args:
            args: user arguments
            models_dir: models directory
        returns:
            list of processed user arg models
        """
        # Get the models argument
        try:
            raw_models = args.models

            # If the user didn't specify any models, return all of the models
            if raw_models is None:
                return self.get_model_names(all_parsed_models)

            # Otherwise, iterate through the user arguments
            else:
                processed_models: Optional[List[str]] = None
                for m in raw_models:

                    # Check if * is used. If so, get the models contained with all
                    # modules in the parent path (i.e., if `extract/*` is used, get the
                    # models for all modules in `extract/`).
                    m_split = m.split('/')
                    if m_split[-1] == '*':
                        parent = '/'.join([m_comp for m_comp in m_split[:-1]])

                        # Parsed subset
                        parsed_subset = [
                            _p for _p in all_parsed_models if _p.model_relative_path.parent == Path(parent)  # noqa: E501
                        ]
                        processed_models = self.get_model_names(parsed_subset)

                    # Check if path is a directory
                    elif Path(models_dir / m).is_dir():
                        raise prism.exceptions.CompileException(
                            message=f'invalid --model argument `{m}`...--module must either by `<module_name>` or `<module_name>.<model_name>`'  # noqa: E501
                        )

                    # Otherwise, the model must be either a module or a module.model
                    else:
                        if len(re.findall(r'(?i)^[a-z0-9\_\-\/\*\.]+(?:\.py)?$', m)) == 0:  # noqa: E501
                            raise prism.exceptions.ParserException(
                                f'invalid --model argument `{m}`...--module must either by `<module_name>` or `<module_name>.<model_name>`'  # noqa: E501
                            )

                        # If the model contains `.py` at the end, remove it. We already
                        # throw a warning with click.
                        if len(re.findall(r'\.py$', m)) > 0:
                            m = re.sub(r'\.py$', '', m)

                        # Check format of the model argument
                        m_split = m.split(".")
                        if not Path(models_dir / f'{m_split[0]}.py').is_file():
                            raise prism.exceptions.CompileException(
                                f'could not find module `{m_split[0]}` in `{models_dir.name}/`, so could not parse the corresponding models'  # noqa: E501
                            )

                        # Otherwise, get the parsed model and model names
                        parsed_subset = [
                            _p for _p in all_parsed_models if _p.model_relative_path == Path(f"{m_split[0]}.py")  # noqa: E501
                        ]
                        processed_models = self.get_model_names(parsed_subset)

                        # If the user wants a specific model within the module, then
                        # update `processed_models`
                        if len(m_split) > 1:
                            processed_models = [
                                p for p in processed_models if p.split(".")[1] == m_split[1]  # noqa: E501
                            ]

            return processed_models

        # If --model argument not specified, then get all models
        except AttributeError:
            return self.get_model_names(all_parsed_models)

    def get_compiled_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Get directory containing compiled models

        args:
            project_dir: project directory
        returns:
            compiled_dir: directory with compiled models
        """
        compiled_dir = project_dir / '.compiled'
        return compiled_dir

    def create_compiled_dir(self,
        project_dir: Path
    ):
        """
        Create folder for compiled models in project directory

        args:
            project_dir: project directory
            models: list of models to compile
        returns:
            None
        """

        # Create compiled directory
        compiled_models_path = self.get_compiled_dir(project_dir)
        if not compiled_models_path.is_dir():
            compiled_models_path.mkdir(parents=True, exist_ok=True)
        return compiled_models_path

    def compile_dag(self,
        project_dir: Path,
        models_dir: Path,
        compiled_dir: Path,
        all_parsed_models: List[AstParser],
        user_arg_models: List[str],
        user_arg_all_downstream: bool = True,
        project: Optional[PrismProject] = None
    ) -> compiler.CompiledDag:
        """
        Wrapper for the `compile` method in the DagCompiler class

        args:
            project_dir: project directory
            models_dir: directory containing all models
            compiled_dir: directory in which to place manifest
            all_parsed_models: parsed models in `models_dir`
            user_arg_models: models passed in user argument
            user_arg_all_downstream: boolean indicating whether the user wants to run
                all models downstream of inputted args
            project: PrismProject
        returns:
            CompiledDag object
        """
        # All models
        all_models = self.get_model_names(all_parsed_models)

        dag_compiler = compiler.DagCompiler(
            project_dir,
            models_dir,
            compiled_dir,
            all_models,
            all_parsed_models,
            user_arg_models,
            user_arg_all_downstream,
            project
        )
        compiled_dag = dag_compiler.compile()

        # Check that all models in topological sort are in project
        for m in compiled_dag.topological_sort:
            if m not in all_models:
                raise prism.exceptions.CompileException(
                    message=f'model `{m}` not found in project'
                )

        # Otherwise, return
        return compiled_dag
