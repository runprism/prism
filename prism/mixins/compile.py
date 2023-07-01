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
        if not models_dir.is_dir():
            raise prism.exceptions.CompileException(
                message=f'`models` directory not found in `{str(project_dir)}`'
            )
        return models_dir

    def get_models(self,
        models_dir: Path,
        prefix: Path = Path('')
    ) -> List[Path]:
        """
        Get all Python models from `models_dir`

        args:
            models_dir: models directory
            prefix: prefix to use for directories in models directory
        returns:
            list of models in directory (as pathlib Path objects)
        """
        models = []
        for path in models_dir.iterdir():

            # If object is a directory...
            if path.is_dir():

                # If parent directory is the models folder, set the prefix to the name
                # of the directory. For example, if the models folder has a directory
                # called "extraction", then the models within "extraction" should be
                # stored as "extraction/..."
                if str(prefix) == '.':
                    models.extend(self.get_models(path, prefix=Path(path.name)))

                # If parent directory is not the models folder, set the prefix to the
                # parent prefix + the name of the directory. Using the above example, if
                # the "extraction" folder has a directory called "step1", then the
                # models within "step1" should be stored as "extraction/step1/..."
                else:
                    models.extend(
                        self.get_models(path, prefix=Path(prefix) / path.name)
                    )

            # If object is a file
            elif path.is_file():

                # If file is a python file...
                if len(re.findall(r'\.py$', path.name)) > 0:

                    # If parent directory is the models folder, then just add the
                    # python file name
                    if str(prefix) == '.':
                        models += [Path(path.name)]

                    # If parent directory is not the models folder, then add prefix to
                    # python file name
                    else:
                        models += [Path(prefix) / Path(path.name)]
        return models

    def user_arg_models(self,
        args: argparse.Namespace,
        models_dir: Path
    ) -> List[Path]:
        """
        Process user arg models

        args:
            args: user arguments
            models_dir: models directory
        returns:
            list of processed user arg models
        """
        try:
            raw_models = args.models
            if raw_models is None:
                processed_models = self.get_models(models_dir)
            else:
                processed_models = []
                for m in raw_models:

                    # Check if * is used
                    m_split = m.split('/')
                    if m_split[-1] == '*':
                        parent = '/'.join([m_comp for m_comp in m_split[:-1]])
                        processed_models.extend(
                            self.get_models(Path(models_dir / parent), Path(parent))
                        )

                    # Check if path is a directory
                    elif Path(models_dir / m).is_dir():
                        raise prism.exceptions.CompileException(
                            message=f'invalid --model argument `{m}`'
                        )

                    # Check if path is a file
                    elif Path(models_dir / m).is_file():
                        if len(re.findall(r'\.py$', Path(models_dir / m).name)) == 0:
                            raise prism.exceptions.CompileException(
                                f'invalid --model argument `{m}`'
                            )
                        else:
                            processed_models.extend([Path(m)])

                    else:
                        raise prism.exceptions.CompileException(
                            f'could not find model `{str(m)}` in `{str(models_dir.name)}/`'  # noqa: E501
                        )

        # If --model argument not specified, then get all models
        except AttributeError:
            processed_models = self.get_models(models_dir)

        return processed_models

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
        compiled_dir: Path,
        all_models: List[Path],
        user_arg_models: List[Path],
        user_arg_all_downstream: bool = True,
        project: Optional[PrismProject] = None
    ) -> compiler.CompiledDag:
        """
        Wrapper for the `compile` method in the DagCompiler class

        args:
            project_dir: project directory
            all_models: all models in project
            user_arg_models: models passed in user argument
            user_arg_all_downstream: boolean indicating whether the user wants to run
                all models downstream of inputted args
            compiler_globals: globals() dictionary
        returns:
            CompiledDag object
        """
        dag_compiler = compiler.DagCompiler(
            project_dir,
            compiled_dir,
            all_models,
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
