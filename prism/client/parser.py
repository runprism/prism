from pathlib import Path
import os
from typing import List, Optional, Union
import re

# Prism-specific imports
from prism.engine.module import _PrismModule
from prism.engine.compiler import _DagCompiler, _CompiledDag
import prism.logging.execution
import prism.logging.loggers


class ProjectParser(object):
    project_dir: Path
    tasks_dir: Union[str, Path]
    all_tasks_downstream: bool

    def __init__(
        self,
        project_dir: Path,
        tasks_dir: Union[str, Path],
        all_tasks_downstream: bool,
    ):
        self.project_dir = project_dir
        self.tasks_dir = tasks_dir
        self.all_tasks_downstream = all_tasks_downstream

        self._confirm_tasks_dir_exists(self.tasks_dir)

    def _confirm_tasks_dir_exists(self, tasks_dir: Union[str, Path]) -> None:
        if not Path(tasks_dir).is_dir():
            raise prism.exceptions.CompileException(
                message=f"`{tasks_dir}` is not a directory!"
            )
        return None

    def _get_all_modules_in_dir(self, rootdir: Union[str, Path]) -> List[str]:
        """
        Get all modules in the `rootdir`. Returns a list of relative paths for modules
        in `dir`

        args:
            rootdir: root directory to search
        returns:
            list of relative paths of modules in `dir`
        """
        module_relpaths: List[str] = []
        if not Path(rootdir).is_dir():
            raise ValueError(f"`{rootdir}` is not a directory!")
        for root, _, files in os.walk(rootdir):
            for fname in files:
                if len(re.findall(r"\.py$", fname)) > 0:
                    full_path = Path(root) / fname
                    rel_path = os.path.relpath(full_path, rootdir)
                    module_relpaths.append(str(rel_path))
        return module_relpaths

    def parse_all_modules(self) -> List[_PrismModule]:
        """
        Create a `_PrismModule` object for each module in the project. This object
        contains all the task nodes in the module (along with the task's refs and
        targets).

        returns:
            list of _PrismModule objects
        """
        module_relpaths = self._get_all_modules_in_dir(self.tasks_dir)
        prism_modules: List[_PrismModule] = []
        for relpath in module_relpaths:
            mod = _PrismModule(self.project_dir, self.tasks_dir, relpath)
            if len(mod.prism_task_nodes.keys()) > 0:
                prism_modules.append(mod)
        return prism_modules

    def compile_dag(
        self,
        project_id: str,
        run_slug: Optional[str],
        tasks_dir: Union[str, Path],
        parsed_module_objs: List[_PrismModule],
        user_arg_task_ids: List[str],
        user_arg_all_downstream: bool,
    ) -> _CompiledDag:
        """
        Wrapper for the `compile` method in the DagCompiler class

        args:
            project_id: project ID
            tasks_dir: directory containing all tasks
            parsed_module_objs: list of _PrismModule objects associated with modules
                that contain tasks
            user_arg_task_ids: task IDs passed in by the user at runtime
            user_arg_all_downstream: boolean indicating whether the user wants to run
                all tasks downstream of inputted `user_arg_task_ids`
            project: PrismProject
        returns:
            CompiledDag object
        """
        dag_compiler = _DagCompiler(
            project_id,
            run_slug,
            tasks_dir,
            parsed_module_objs,
            user_arg_task_ids,
            user_arg_all_downstream,
        )
        compiled_dag = dag_compiler.compile()
        return compiled_dag
