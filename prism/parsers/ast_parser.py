"""
Class for parsing Python ASTs.

Table of Contents:
- Imports
- Class definition
"""


###########
# Imports #
###########

# Standard library imports
import re
import ast
import astor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Prism imports
import prism.constants
import prism.exceptions
import prism.prism_logging
from prism.infra.manifest import TaskManifest


####################
# Class definition #
####################

# Constant
prism_task_manager_alias = 'tasks'
prism_hooks_alias = 'hooks'


class AstParser:
    """
    Class for converting task into AST and parsing the resulting tree
    """

    def __init__(self,
        task_relative_path: Path,
        parent_path: Path
    ):
        self.task_relative_path = task_relative_path
        self.parent_path = parent_path

        # Create a task manifest
        self.task_manifest = TaskManifest()

        # Extract task as a string and parse
        self.task_path = Path(self.parent_path / self.task_relative_path)
        with open(self.task_path, 'r') as f:
            self.task_str = f.read()
        f.close()
        self.ast_module = ast.parse(self.task_str)

        # Add task source code to manifest
        self.task_manifest.add_task(self.task_relative_path)

        # Check existence of if-name-main
        bool_if_name_main = self.check_if_name_main(self.ast_module)
        if bool_if_name_main:
            msg = f'found `if __name__ == "__main__"` in `{str(self.task_relative_path)}`; all task-specific code should be placed in `run` method'  # noqa: E501
            raise prism.exceptions.ParserException(message=msg)

        # Get classes and bases
        self.classes, self.bases = self.get_classes_bases(self.ast_module)

        # Get the Prism task nodes
        self.prism_task_nodes = self.get_prism_task_nodes(
            self.classes, self.bases
        )
        self.prism_task_names = [n.name for n in self.prism_task_nodes]

    def __eq__(self, other):
        return (
            self.task_relative_path == other.task_relative_path
            and self.parent_path == other.parent_path  # noqa: W503
        )

    def get_classes_bases(self,
        task: ast.Module
    ) -> Tuple[List[ast.ClassDef], List[List[ast.expr]]]:
        """
        Get the classes ans associated bases

        args:
            task: task represented as an AST node
        returns:
            list of classes and associated bases
        """
        classes = [n for n in task.body if isinstance(n, ast.ClassDef)]
        class_bases = [class_.bases for class_ in classes]
        return classes, class_bases

    def get_num_prism_task_functions(self) -> int:
        """
        Get number of functions decorated with `@task`. For testing...
        """
        tasks = []
        for node in ast.walk(self.ast_module):
            if isinstance(node, ast.FunctionDef):
                decorators = [
                    self.get_decorator_name(d) for d in node.decorator_list
                ]
                if (
                    "task" in decorators
                    or "prism.decorators.task" in decorators  # noqa: W503
                ):
                    tasks.append(node)

        # There should only be one function with the `task` decorator
        return len(tasks)

    def get_prism_task_nodes(self,
        classes: List[ast.ClassDef],
        bases: List[List[ast.expr]]
    ) -> List[Any]:
        """
        Get the node associated with the prism task from `task`

        args:
            task: task represented as an AST node
        returns:
            node associated with prism task
        """
        udf_classes = []
        for class_, base_ in zip(classes, bases):
            for obj in base_:
                if isinstance(obj, ast.Name):
                    if obj.id == "PrismTask":
                        udf_classes.append(class_)
                elif isinstance(obj, ast.Attribute):
                    if obj.attr == "PrismTask":
                        udf_classes.append(class_)

        # Check if any task is decorated with `@task`
        udf_fns = self.get_task_decorated_function()

        # Return all classes and functions
        return udf_classes + udf_fns

    def get_attribute_name(self, attribute):
        """
        Recursive function that gets the name associated with an ast.Attribute object
        """
        if isinstance(attribute.value, ast.Attribute):
            return self.get_attribute_name(attribute.value) + '.' + attribute.attr
        else:
            return attribute.value.id + '.' + attribute.attr

    def get_decorator_name(self, decorator):
        """
        Get the name of a decorator used on a function
        """
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Attribute):
            return self.get_attribute_name(decorator)
        elif isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
            return decorator.func.id
        elif isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Attribute):  # noqa: E501
            return self.get_attribute_name(decorator.func)
        else:
            return None

    def get_task_decorated_function(self) -> Optional[ast.FunctionDef]:
        """
        Get the function decorated with the `task` decorator (if any)
        """
        tasks = []
        for node in ast.walk(self.ast_module):
            if isinstance(node, ast.FunctionDef):
                decorators = [
                    self.get_decorator_name(d) for d in node.decorator_list
                ]
                if (
                    "task" in decorators
                    or "prism.decorators.task" in decorators  # noqa: W503
                ):
                    tasks.append(node)
        return tasks

    def get_all_funcs(self,
        prism_task: Union[ast.FunctionDef, ast.ClassDef]
    ) -> List[ast.FunctionDef]:
        """
        Get all functions from PrismTask class

        args:
            prism_task: PrismTask class as an AST class
        returns:
            run function as an ast.FunctionDef
        """
        if isinstance(prism_task, ast.ClassDef):
            return [f for f in prism_task.body if isinstance(f, ast.FunctionDef)]

        # If the task is a decorated function, then just return that function
        elif isinstance(prism_task, ast.FunctionDef):
            return [prism_task]

        # This should never happen
        else:
            raise prism.exceptions.ParserException(
                f"unrecognized task type {prism_task.__class__.__name__}!"
            )

    def get_run_func(self,
        prism_task: Union[ast.FunctionDef, ast.ClassDef],
    ) -> Optional[ast.FunctionDef]:
        """
        Get `run` function from PrismTask class

        args:
            prism_task: PrismTask class as an AST class
        returns:
            run function as an ast.FunctionDef
        """
        # If the prism task is a function, then the user used a decorator
        if isinstance(prism_task, ast.FunctionDef):
            return prism_task

        # Otherwise, check the classes within the user-defined class
        functions = [f for f in prism_task.body if isinstance(f, ast.FunctionDef)]
        for func in functions:
            if func.name == "run":
                return func
        return None

    def get_func_args(self, func: ast.FunctionDef) -> List[str]:
        """
        Get arguments of `func` as a list of strings

        args:
            func: function for which to retrieve arguments
        returns:
            arguments of `func` as a list of strings
        """
        results = []
        args_list = func.args.args
        for a in args_list:
            results.append(a.arg)
        return results

    def get_ref_arg_values(self,
        task_name: str,
        ref_call: ast.Call
    ) -> Tuple[str, bool]:
        """
        Parse the args and kwargs for `tasks.ref()` and get the argument values

        args:
            ref_call: `tasks.ref()` call as an ast.Call object
        returns:
            tuple of (task, local)
        """
        args = ref_call.args
        kwargs = ref_call.keywords

        if len(args) + len(kwargs) > 2:
            raise prism.exceptions.ReferenceException(
                message=f"too many args in `tasks.ref()` in `{task_name}` in `{self.task_relative_path}`"  # noqa: E501
            )

        # Instantiate the arguments
        task_arg = None
        local_arg = None

        # Check args
        for i in range(0, len(args)):
            _a = args[i]
            if hasattr(_a, "s"):
                tmp_value = _a.s
                if i == 0 and not isinstance(tmp_value, str):
                    raise prism.exceptions.ReferenceException(
                        message=f"{self.task_relative_path}.{task_name}: `task` argument in `tasks.ref()` must be a string"  # noqa: E501
                    )

                if i == 1 and isinstance(tmp_value, bool):
                    raise prism.exceptions.ReferenceException(
                        message=f"{self.task_relative_path}.{task_name}: `local` argument in `tasks.ref()` must be a bool"  # noqa: E501
                    )

                if i == 0:
                    task_arg = tmp_value

                elif i == 1:
                    local_arg = tmp_value

        # Check kwargs
        for _kw in kwargs:
            tmp_value = _kw.value.value
            if _kw.arg == "task":
                if not isinstance(tmp_value, str):
                    raise prism.exceptions.ReferenceException(
                        message=f"{self.task_relative_path}.{task_name}: `task` argument in `tasks.ref()` must be a string"  # noqa: E501
                    )
                task_arg = tmp_value
            if _kw.arg == "local":
                if not isinstance(tmp_value, bool):
                    raise prism.exceptions.ReferenceException(
                        message=f"{self.task_relative_path}.{task_name}: `local` argument in `tasks.ref()` must be a bool"  # noqa: E501
                    )
                local_arg = tmp_value

        # If the user doesn't have a a task arg, throw an error
        if task_arg is None:
            raise prism.exceptions.ReferenceException(
                message=f"{self.task_relative_path}.{task_name}: `task` argument in `tasks.ref()` must be non-Null"  # noqa: E501
            )
        if local_arg is None:
            local_arg = False

        # Return tuple
        return task_arg, local_arg

    def define_non_local_ref_task(self,
        ref_task_arg: str,
        refd_parser: Any
    ) -> str:
        """
        If the user ref's a task from a different module, return their ref argument as
        a processed task string.

        args:
            ref_task_arg: user's `tasks.ref()` task argument
            refd_parser: AstParser associated with `ref_task_arg`
        returns:
            processed task string: <module_name>.<task_name>
        """
        # If the user only enters the module name, then that module should only contain
        # one task.
        ref_task_arg_split = ref_task_arg.split(".")
        if len(ref_task_arg_split) == 1:

            # If the ref'd module contains more than one module, then throw an error.
            if len(refd_parser.prism_task_nodes) > 1:
                raise prism.exceptions.ReferenceException(
                    message=f"module `{ref_task_arg}` has multiple tasks...specify the task name and try again"  # noqa: E501
                )

            # Otherwise, grab the one task
            else:
                return f'{ref_task_arg}.{refd_parser.prism_task_names[0]}'

        # Otherwise, see if the specific task exists
        else:
            _task = ref_task_arg_split[1]
            flag_has_task = any([
                n == _task for n in refd_parser.prism_task_names  # noqa: E501
            ])
            if not flag_has_task:
                raise prism.exceptions.ReferenceException(
                    message=f"could not find task `{_task}` in `{ref_task_arg_split[0]}.py"  # noqa: E501
                )

            # Otherwise, grab the task
            else:
                return ref_task_arg

    def define_local_ref_task(self,
        ref_task_arg: str,
        curr_task_name: str,
    ) -> str:
        """
        If the user ref's a task from the same module, return their ref argument as
        a processed task string.

        args:
            ref_task_arg: user's `tasks.ref()` task argument
        returns:
            processed task string: <module_name>.<task_name>
        """
        # If the user only enters the module name, then that module should only contain
        # one task.
        ref_task_arg_split = ref_task_arg.split(".")
        if len(ref_task_arg_split) > 1:
            raise prism.exceptions.ReferenceException(
                "format <module_name>.<task_name> incompatible with `local = True`, since `local` implies the task exists in the current module"  # noqa: E501
            )

        # Check for self-reference
        if curr_task_name == ref_task_arg:
            raise prism.exceptions.ReferenceException(
                message=f'"{self.task_relative_path}.{curr_task_name}: self-references found'  # noqa: E501
            )

        # See if the task exists
        flag_has_task = any([
            n == ref_task_arg for n in self.prism_task_names
        ])
        if not flag_has_task:
            raise prism.exceptions.ReferenceException(
                message=f"{self.task_relative_path}.{curr_task_name}: task `{ref_task_arg}` not found"  # noqa: E501
            )

        # Otherwise, return
        return f'{str(self.task_relative_path).replace(".py", "")}.{ref_task_arg}'

    def get_prism_mod_calls(self,
        task_name: str,
        func: ast.FunctionDef,
        other_parsed_tasks: List[Any],
    ) -> List[Path]:
        """
        Get calls to `tasks.ref` from `func`

        args:
            func: run function represented as an ast.FunctionDef object
        returns:
            calls to prism.mod contained within function
        """
        tasks_ref_calls = []

        # Get all function calls
        call_objs = []
        for node in ast.walk(func):
            if isinstance(node, ast.Call):
                call_objs.append(node)

        # Iterate through function calls
        for c in call_objs:
            if not isinstance(c.func, ast.Attribute):
                continue
            else:
                try:
                    if c.func.value.id == prism_task_manager_alias and c.func.attr == 'ref':  # type: ignore # noqa: E501

                        # Get the ref arg values
                        ref_task_arg, ref_local_arg = self.get_ref_arg_values(
                            task_name, c
                        )

                        # The task arg can be in one of two formats:
                        #   1) <module name>
                        #   2) <module name>.<task name>

                        task_arg_regex = r'(?i)^[a-z0-9\_\-\/\*]+\.?(?:[a-z0-9\_\-\/\*]+|py)?$'  # noqa: E501
                        if len(re.findall(task_arg_regex, ref_task_arg)) == 0:
                            raise prism.exceptions.ParserException(
                                f'invalid task name `{ref_task_arg}`...must be in the format `<module_name>` or `<module_name>.<task_name>`'  # noqa: E501
                            )
                        if len(re.findall(r'\.py$', ref_task_arg)) > 0:
                            prism.prism_logging.fire_console_event(
                                prism.prism_logging.PyWarningEvent(str(self.task_relative_path)),  # noqa: E501
                                event_list=[],
                                log_level='warn'
                            )
                            ref_task_arg = re.sub(r'\.py$', '', ref_task_arg)

                        # Let's start with the case where local = False. This means that
                        # the user is referencing a task from a separate module.
                        if not ref_local_arg:
                            relative_path = Path(f'{ref_task_arg.split(".")[0]}.py')

                            # If the ref'd module is the same as the current one, throw
                            # an error.
                            if relative_path == self.task_relative_path:
                                raise prism.exceptions.ReferenceException(
                                    'Are you trying to access a task in the same module? If so, use only the task name as your tasks.ref() argument and set `local = True`'  # noqa: E501
                                )

                            # Now, get the ref'd task. If the argument is in the format
                            # `<module>`, then the ref'd module should only have one
                            # task. Otherwise, the user wants a specific task within
                            # the module.
                            refd_parser_list = [
                                _p for _p in other_parsed_tasks if _p.task_relative_path == relative_path  # noqa: E501
                            ]
                            if len(refd_parser_list) == 0:
                                raise prism.exceptions.ReferenceException(
                                    message=f"could not find module associated with `{ref_task_arg}`, so could not parse task"  # noqa: E501
                                )
                            refd_parser = refd_parser_list[0]
                            processed_ref_task = self.define_non_local_ref_task(
                                ref_task_arg,
                                refd_parser
                            )

                        # Next, handle the case where we are grabbing a local task
                        else:
                            processed_ref_task = self.define_local_ref_task(
                                ref_task_arg,
                                task_name
                            )
                        tasks_ref_calls.append(processed_ref_task)

                # If we encounter an Attribute error, then the call object producing the
                # error is not of interest to us. Skip.
                except AttributeError:
                    continue

        return tasks_ref_calls

    def check_if_name_main(self,
        ast_task: ast.Module
    ) -> bool:
        """
        Check if `ast_task` has an if __name__ == "__main__" block.

        args:
            ast_task: task represented as an AST
        returns:
            boolean indicating if `ast_task` has an if __name__ == "__main__" block
        """
        # TODO: optimize function by removing duplicate DFS searches via ast.walk

        # If-name-main block needs to appear in main body
        if_name_main_blocks = [c for c in ast_task.body if isinstance(c, ast.If)]
        for node in if_name_main_blocks:
            compares = []
            for nested_node in ast.walk(node):
                if isinstance(nested_node, ast.Compare):
                    compares.append(nested_node)

            # For each compare object, iterate through all sub-nodes and examine if
            # there is a Name object with id __name__ and a string object with s
            # "__main__".
            for comp in compares:
                has_name = 0
                has_main = 0
                for comp_subnode in ast.walk(comp):
                    if isinstance(comp_subnode, ast.Name) \
                            and comp_subnode.id == "__name__":
                        has_name = 1
                    if isinstance(comp_subnode, ast.Str) \
                            and comp_subnode.s == "__main__":
                        has_main = 1

                if max(has_name, has_main) == 1:
                    return True
        return False

    def get_keyword_value(self, kw: ast.keyword):
        """
        Get the argument value from a keyword argument as a string. This is used to
        parse targets.

        args:
            kw: keyword argument
        returns:
            argument value (as a string)
        """
        # Python introduced ast.unparse in version 3.9, which reverses
        # ast.parse and converts a node back into string. mypy thinks ast
        #  doesn't have an unparse method, but this is fine.
        python_greater_than_39 = prism.constants.PYTHON_VERSION.major == 3 and prism.constants.PYTHON_VERSION.minor >= 9  # noqa: E501
        if prism.constants.PYTHON_VERSION.major > 3 or python_greater_than_39:
            return ast.unparse(kw.value)  # type: ignore

        # # Otherwise, use the astor library. This is compatible with Python
        # # >=3.5
        else:
            target_value = re.sub('\n$', '', astor.to_source(kw.value))
            if target_value[0] == "(" and target_value[-1] == ")":
                target_value = target_value[1:-1]
            return target_value

    def get_targets_class_def(self,
        run_func: ast.FunctionDef,
    ) -> Union[str, List[str]]:
        """
        Get targets are strings when the task is a PrismTask class (not a decorated
        function)

        args:
            run_function: run function as an ast FunctionDef object
        returns:
            targets as strings (or a list of strings)
        """
        # Targets will always be specified in decorators
        decs = run_func.decorator_list

        target_decs = []
        for call in decs:
            if not isinstance(call, ast.Call):
                raise prism.exceptions.CompileException(
                    message="invalid target declaration"
                )
            if isinstance(call.func, ast.Attribute):
                if call.func.attr in ["target", "target_iterator"]:
                    target_decs.append(call)
            elif isinstance(call.func, ast.Name):
                if call.func.id in ["target", "target_iterator"]:
                    target_decs.append(call)

        # Iterate through target decorators and pull out the loc keyword
        locs: List[str] = []
        for targ_call in target_decs:
            kws = targ_call.keywords
            for kw in kws:
                if kw.arg == "loc":
                    locs.append(self.get_keyword_value(kw))

        if len(locs) == 1:
            return locs[0]
        else:
            return locs

    def get_task_decorator_call(self,
        function: ast.FunctionDef
    ) -> ast.Call:
        """
        Get the `@task` decorator Call object

        args:
            function: original function decorated with `@task`
        returns:
            `@task` decorator as a Call object
        """
        # Targets will always be specified as inputs in the decorator
        task_decs = []
        decorators = function.decorator_list
        for dec in decorators:
            if self.get_decorator_name(dec) in ["task", "prism.decorators.task"]:
                task_decs.append(dec)

        # If there are multiple task decorators on the function, throw an error
        if len(task_decs) > 1:
            raise prism.exceptions.RuntimeException(
                f"can only be one `@task` decorator for a function...check `{str(self.task_relative_path)}`"  # noqa: E501
            )
        task_dec = task_decs[0]
        if not isinstance(task_dec, ast.Call):
            raise prism.exceptions.RuntimeException(
                "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
            )
        return task_dec

    def get_targets_function_def(self,
        function: ast.FunctionDef
    ) -> Union[str, List[str]]:
        """
        Get targets are strings when the task is a decorated function (not a PrismTask)

        args:
            function: decorated function that acts as a PrismTask
        returns:
            targets as strings (or a list of strings)
        """
        task_dec = self.get_task_decorator_call(function)

        # The `task` decorator doesn't accept positional arguments. mypy doesn't think
        # the decorator has keywords...ignore.
        locs: List[str] = []
        if not isinstance(task_dec, ast.Call):
            raise prism.exceptions.RuntimeException(
                "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
            )

        keywords = task_dec.keywords  # type: ignore
        for kw in keywords:
            if kw.arg == "targets":
                targets = kw.value
                if not isinstance(targets, ast.List):
                    raise prism.exceptions.ParserException(
                        f'invalid `targets` in `@task` decorator {str(self.task_relative_path)}; must be a list!'  # noqa: E501
                    )

                # Iterate through the elements of the list
                for elt in targets.elts:
                    if not isinstance(elt, ast.Call):
                        msg = "\n".join([
                            f'invalid  element in `targets` list in `@task` decorator {str(self.task_relative_path)}',  # noqa: E501
                            "should be something like `target(type=..., loc=...)`"
                        ])
                        raise prism.exceptions.ParserException(msg)

                    # The target function also only accepts keywords
                    keywords = elt.keywords
                    for kw in keywords:
                        if kw.arg == "loc":
                            locs.append(self.get_keyword_value(kw))

        if len(locs) == 1:
            return locs[0]
        else:
            return locs

    def get_targets(self,
        prism_task_node: Union[ast.FunctionDef, ast.ClassDef],
        run_func: ast.FunctionDef
    ) -> Union[str, List[str]]:
        """
        Get targets as strings

        args:
            run_function: run function as an ast FunctionDef object
        returns:
            targets as strings (or a list of strings)
        """
        if isinstance(prism_task_node, ast.ClassDef):
            return self.get_targets_class_def(run_func)
        elif isinstance(prism_task_node, ast.FunctionDef):
            return self.get_targets_function_def(run_func)

        # This should never happen
        else:
            raise prism.exceptions.ParserException(
                f"unrecognized task type {prism_task_node.__class__.__name__}!"
            )

    def parse(
        self,
        task: str,
        other_parsed_tasks: List[Any]
    ) -> Dict[str, List[str]]:
        """
        Parse task references in `task`

        args:
            task: task to parse
            other_parsed_tasks: list of AstParser objects associated with other
                modules/tasks
        returns:
            task references as a dictionary
        """
        # Get PrismTask, run function, and mod calls
        if len(self.prism_task_nodes) == 0:
            raise prism.exceptions.ParserException(
                message=f"no PrismTask in `{str(self.task_relative_path)}`"
            )

        # Iterate through all of the Prism tasks
        all_task_refs = {}
        for _node in self.prism_task_nodes:
            if _node.name != task:
                continue

            # Check if run function exists. If the user used a decorator, then the `run`
            # function is just the decorated function.
            run_func = self.get_run_func(_node)
            if run_func is None:
                raise prism.exceptions.ParserException(
                    message=f"no `run` function in PrismTask in `{str(self.task_relative_path)}`"  # noqa: E501
                )

            # Check function arguments
            run_args = self.get_func_args(run_func)
            if isinstance(_node, ast.ClassDef):
                expected = ["self", prism_task_manager_alias, prism_hooks_alias]
            elif isinstance(_node, ast.FunctionDef):
                expected = [prism_task_manager_alias, prism_hooks_alias]
            if sorted(run_args) != sorted(expected):
                msg = f'invalid arguments in `run` function in PrismTask in `{str(self.task_relative_path)}.{_node.name}`; should only be {",".join([f"`{a}`" for a in expected])}'  # noqa: E501
                raise prism.exceptions.ParserException(message=msg)

            # Parse targets
            target_locs = self.get_targets(_node, run_func)
            self.task_manifest.add_targets(
                self.task_relative_path,
                _node.name,
                target_locs
            )

            # Iterate through all functions and get prism task.ref calls
            all_funcs = self.get_all_funcs(_node)
            all_task_refs: List[Path] = []
            for func in all_funcs:
                all_task_refs += self.get_prism_mod_calls(
                    _node.name,
                    func,
                    other_parsed_tasks
                )

            # Add refs to manifest
            self.task_manifest.add_refs(
                target_module=self.task_relative_path,
                target_task=_node.name,
                sources=all_task_refs
            )
        return all_task_refs

    def get_variable_assignments(self, node, var_name: str):
        """
        Get `var_name` assignment from the Prism task. This can be used to assess the
        number of retries and the retry delay seconds.

        args:
            node: parent node
            var_name: variable name
        returns:
            assigned value of variable
        """
        # If the inputted node is an ast.Assign object, then the function is executing
        # one of its recursive calls. In this case, walk through the assign object and
        # find the names / constants.
        if isinstance(node, ast.Assign):

            # We eventually want to find names and constants. The order of the names
            # should exactly match the order of the constants. E.g., if the user says
            # VAR1, VAR2 = 30, 60, then the first name object will be VAR1 and the first
            # constant object will be 30.
            names = []
            constants = []
            for sub_node in ast.walk(node):
                if isinstance(sub_node, ast.Name):
                    names.append(sub_node.id)
                if isinstance(sub_node, ast.Constant):
                    constants.append(sub_node.value)

            # Get the var name
            for n, c in zip(names, constants):
                if n == var_name:
                    return c

            # If nothing has been returned, return None

        # Assume that the var name is an ast.Assign object. If it isn't, then it isn't a
        # variable assignment, it's something else.
        else:
            assigns = []
            for node in ast.walk(node):
                if isinstance(node, ast.Assign):
                    assigns.append(node)

            # Iterate through assign objects
            val = None
            for ast_assign in assigns:
                new_val = self.get_variable_assignments(ast_assign, var_name)
                if new_val is not None:
                    val = new_val

            # Return val. This will return None if the var name is found, and it will
            # return the last value for var name if it is defined multiple times.
            return val
