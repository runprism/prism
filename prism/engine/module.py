import ast
import astor
from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

# Prism imports
import prism.constants
import prism.exceptions


# Auxiliary functions (primarily used in testing)
def _get_func_args(func: ast.FunctionDef) -> List[str]:
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


@dataclass
class _Ref:
    """
    Source refers to the referenced task and target refers to the task that calls
    `CurrentRun.ref(...)`. This is how the ref will be represented in the topological
    sort.
    """

    target: str
    source: str


@dataclass
class _ModuleRefsAndTargets:
    refs: List[_Ref]
    targets: List[str]


class _PrismModule:
    project_dir: Path
    root_task_dir: Union[Path, str]
    module_task_relpath: str
    module_project_relpath: str
    module_path: Path
    ast_module: ast.Module
    classes: List[ast.ClassDef]
    bases: List[List[ast.expr]]
    prism_task_nodes: Dict[str, Union[ast.ClassDef, ast.FunctionDef]]
    prism_task_ids: List[str]

    def __init__(
        self,
        project_dir: Path,
        root_task_dir: Union[str, Path],
        module_task_relpath: str,
    ):
        self.project_dir = project_dir
        self.root_task_dir = root_task_dir
        self.module_task_relpath = module_task_relpath
        self.module_path = Path(root_task_dir) / module_task_relpath
        with open(self.module_path, "r") as f:
            module_txt = f.read()
        f.close()
        self.ast_module = ast.parse(module_txt)

        # Module path relative to project directory
        self.module_project_relpath = str(
            os.path.relpath(str(self.module_path), str(self.project_dir))
        )

        # Get classes and bases
        self.classes, self.bases = self.get_classes_bases(self.ast_module)

        # Get the Prism task nodes
        self.prism_task_nodes = self.get_prism_task_nodes(self.classes, self.bases)
        self.prism_task_ids = [k for k, _ in self.prism_task_nodes.items()]

    def __eq__(self, other):
        return (
            self.module_task_relpath == other.module_task_relpath
            and self.root_task_dir == other.root_task_dir  # noqa: W503
        )

    def get_classes_bases(
        self, module: ast.Module
    ) -> Tuple[List[ast.ClassDef], List[List[ast.expr]]]:
        """
        Get list of classes and associated base classes from the ast.Module instance
        associated with this module.

        args:
            module: ast.Module instance associated with this module
        returns:
            list of classes and associated base classes
        """
        classes = [n for n in module.body if isinstance(n, ast.ClassDef)]
        class_bases = [class_.bases for class_ in classes]
        return classes, class_bases

    def _get_attribute_name(self, attribute):
        """
        Recursive function that gets the name associated with an ast.Attribute object
        """
        if isinstance(attribute.value, ast.Attribute):
            return self._get_attribute_name(attribute.value) + "." + attribute.attr
        else:
            return attribute.value.id + "." + attribute.attr

    def _get_decorator_name(self, decorator):
        """
        Get the name of a decorator used on a function
        """
        if isinstance(decorator, ast.Name):
            return decorator.id
        elif isinstance(decorator, ast.Attribute):
            return self._get_attribute_name(decorator)
        elif isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name):
            return decorator.func.id
        elif isinstance(decorator, ast.Call) and isinstance(
            decorator.func, ast.Attribute
        ):  # noqa: E501
            return self._get_attribute_name(decorator.func)
        else:
            return None

    def get_task_decorated_functions(self) -> List[ast.FunctionDef]:
        """
        Get the function decorated with the `@task` decorator (if any)
        """
        tasks = []
        for node in ast.walk(self.ast_module):
            if isinstance(node, ast.FunctionDef):
                decorators = [self._get_decorator_name(d) for d in node.decorator_list]
                if (
                    "task" in decorators
                    or "prism.decorators.task" in decorators  # noqa: W503
                ):
                    tasks.append(node)
        return tasks

    def get_class_attribute_value(
        self, node: Union[ast.ClassDef, ast.Assign], attribute_name: str
    ) -> Optional[Any]:
        """
        Get the value of class attribute `attribute_name` from `node`, which is an
        ast.ClassDef instance.

        args:
            node: ast.ClassDef instance
            attribute_name: name of attribute we wish to retrive
        returns:
            clas attribute value
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
                if n == attribute_name:
                    return c

            # If nothing has been returned, return None
            return None

        # Assume that the var name is an ast.Assign object. If it isn't, then it isn't a
        # variable assignment, it's something else.
        else:
            assigns = []
            for subnode in ast.walk(node):
                if isinstance(subnode, ast.Assign):
                    assigns.append(subnode)

            # Iterate through assign objects
            val = None
            for ast_assign in assigns:
                new_val = self.get_class_attribute_value(ast_assign, attribute_name)
                if new_val is not None:
                    val = new_val

            # Return val. This will return None if the var name is found, and it will
            # return the last value for var name if it is defined multiple times.
            return val

    def get_prism_task_id_from_class(self, prism_task_class: ast.ClassDef) -> str:
        """
        Get the task ID from the Prism task, where the task is a class that inherits the
        prism.task.PrismTask class.

        args:
            prism_task_class: Prism task as an ast.ClassDef object
        returns:
            task_id as a string
        """
        task_id_attr: Optional[str] = self.get_class_attribute_value(
            prism_task_class, "task_id"
        )
        task_id = (
            f"{self.module_task_relpath.replace('.py', '')}.{prism_task_class.name}"
            if task_id_attr is None
            else task_id_attr
        )  # noqa: E501
        return task_id

    def get_task_decorator_call(self, function: ast.FunctionDef) -> ast.Call:
        """
        Get the `@task` decorator ast.Call object

        args:
            function: original function decorated with `@task`
        returns:
            `@task` decorator as a Call object
        """
        # Targets will always be specified as inputs in the decorator
        task_decs = []
        decorators = function.decorator_list
        for dec in decorators:
            if self._get_decorator_name(dec) in ["task", "prism.decorators.task"]:
                task_decs.append(dec)

        # If there are multiple task decorators on the function, throw an error
        if len(task_decs) > 1:
            raise prism.exceptions.RuntimeException(
                f"Error in `{function.name}` can only be one `@task` decorator for a function`"  # noqa: E501
            )
        task_dec = task_decs[0]
        if not isinstance(task_dec, ast.Call):
            raise prism.exceptions.RuntimeException(
                "`task` decorator not properly specified...try adding parentheses to it, e.g., `@task()`"  # noqa: E501
            )
        return task_dec

    def _get_keyword_arg_from_task_decorator(
        self,
        prism_task_func: ast.FunctionDef,
        decorator_call: ast.Call,
        keyword: str,
        expected_type: Literal["str", "int"],
    ) -> Optional[Union[str, int]]:
        # We enforce the use of keyword arguments in the task decorator
        kwargs = decorator_call.keywords
        for _kw in kwargs:
            if hasattr(_kw.value, "value"):
                tmp_value = _kw.value.value
                if _kw.arg != keyword:
                    continue
                task_id_kw = tmp_value
                if task_id_kw:
                    if expected_type == "str":
                        if not isinstance(task_id_kw, str):
                            raise prism.exceptions.ReferenceException(
                                message=f"Error in `@task()` decorator call in `{prism_task_func.name}`: `task_id` must be a `str`!"  # noqa: E501
                            )
                        return task_id_kw
                    elif expected_type == "int":
                        if not isinstance(task_id_kw, int):
                            raise prism.exceptions.ReferenceException(
                                message=f"Error in `@task()` decorator call in `{prism_task_func.name}`: `task_id` must be a `int`!"  # noqa: E501
                            )
                        return task_id_kw
                    else:
                        raise ValueError(f"Unexpected type `{expected_type}`")
        return None

    def get_prism_task_id_from_func(self, prism_task_func: ast.FunctionDef) -> str:
        """
        Get the task ID from the Prism task, where the task is a class a function
        decorated with `prism.decorators.task`

        argsL
            prism_task_func: Prism task as an ast.FunctionDef object
        returns:
            task_id as a string
        """
        # Get the `ast.Call` associated with the decorator
        decorator_call = self.get_task_decorator_call(prism_task_func)
        task_id_kw = self._get_keyword_arg_from_task_decorator(
            prism_task_func, decorator_call, "task_id", "str"
        )
        if task_id_kw is not None:
            if not isinstance(task_id_kw, str):
                raise prism.exceptions.ReferenceException(
                    f"Unexpected type: task ID `{task_id_kw}` should be a `str`"
                )
        task_id = (
            f"{self.module_task_relpath.replace('.py', '')}.{prism_task_func.name}"
            if task_id_kw is None
            else task_id_kw
        )
        return task_id

    def get_prism_task_nodes(
        self, classes: List[ast.ClassDef], bases: List[List[ast.expr]]
    ) -> Dict[str, Union[ast.ClassDef, ast.FunctionDef]]:
        """
        Get all task nodes (which are either classes that inherit from
        prism.task.PrismTask or functions decorated with `@prism.decorators.task`

        args:
            classes: list of ast.ClassDef nodes found in the module
            bases: list of base classes associated with `classes`
        returns:
            list of all ast.ClassDef and ast.FunctionDef nodes associated with a task
        """
        udf_classes: List[ast.ClassDef] = []
        for class_, base_ in zip(classes, bases):
            for obj in base_:
                if isinstance(obj, ast.Name):
                    if obj.id == "PrismTask":
                        udf_classes.append(class_)
                elif isinstance(obj, ast.Attribute):
                    if obj.attr == "PrismTask":
                        udf_classes.append(class_)

        # Check if any task is decorated with `@task`
        udf_fns = self.get_task_decorated_functions()

        # Dictionaries
        node_dict: Dict[str, Union[ast.ClassDef, ast.FunctionDef]] = {}
        class_dict = {self.get_prism_task_id_from_class(c): c for c in udf_classes}
        func_dict = {self.get_prism_task_id_from_func(f): f for f in udf_fns}
        node_dict.update(class_dict)
        node_dict.update(func_dict)

        return node_dict

    def get_all_funcs(
        self, node: Union[ast.FunctionDef, ast.ClassDef]
    ) -> List[ast.FunctionDef]:
        """
        Get all functions from `node`, which is either an ast.ClassDef instance or an
        ast.FunctionDef instance. If it's the former, then we parse through the class's
        methods and return those as a list. If it's the latter, then we just return the
        function in a list of size n=1.

        args:
            node: either ast.ClassDef instance or ast.Function def instance
        returns:
            list of functions within `node`
        """
        if isinstance(node, ast.ClassDef):
            return [f for f in node.body if isinstance(f, ast.FunctionDef)]

        # If the task is a decorated function, then just return that function
        elif isinstance(node, ast.FunctionDef):
            return [node]

        # This should never happen
        else:
            raise prism.exceptions.ParserException(
                f"unrecognized task type {node.__class__.__name__}!"
            )

    def get_run_func(
        self,
        prism_task_node: Union[ast.FunctionDef, ast.ClassDef],
    ) -> Optional[ast.FunctionDef]:
        """
        Get `run` function from PrismTask node, which is either an ast.ClassDef instance
        or an ast.FunctionDef instance. If it's the former, then we parse through the
        class's methods and return the `run` method, if it exists. If it's the latter,
        then we just return the node itself.

        args:
            prism_task: PrismTask node as either an ast.ClassDef instance or
                ast.FunctionDef instance.
        returns:
            run function as an ast.FunctionDef
        """
        # If the prism task is a function, then the user used a decorator
        if isinstance(prism_task_node, ast.FunctionDef):
            return prism_task_node

        # Otherwise, check the classes within the user-defined class
        functions = [f for f in prism_task_node.body if isinstance(f, ast.FunctionDef)]
        for func in functions:
            if func.name == "run":
                return func
        return None

    def get_task_id_from_ref_call(self, parent_task_id: str, ref_call: ast.Call) -> str:
        """
        Parse the args / kwargs `CurrentRun.ref(...)` calls and get the argument
        value. The argument value should be a string, and it represents a task ID whose
        output the user wants to retrieve.

        args:
            parent_task_id: task ID of the parent task (i.e., the task that contains the
                `CurrentRun.ref()` call))
            ref_call: `PrismRef.get` call as an ast.Call object
        returns:
            task_id
        """
        args = ref_call.args
        kwargs = ref_call.keywords
        if len(args) + len(kwargs) > 1:
            raise prism.exceptions.ReferenceException(
                message=f"too many arguments in `CurrentRun.ref()` in task `{parent_task_id}`"  # noqa: E501
            )

        # Check args
        for _arg in args:
            if not hasattr(_arg, "value"):
                raise prism.exceptions.PrismASTException(
                    call_name="CurrentRun.ref()", attribute="value"
                )
            task_id_arg = _arg.value
            if not isinstance(task_id_arg, str):
                raise prism.exceptions.ReferenceException(
                    message=f"Error in `CurrentRun.ref()` call in `{parent_task_id}`: `task_id` must be a string!"  # noqa: E501
                )
            return task_id_arg

        # Check kwargs
        for _kw in kwargs:
            if hasattr(_kw.value, "value"):
                tmp_value = _kw.value.value
                if _kw.arg != "task_id":
                    raise prism.exceptions.ReferenceException(
                        message=f"Error in `CurrentRun.ref()` call in `{parent_task_id}`: unrecognized argument `{_kw.arg}`"  # noqa: E501
                    )
                task_id_kw = tmp_value
                if not isinstance(task_id_kw, str):
                    raise prism.exceptions.ReferenceException(
                        message=f"Error in `CurrentRun.ref()` call in `{parent_task_id}`: `task_id` must be a string!"  # noqa: E501
                    )
                return task_id_kw

        # If nothing has been returned, raise
        raise prism.exceptions.ReferenceException(
            message=f"could not parse task ID from `CurrentRun.ref()` in task `{parent_task_id}`"  # noqa: E501
        )

    def get_task_ids_from_refs(
        self,
        parent_task_id: str,
        func: ast.FunctionDef,
    ) -> List[str]:
        """
        Parse task IDs within `CurrentRun.ref(...)` calls within `func`. Note that these
        could appear within the functions arguments / keyword arguments or within the
        function definition itself.

        args:
            func: run function represented as an ast.FunctionDef object
        returns:
            list of task IDs used in `CurrentRun.ref(...)` cals
        """
        task_ids: List[str] = []

        # `CurrentRun.ref(...)` calls are represented as ast.Call objects. Iterate
        # through function calls
        all_call_objs = [n for n in ast.walk(func) if isinstance(n, ast.Call)]
        for c in all_call_objs:
            bool_is_ref_call: bool = False

            # The call object must be an ast.Attribute, and the value of this attribute
            # must be "ref". If it isn't, then we don't care about it.
            if not isinstance(c.func, ast.Attribute):
                continue
            else:
                try:
                    if c.func.attr == "ref":
                        # Make sure that the `CurrentRun` object is calling the `ref`
                        # attribute.
                        current_obj = c.func.value
                        if isinstance(current_obj, ast.Name):
                            if current_obj.id == "CurrentRun":
                                bool_is_ref_call = True
                        elif isinstance(current_obj, ast.Attribute):
                            while hasattr(current_obj, "value") and isinstance(
                                current_obj, ast.Attribute
                            ):  # noqa: W503
                                if current_obj.attr == "CurrentRun":
                                    bool_is_ref_call = True
                                    break
                                current_obj = current_obj.value

                    # If it is a `CurrentRun.ref(...)` call, then parse the task ID
                    if bool_is_ref_call:
                        ref_task_arg = self.get_task_id_from_ref_call(
                            parent_task_id=parent_task_id, ref_call=c
                        )
                        task_ids.append(ref_task_arg)

                # If we encounter an Attribute error, then the call object producing the
                # error is not of interest to us. Skip.
                except AttributeError:
                    continue

        return task_ids

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
        python_greater_than_39 = (
            prism.constants.PYTHON_VERSION.major == 3
            and prism.constants.PYTHON_VERSION.minor >= 9  # noqa: W503
        )
        if prism.constants.PYTHON_VERSION.major > 3 or python_greater_than_39:
            return ast.unparse(kw.value)  # type: ignore

        # # Otherwise, use the astor library. This is compatible with Python
        # # >=3.5
        else:
            target_value = re.sub("\n$", "", astor.to_source(kw.value))
            if target_value[0] == "(" and target_value[-1] == ")":
                target_value = target_value[1:-1]
            return target_value

    def get_targets_class_def(
        self,
        run_func: ast.FunctionDef,
    ) -> List[str]:
        """
        Get targets as strings when the task is a PrismTask class (not a decorated
        function)

        args:
            run_function: run function as an ast.FunctionDef object
        returns:
            targets as strings (or a list of strings)
        """
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
        return locs

    def get_targets_function_def(self, function: ast.FunctionDef) -> List[str]:
        """
        Get targets as strings when the task is a decorated function (not a class)

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
                        f"invalid `targets` in `@task` decorator {str(self.module_task_relpath)}; must be a list!"  # noqa: E501
                    )

                # Iterate through the elements of the list
                for elt in targets.elts:
                    if not isinstance(elt, ast.Call):
                        msg = "\n".join(
                            [
                                f"invalid  element in `targets` list in `@task` decorator {str(self.module_task_relpath)}",  # noqa: E501
                                "should be something like `target(type=..., loc=...)`",
                            ]
                        )
                        raise prism.exceptions.ParserException(msg)

                    # The target function also only accepts keywords
                    keywords = elt.keywords
                    for kw in keywords:
                        if kw.arg == "loc":
                            locs.append(self.get_keyword_value(kw))

        return locs

    def get_targets(
        self,
        prism_task_node: Union[ast.FunctionDef, ast.ClassDef],
        run_func: ast.FunctionDef,
    ) -> List[str]:
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

    def parse(self, task_id: str) -> _ModuleRefsAndTargets:
        """
        Parse all `CurrentRun.ref(...)` calls in `task_id`

        args:
            task_id: task ID in which to look for `CurrentRun.ref(...)` calls
        returns:
            task references as a dictionary
        """
        # Iterate through all of the Prism tasks
        if task_id not in self.prism_task_nodes:
            # TODO: clean up errors
            raise ValueError(
                f"Error in `{self.module_task_relpath}`: could not find task with ID `{task_id}`!"  # noqa: E501
            )
        node = self.prism_task_nodes[task_id]

        # Check if run function exists. If the user used a decorator, then the `run`
        # function is just the decorated function.
        run_func = self.get_run_func(node)
        if run_func is None:
            raise prism.exceptions.ParserException(
                f"Error in task `{task_id}` in `{self.module_task_relpath}`: no run function!"  # noqa: E501
            )

        # Task functions should not have any arguments
        num_args = 0
        if isinstance(node, ast.ClassDef):
            num_args = 1
        run_func_args = run_func.args
        if (
            len(run_func_args.args) > num_args
            or len(run_func_args.kwonlyargs) > num_args  # noqa: W503
            or run_func_args.kwarg is not None  # noqa: W503
        ):
            # TODO: clean up errors
            raise ValueError(
                f"Error in task `{task_id}` `{self.module_task_relpath}`: task function cannot have any arguments!"  # noqa: E501
            )

        # Parse targets
        target_locs = self.get_targets(node, run_func)

        # Iterate through all functions and get `CurrentRun.ref(...)` calls
        curr_task_funcs = self.get_all_funcs(node)
        curr_task_refs: List[_Ref] = []
        for func in curr_task_funcs:
            curr_func_task_refs = self.get_task_ids_from_refs(task_id, func)
            ref_objs = [_Ref(target=task_id, source=r) for r in curr_func_task_refs]
            curr_task_refs.extend(ref_objs)

        return _ModuleRefsAndTargets(refs=curr_task_refs, targets=target_locs)
