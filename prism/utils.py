"""
Util functions
"""

# Imports
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Union,
)
import importlib
from functools import wraps


# Util functions
def requires_dependencies(
    dependencies: Union[str, List[str]],
    extras: Optional[str] = None,
):
    """
    Wrapper used to prompt the user to `pip install` a package and/or Prism extracts in
    order to run a function. Borrowed heavily from the `unstructured` library:
        https://github.com/Unstructured-IO/unstructured/blob/main/unstructured/utils.py

    args:
        dependencies: required dependencies
        extracts: list of Prism extras that the user can `pip install`
    """
    if isinstance(dependencies, str):
        dependencies = [dependencies]

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args, **kwargs):
            missing_deps: List[str] = []
            for dep in dependencies:
                if not dependency_exists(dep):
                    missing_deps.append(dep)
            if len(missing_deps) > 0:
                raise ImportError(
                    f"""Following dependencies are missing: {', '.join(["`" + dep + "`" for dep in missing_deps])}. """  # noqa
                    + (  # noqa
                        f"""Please install them using `pip install "prism-ds[{extras}]"`."""  # noqa
                        if extras
                        else f"Please install them using `pip install {' '.join(missing_deps)}`."  # noqa
                    ),
                )
            return func(*args, **kwargs)

        return wrapper
    return decorator


def dependency_exists(dependency: str):
    try:
        importlib.import_module(dependency)
    except ImportError as e:
        # Check to make sure this isn't some unrelated import error.
        if dependency in repr(e):
            return False
    return True
