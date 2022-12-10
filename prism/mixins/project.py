"""
Mixins for functions related to the prism_project.py
"""

# Imports
import os
from pathlib import Path


# Class definition
class PrismProjectMixin:

    def load_prism_project_py(self,
        project_dir: Path,
        filename: str = "prism_project.py"
    ) -> str:
        """
        Load the prism_project.py file as a string

        args:
            project_dir: project directory
            filename: name of prism_project.py file; default is "prism_project.py"
            type: output type of python file; one of either "str" or "list"
        returns:
            prism_project_py: string representation of prism_project.py
        """
        os.chdir(project_dir)
        prism_project_py_path = project_dir / filename

        # Return file as string
        with open(prism_project_py_path, 'r') as f:
            prism_project_py = f.read()
        f.close()
        return prism_project_py
