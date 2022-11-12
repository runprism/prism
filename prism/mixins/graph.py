"""
Mixin classes for SysHandler

Table of Contents
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
from pathlib import Path
import shutil

# Prism-specific imports
import prism.constants
import prism.logging
from prism.docs import DOCS_INDEX_FILE_DIR


######################
## Class definition ##
######################

class GraphMixin():
    """
    Mixin for gen_docs task
    """

    def get_docs_dir(self,
        project_dir: Path
    ) -> Path:
        """
        Create a docs/ directory in the project directory

        args:
            project_dir: Prism project directory
        returns:
            documentation directory
        """
        return project_dir / 'docs'


    def create_docs_dir(self,
        project_dir: Path
    ):
        """
        Create a docs/ directory in the project directory

        args:
            project_dir: Prism project directory
        returns:
        """
        # Create compiled directory
        docs_dir = self.get_docs_dir(project_dir)
        if not docs_dir.is_dir():
            docs_dir.mkdir(parents=True, exist_ok=True)
        return docs_dir
    

    def populate_docs_build(self, 
        project_dir: Path, 
        compiled_dir: Path
    ) -> Path:
        """
        Populate build directory in docs/build

        args:
            project_dir: Prism project directory
            compiled_dir: directory with compiled manifest.json
        returns:
            path to docs/build directory
        """
        self.create_docs_dir(project_dir)
        docs_dir = self.get_docs_dir(project_dir)

        # Copy the build directory into the docs folder. For some reason, mypy doesn't think that
        # shutil has a dirs_exist_ok field, but it does.
        shutil.copytree(DOCS_INDEX_FILE_DIR, docs_dir / 'build', dirs_exist_ok=True) # type: ignore

        # Copy the manifest.json into the docs build directory
        manifest_json_path = compiled_dir / 'manifest.json'
        if Path(docs_dir / 'build' / 'manifest.json').is_file():
            os.unlink(Path(docs_dir / 'build' / 'manifest.json'))
        shutil.copyfile(manifest_json_path, Path(docs_dir / 'build' / 'manifest.json'))

        # Return build path
        build_path = docs_dir / 'build'
        return build_path


# EOF