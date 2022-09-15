"""
GenerateDocs class definition, called via `prism gen-docs`

Table of Contents
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
import subprocess

# Prism-specific imports
import prism.cli.compile


class GenerateDocsTask(prism.cli.compile.CompileTask):
    """
    Class for generate Prism documentation
    """
       

    def run(self):
        pass


# EOF