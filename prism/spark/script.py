"""
Jinja2 template for PySpark spark-submit script.

Table of Contents:
- Imports
- Main
"""

#############
## Imports ##
#############

# Standard library imports
import argparse
from argparse import Namespace

# Prism-specific imports
import prism.cli.run


##########
## Main ##
##########

def main(args: argparse.Namespace):
    task = prism.cli.run.RunTask(args)
    task.run()


if __name__=="__main__":
    main({{ args }}) # type: ignore


# EOF