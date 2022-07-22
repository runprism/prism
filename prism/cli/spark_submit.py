"""
SparkSubmit class definition, called via `prism spark-submit`

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
import prism.cli.run
from prism.spark import wrapper


class SparkSubmitTask(prism.cli.run.RunTask):
    """
    Class for submitting a PySpark-based project as a job
    """
       

    def run(self):
        self.args.cls = None
        args_dict = {
            'args': self.args
        }
        spark_wrapper = wrapper.SparkWrapper(args_dict=args_dict)
        pyfile, bash = spark_wrapper.bash_command()
        subprocess.run(bash)

        # Remove the file
        os.unlink(pyfile)


# EOF