"""
Target class definition.

Table of Contents
- Imports
- Class definitions
- Target decorators
"""

###########
# Imports #
###########

# Prism imports
import prism.exceptions
import prism.infra.hooks
import prism.infra.task_manager


#####################
# Class definitions #
#####################

class PrismTarget:

    def __init__(self, obj, loc, hooks):
        self.obj = obj
        self.loc = loc
        self.hooks = hooks

    def save(self):
        raise prism.exceptions.RuntimeException(message="`save` method not implemented")


class PySparkParquet(PrismTarget):

    def save(self, **kwargs):
        self.obj.write.parquet(self.loc, **kwargs)


class PandasCsv(PrismTarget):

    def save(self, **kwargs):
        self.obj.to_csv(self.loc, **kwargs)


class NumpyTxt(PrismTarget):

    def save(self, **kwargs):
        import numpy as np
        np.savetxt(self.loc, self.obj, **kwargs)


class Txt(PrismTarget):

    def save(self, **kwargs):
        with open(self.loc, "w") as f:
            f.write(self.obj, **kwargs)
        f.close()


class MatplotlibPNG(PrismTarget):

    def save(self, **kwargs):
        self.obj.savefig(self.loc, **kwargs)


class JSON(PrismTarget):

    def save(self, **kwargs):

        # Imports
        import json

        # Check object type
        if not isinstance(self.obj, dict):
            raise ValueError(
                "object must be a dictionary!"
            )
        json_object = json.dumps(self.obj, **kwargs)

        # Write
        with open(self.loc, "w") as f:
            f.write(json_object)
