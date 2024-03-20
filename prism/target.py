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

#####################
# Class definitions #
#####################


class PrismTarget:
    def __init__(self, obj, loc):
        self.obj = obj
        self.loc = loc

    def save(self):
        raise prism.exceptions.RuntimeException(message="`save` method not implemented")

    @classmethod
    def from_args(cls, obj, loc):
        return cls(obj, loc)

    @classmethod
    def open(cls, loc):
        raise prism.exceptions.RuntimeException(message="`open` method not implemented")


class PandasCsv(PrismTarget):
    def save(self, **kwargs):
        self.obj.to_csv(self.loc, **kwargs)

    @classmethod
    def open(cls, loc):
        import pandas as pd

        obj = pd.read_csv(loc)
        return cls(obj, loc)


class NumpyTxt(PrismTarget):
    def save(self, **kwargs):
        import numpy as np

        np.savetxt(self.loc, self.obj, **kwargs)

    @classmethod
    def open(cls, loc):
        import numpy as np

        obj = np.loadtxt(loc)
        return cls(obj, loc)


class Txt(PrismTarget):
    def save(self, **kwargs):
        with open(self.loc, "w") as f:
            f.write(self.obj, **kwargs)
        f.close()

    @classmethod
    def open(cls, loc):
        with open(loc, "r") as f:
            obj = f.read()
        return cls(obj, loc)


class MatplotlibPNG(PrismTarget):
    def save(self, **kwargs):
        self.obj.savefig(self.loc, **kwargs)

    @classmethod
    def open(cls, loc):
        from PIL import Image

        obj = Image.open(loc)
        return cls(obj, loc)


class JSON(PrismTarget):
    def save(self, **kwargs):
        import json

        json_object = json.dumps(self.obj, **kwargs)
        with open(self.loc, "w") as f:
            f.write(json_object)

    @classmethod
    def open(cls, loc):
        import json

        with open(loc, "r") as f:
            obj = json.loads(f.read())
        return cls(obj, loc)
