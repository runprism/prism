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

    @classmethod
    def open(cls, loc, hooks):
        raise prism.exceptions.RuntimeException(message="`open` method not implemented")


class PySparkParquet(PrismTarget):

    def save(self, **kwargs):
        self.obj.write.parquet(self.loc, **kwargs)

    @classmethod
    def open(cls, loc, hooks):
        # Imports
        from pyspark.sql import SparkSession

        # Identify SparkSession alias
        spark = None
        for attr in dir(hooks):
            _tmp = getattr(hooks, attr)
            if isinstance(_tmp, SparkSession):
                spark = _tmp
                break

        # If we couldn't find the spark session, raise error
        if spark is None:
            raise prism.exceptions.RuntimeException("could not find SparkSession in PrismHooks!")  # noqa: E501

        # Object
        obj = spark.read.parquet(loc)
        return cls(obj, loc, hooks)


class PandasCsv(PrismTarget):

    def save(self, **kwargs):
        self.obj.to_csv(self.loc, **kwargs)

    @classmethod
    def open(cls, loc, hooks):
        import pandas as pd
        obj = pd.read_csv(loc)
        return cls(obj, loc, hooks)


class NumpyTxt(PrismTarget):

    def save(self, **kwargs):
        import numpy as np
        np.savetxt(self.loc, self.obj, **kwargs)

    @classmethod
    def open(cls, loc, hooks):
        import numpy as np
        obj = np.loadtxt(loc)
        return cls(obj, loc, hooks)


class Txt(PrismTarget):

    def save(self, **kwargs):
        with open(self.loc, "w") as f:
            f.write(self.obj, **kwargs)
        f.close()

    @classmethod
    def open(cls, loc, hooks):
        with open(loc, 'r') as f:
            obj = f.read()
        return cls(obj, loc, hooks)


class MatplotlibPNG(PrismTarget):

    def save(self, **kwargs):
        self.obj.savefig(self.loc, **kwargs)

    @classmethod
    def open(cls, loc, hooks):
        from PIL import Image
        obj = Image.open(loc)
        return cls(obj, loc, hooks)


class JSON(PrismTarget):

    def save(self, **kwargs):
        import json
        json_object = json.dumps(self.obj, **kwargs)
        with open(self.loc, "w") as f:
            f.write(json_object)

    @classmethod
    def open(cls, loc, hooks):
        import json
        with open(loc, 'r') as f:
            obj = json.loads(f.read())
        return cls(obj, loc, hooks)
