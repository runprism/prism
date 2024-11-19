import pandas as pd

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import Ref


class Task03(prism.task.PrismTask):
    def get_txt_output(self, path):
        with open(path) as f:
            lines = f.read()
        f.close()
        return lines

    # Run
    def run(self):
        d1 = Ref("module01.Task01")
        assert isinstance(d1, pd.DataFrame)
        d2 = Ref("module02.Task02")
        assert isinstance(d2, pd.DataFrame)
        return "Hello from task 3!"
