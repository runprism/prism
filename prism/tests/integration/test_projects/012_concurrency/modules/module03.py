import pandas as pd

# Prism imports
import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


class Task03(prism.task.PrismTask):

    def get_txt_output(self, path):
        with open(path) as f:
            lines = f.read()
        f.close()
        return lines

    # Run
    def run(self):
        d1 = CurrentRun.ref("module01.Task01")
        assert isinstance(d1, pd.DataFrame)
        d2 = CurrentRun.ref("module02.Task02")
        assert isinstance(d2, pd.DataFrame)
        return "Hello from task 3!"
