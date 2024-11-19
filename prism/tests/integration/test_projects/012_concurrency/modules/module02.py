import time

import pandas as pd

import prism.decorators
import prism.target

# Prism imports
import prism.task
from prism.runtime import Context

####################
# Class definition #
####################


class Task02(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=Context("OUTPUT") / "task02.csv",
        index=False,
    )
    def run(self):
        start_time = time.time()
        time.sleep(5)
        end_time = time.time()
        time_df = pd.DataFrame({"start_time": [start_time], "end_time": [end_time]})
        return time_df
