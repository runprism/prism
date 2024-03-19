import time
import pandas as pd

# Prism imports
import prism.task
import prism.target
import prism.decorators
from prism.runtime import CurrentRun


####################
# Class definition #
####################


class Task02(prism.task.PrismTask):

    # Run
    @prism.decorators.target(
        type=prism.target.PandasCsv,
        loc=CurrentRun.ctx("OUTPUT") / "task02.csv",
        index=False,
    )
    def run(self):
        start_time = time.time()
        time.sleep(5)
        end_time = time.time()
        time_df = pd.DataFrame({"start_time": [start_time], "end_time": [end_time]})
        return time_df
