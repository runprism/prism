###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project

# Other imports
import pyspark.sql.functions as F


######################
## Class definition ##
######################

class Model04(prism.task.PrismTask):

    ## Run
    @prism.decorators.target(type=prism.target.PySparkParquet, loc=str(prism_project.OUTPUT / 'model04'), mode='overwrite')
    def run(self, tasks, hooks):
        df_new = tasks.ref('model03.py').filter(F.col('col1')>=F.lit('col1_value4'))
        return df_new


# EOF