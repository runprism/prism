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

class Module04(prism.task.PrismTask):

    ## Run
    @prism.decorators.target(type=prism.target.PySparkParquet, loc=str(prism_project.OUTPUT / 'module04'), mode='overwrite')
    def run(self, tasks, hooks):
        df_new = tasks.ref('module03.py').filter(F.col('col1')>=F.lit('col1_value4'))
        return df_new


# EOF