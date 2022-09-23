#############
## Imports ##
#############

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project

# Other imports
import pandas as pd


######################
## Class definition ##
######################

class Module03(prism.task.PrismTask):

    ## Run    
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'target_csv_mult_df1.csv', index=False)
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'target_csv_mult_df2.csv', index=False)
    def run(self, tasks, hooks):
        data1 = {
            'col1': ['col1_value1', 'col1_value2', 'col1_value3'],
            'col2': ['col2_value1', 'col2_value2', 'col2_value3'],
            'col3': ['col3_value1', 'col3_value2', 'col3_value3'],
            'col4': ['col4_value1', 'col4_value2', 'col4_value3'],
            'col5': ['col5_value1', 'col5_value2', 'col5_value3'],
            'col6': ['col6_value1', 'col6_value2', 'col6_value3']
        }

        data2 = {
            'colA': ['colA_value1', 'colA_value2', 'colA_value3'],
            'colB': ['colB_value1', 'colB_value2', 'colB_value3'],
            'colC': ['colC_value1', 'colC_value2', 'colC_value3'],
            'colD': ['colD_value1', 'colD_value2', 'colD_value3'],
            'colE': ['colE_value1', 'colE_value2', 'colE_value3'],
            'colF': ['colF_value1', 'colF_value2', 'colF_value3']
        }
        df1 = pd.DataFrame(data1)
        df2 = pd.DataFrame(data2)
        
        return df1, df2


# EOF