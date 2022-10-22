#############
## Imports ##
#############

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project


######################
## Class definition ##
######################

class FilterCustomers(prism.task.PrismTask):

    ## Run    
    @prism.decorators.target(type=prism.target.PandasCsv, loc=prism_project.OUTPUT / 'bad_adapter.csv', index=False)
    def run(self, tasks, hooks):
        df = hooks.dbt_ref('dbt_prsdfofile', 'customers')
        df_new = df.iloc[:10]
        return df_new


# EOF