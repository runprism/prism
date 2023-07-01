###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project


######################
## Class definition ##
######################

class Model02(prism.task.PrismTask):

    ## Run    
    @prism.decorators.target(type=prism.target.Txt, loc=prism_project.OUTPUT / 'target_txt.txt')
    def run(self, tasks, hooks):
        string = "Hello, world!"
        return string


# EOF