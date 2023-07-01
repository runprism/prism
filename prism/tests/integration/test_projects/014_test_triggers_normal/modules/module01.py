###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators


####################
# Class definition #
####################

class Model01(prism.task.PrismTask):

    # Run
    def run(self, tasks, hooks):
        """
        Execute task.
        """
        return "Hello from model 1!"
