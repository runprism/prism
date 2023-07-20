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

class Task01(prism.task.PrismTask):

    # Run
    def run(self, tasks, hooks):
        """
        Execute task.
        """
        return "Hello from task 1!"
