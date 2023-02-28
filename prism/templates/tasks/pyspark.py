
TEMPLATE = """
###########
# Imports #
###########

# Prism infrastructure imports
import prism.task
import prism.target
import prism.decorators

# Prism project imports
import prism_project


###################
# Task definition #
###################

class {{ task_cls_name }}(prism.task.PrismTask):

    # Run
    def run(self, tasks, hooks):
        \"\"\"
        Execute task.
        \"\"\"
        #TODO: implement

"""