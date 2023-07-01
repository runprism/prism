from prism.task import PrismTask
import prism.target as PrismTarget

class Model14(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model11.py') + "This is model 14. "


# EOF