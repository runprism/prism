from prism.task import PrismTask
import prism.target as PrismTarget

class Model15(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model11.py') + "This is model 15. "


# EOF