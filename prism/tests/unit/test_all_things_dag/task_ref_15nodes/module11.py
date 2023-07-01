from prism.task import PrismTask
import prism.target as PrismTarget

class Model11(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model10.py') + "This is model 11. "


# EOF