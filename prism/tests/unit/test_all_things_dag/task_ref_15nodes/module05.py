from prism.task import PrismTask
import prism.target as PrismTarget

class Model05(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model01.py') + "This is model 05. "


# EOF