from prism.task import PrismTask
import prism.target as PrismTarget

class Model04(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model02.py') + tasks.ref('model03.py') + "This is model 04. "


# EOF