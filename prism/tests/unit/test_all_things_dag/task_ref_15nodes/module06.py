from prism.task import PrismTask
import prism.target as PrismTarget

class Model06(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model05.py') + "This is model 06. "


# EOF