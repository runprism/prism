from prism.task import PrismTask
import prism.target as PrismTarget

class Model09(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model05.py') + tasks.ref('model08.py') + "This is model 09. "


# EOF