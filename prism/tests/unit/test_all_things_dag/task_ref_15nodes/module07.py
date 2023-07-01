from prism.task import PrismTask
import prism.target as PrismTarget

class Model07(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('model04.py') + tasks.ref('model06.py') + "This is model 07. "


# EOF