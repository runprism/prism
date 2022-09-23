from prism.task import PrismTask
import prism.target as PrismTarget

class Module04(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module02.py') + tasks.ref('module03.py') + "This is module 04. "


# EOF