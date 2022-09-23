from prism.task import PrismTask
import prism.target as PrismTarget

class Module03(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module02.py') + "This is module 3."


# EOF