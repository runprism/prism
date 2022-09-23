from prism.task import PrismTask
import prism.target as PrismTarget

class Module15(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module11.py') + "This is module 15. "


# EOF