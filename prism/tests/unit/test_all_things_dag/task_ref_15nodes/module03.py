from prism.task import PrismTask
import prism.target as PrismTarget

class Module03(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module01.py') + "This is module 03. "


# EOF