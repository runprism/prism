from prism.task import PrismTask
import prism.target as PrismTarget

class Module13(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module10.py') + "This is module 13. "


# EOF