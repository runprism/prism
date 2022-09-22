from prism.task import PrismTask
import prism.target as PrismTarget

class Module07(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module04.py') + tasks.ref('module06.py') + "This is module 07. "


# EOF