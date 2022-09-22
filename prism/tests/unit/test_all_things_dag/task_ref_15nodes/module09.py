from prism.task import PrismTask
import prism.target as PrismTarget

class Module09(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module05.py') + tasks.ref('module08.py') + "This is module 09. "


# EOF