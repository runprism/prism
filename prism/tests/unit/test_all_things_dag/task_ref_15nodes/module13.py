from prism.task import PrismTask
import prism.target as PrismTarget

class Module13(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('module10.py') + "This is module 13. "


# EOF