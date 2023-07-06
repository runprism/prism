from prism.task import PrismTask
import prism.target as PrismTarget

class Taskd(PrismTask):

    def run(self, tasks, hooks):
        return "This is task D."


# EOF