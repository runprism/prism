from prism.task import PrismTask
import prism.target as PrismTarget

class Taskc(PrismTask):

    def run(self, tasks, hooks):
        return "This is task C."


# EOF