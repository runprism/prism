from prism.task import PrismTask
import prism.target as PrismTarget

class Taska(PrismTask):

    def run(self, tasks, hooks):
        return "This is task A."


# EOF