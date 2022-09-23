from prism.task import PrismTask
import prism.target as PrismTarget

class Modulee(PrismTask):

    def run(self, tasks, hooks):
        return "This is module E."


# EOF