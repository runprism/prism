from prism.task import PrismTask
import prism.target as PrismTarget

class Moduled(PrismTask):

    def run(self, tasks, hooks):
        return "This is module D."


# EOF