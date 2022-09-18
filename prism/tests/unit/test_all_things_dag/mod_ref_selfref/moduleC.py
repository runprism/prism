from prism.task import PrismTask
import prism.target as PrismTarget

class Modulec(PrismTask):

    def run(self, mods, hooks):
        return "This is module C."


# EOF