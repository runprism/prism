from prism.task import PrismTask
import prism.target as PrismTarget

class Modulee(PrismTask):

    def run(self, mods, hooks):
        return "This is module E."


# EOF