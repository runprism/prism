from prism.task import PrismTask
import prism.target as PrismTarget

class Modulea(PrismTask):

    def run(self, mods, hooks):
        return "This is module A."


# EOF