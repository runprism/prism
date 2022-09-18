from prism.task import PrismTask
import prism.target as PrismTarget

class Modulec(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('moduleA.py') + " This is module C."


# EOF