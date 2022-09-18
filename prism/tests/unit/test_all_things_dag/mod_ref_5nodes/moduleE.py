from prism.task import PrismTask
import prism.target as PrismTarget

class Modulee(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('moduleA.py') + mods.ref('moduleC.py') + mods.ref('moduleD.py') + " This is module E."


# EOF