from prism.task import PrismTask
import prism.target as PrismTarget

class Moduled(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('moduleB.py') + mods.ref('moduleA.py') + mods.ref('moduleC.py') + " This is module D."


# EOF