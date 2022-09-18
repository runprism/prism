from prism.task import PrismTask
import prism.target as PrismTarget

class Module04(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('module02.py') + mods.ref('module03.py') + "This is module 04. "


# EOF