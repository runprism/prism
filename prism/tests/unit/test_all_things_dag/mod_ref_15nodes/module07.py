from prism.task import PrismTask
import prism.target as PrismTarget

class Module07(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('module04.py') + mods.ref('module06.py') + "This is module 07. "


# EOF