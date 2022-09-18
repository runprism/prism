from prism.task import PrismTask
import prism.target as PrismTarget

class Module09(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('module05.py') + mods.ref('module08.py') + "This is module 09. "


# EOF