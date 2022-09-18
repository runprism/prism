from prism.task import PrismTask
import prism.target as PrismTarget

class Module02(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('module01.py') + "This is module 02."


# EOF