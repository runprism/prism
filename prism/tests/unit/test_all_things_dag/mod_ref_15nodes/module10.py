from prism.task import PrismTask
import prism.target as PrismTarget

class Module10(PrismTask):

    def run(self, mods, hooks):
        return mods.ref('module01.py') + "This is module 10. "


# EOF