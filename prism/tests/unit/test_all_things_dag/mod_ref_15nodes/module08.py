from prism.task import PrismTask
import prism.target as PrismTarget

class Module08(PrismTask):

    def run(self, psm):
        return psm.mod('module01.py') + "This is module 08. "


# EOF