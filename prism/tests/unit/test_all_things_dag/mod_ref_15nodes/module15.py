from prism.task import PrismTask
import prism.target as PrismTarget

class Module15(PrismTask):

    def run(self, psm):
        return psm.mod('module11.py') + "This is module 15. "


# EOF