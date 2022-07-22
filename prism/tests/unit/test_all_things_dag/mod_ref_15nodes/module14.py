from prism.task import PrismTask
import prism.target as PrismTarget

class Module14(PrismTask):

    def run(self, psm):
        return psm.mod('module11.py') + "This is module 14. "


# EOF