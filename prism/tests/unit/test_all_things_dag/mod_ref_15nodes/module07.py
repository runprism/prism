from prism.task import PrismTask
import prism.target as PrismTarget

class Module07(PrismTask):

    def run(self, psm):
        return psm.mod('module04.py') + psm.mod('module06.py') + "This is module 07. "


# EOF