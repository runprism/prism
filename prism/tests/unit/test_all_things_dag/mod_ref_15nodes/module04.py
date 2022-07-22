from prism.task import PrismTask
import prism.target as PrismTarget

class Module04(PrismTask):

    def run(self, psm):
        return psm.mod('module02.py') + psm.mod('module03.py') + "This is module 04. "


# EOF