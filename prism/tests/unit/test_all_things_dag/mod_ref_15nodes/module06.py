from prism.task import PrismTask
import prism.target as PrismTarget

class Module06(PrismTask):

    def run(self, psm):
        return psm.mod('module05.py') + "This is module 06. "


# EOF