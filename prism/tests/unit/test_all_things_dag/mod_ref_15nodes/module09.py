from prism.task import PrismTask
import prism.target as PrismTarget

class Module09(PrismTask):

    def run(self, psm):
        return psm.mod('module05.py') + psm.mod('module08.py') + "This is module 09. "


# EOF