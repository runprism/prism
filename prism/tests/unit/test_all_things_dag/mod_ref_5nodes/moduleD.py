from prism.task import PrismTask
import prism.target as PrismTarget

class Moduled(PrismTask):

    def run(self, psm):
        return psm.mod('moduleB.py') + psm.mod('moduleA.py') + psm.mod('moduleC.py') + " This is module D."


# EOF