from prism.task import PrismTask
import prism.target as PrismTarget

class Modulee(PrismTask):

    def run(self, psm):
        return psm.mod('moduleA.py') + psm.mod('moduleC.py') + psm.mod('moduleD.py') + " This is module E."


# EOF