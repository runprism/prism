from prism.task import PrismTask
import prism.target as PrismTarget

class Modulec(PrismTask):

    def run(self, psm):
        return psm.mod('moduleA.py') + " This is module C."


# EOF