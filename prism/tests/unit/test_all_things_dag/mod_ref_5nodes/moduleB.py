from prism.task import PrismTask
import prism.target as PrismTarget

class Moduleb(PrismTask):

    def run(self, psm):
        return psm.mod('moduleA.py') + " This is module B."


# EOF