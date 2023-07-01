from prism.task import PrismTask
import prism.target as PrismTarget

class Modelb(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('modelA.py') + " This is model B."


# EOF