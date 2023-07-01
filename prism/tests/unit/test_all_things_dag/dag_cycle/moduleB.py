from prism.task import PrismTask
import prism.target as PrismTarget

class Modelb(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('modelA.py') + tasks.ref('modelE.py') + " This is model B."


# EOF