from prism.task import PrismTask
import prism.target as PrismTarget

class Modele(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('modelA.py') + tasks.ref('modelC.py') + tasks.ref('modelD.py') + " This is model E."


# EOF