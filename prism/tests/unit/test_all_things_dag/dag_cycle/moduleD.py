from prism.task import PrismTask
import prism.target as PrismTarget

class Modeld(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('modelB.py') + tasks.ref('modelA.py') + tasks.ref('modelC.py') + " This is model D."


# EOF