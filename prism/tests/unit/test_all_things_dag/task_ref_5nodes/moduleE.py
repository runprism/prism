from prism.task import PrismTask
import prism.target as PrismTarget

class Taske(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskA.py') + tasks.ref('taskC.py') + tasks.ref('taskD.py') + " This is task E."


# EOF