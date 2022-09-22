from prism.task import PrismTask
import prism.target as PrismTarget

class Modulee(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleA.py') + tasks.ref('moduleC.py') + tasks.ref('moduleD.py') + " This is module E."


# EOF