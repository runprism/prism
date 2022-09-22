from prism.task import PrismTask
import prism.target as PrismTarget

class Modulec(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleA.py') + " This is module C."


# EOF