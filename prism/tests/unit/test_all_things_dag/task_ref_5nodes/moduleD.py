from prism.task import PrismTask
import prism.target as PrismTarget

class Moduled(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleB.py') + tasks.ref('moduleA.py') + tasks.ref('moduleC.py') + " This is module D."


# EOF