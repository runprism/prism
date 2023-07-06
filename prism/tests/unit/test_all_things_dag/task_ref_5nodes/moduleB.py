from prism.task import PrismTask
import prism.target as PrismTarget

class Taskb(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskA.py') + " This is task B."


# EOF