from prism.task import PrismTask
import prism.target as PrismTarget

class Taskb(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskB.py')


# EOF