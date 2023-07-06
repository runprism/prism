from prism.task import PrismTask
import prism.target as PrismTarget

class Task03(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task02.py') + "This is task 3."


# EOF