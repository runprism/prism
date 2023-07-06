from prism.task import PrismTask
import prism.target as PrismTarget

class Task04(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task02.py') + tasks.ref('task03.py') + "This is task 04. "


# EOF