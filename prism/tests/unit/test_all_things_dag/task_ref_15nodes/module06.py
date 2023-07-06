from prism.task import PrismTask
import prism.target as PrismTarget

class Task06(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task05.py') + "This is task 06. "


# EOF