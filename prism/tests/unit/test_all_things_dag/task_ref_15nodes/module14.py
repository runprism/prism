from prism.task import PrismTask
import prism.target as PrismTarget

class Task14(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task11.py') + "This is task 14. "


# EOF