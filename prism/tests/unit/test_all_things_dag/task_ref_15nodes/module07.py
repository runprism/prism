from prism.task import PrismTask
import prism.target as PrismTarget

class Task07(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task04.py') + tasks.ref('task06.py') + "This is task 07. "


# EOF