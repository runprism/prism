from prism.task import PrismTask
import prism.target as PrismTarget

class Task08(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task01.py') + "This is task 08. "


# EOF