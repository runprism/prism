from prism.task import PrismTask
import prism.target as PrismTarget

class Task12(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task10.py') + "This is task 12. "


# EOF