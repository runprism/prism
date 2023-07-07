from prism.task import PrismTask


class Taske(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskA.py') + tasks.ref('taskC.py') + tasks.ref('taskD.py') + " This is task E."  # noqa
