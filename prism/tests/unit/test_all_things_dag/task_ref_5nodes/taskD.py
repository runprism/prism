from prism.task import PrismTask


class Taskd(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskB.py') + tasks.ref('taskA.py') + tasks.ref('taskC.py') + " This is task D."  # noqa
