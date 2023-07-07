from prism.task import PrismTask


class Taskc(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskA.py') + " This is task C."
