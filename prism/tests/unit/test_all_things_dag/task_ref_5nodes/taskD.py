from prism.task import PrismTask


class Taskd(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskB') + tasks.ref('taskA') + tasks.ref('taskC') + " This is task D."  # noqa
