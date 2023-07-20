from prism.task import PrismTask


class Taske(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskA') + tasks.ref('taskC') + tasks.ref('taskD') + " This is task E."  # noqa
