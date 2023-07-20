from prism.task import PrismTask


class Taskc(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('taskA') + " This is task C."
