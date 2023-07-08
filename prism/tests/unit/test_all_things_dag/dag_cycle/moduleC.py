from prism.task import PrismTask


class Taskc(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleA') + " This is task C."
