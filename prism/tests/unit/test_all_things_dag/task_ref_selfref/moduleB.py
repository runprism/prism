from prism.task import PrismTask


class Taskb(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleB')
