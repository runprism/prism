from prism.task import PrismTask


class Taskd(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleB') + tasks.ref('moduleA') + tasks.ref('moduleC') + " This is task D."  # noqa
