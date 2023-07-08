from prism.task import PrismTask


class Taske(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleA') + tasks.ref('moduleC') + tasks.ref('moduleD') + " This is task E."  # noqa
