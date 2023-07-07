from prism.task import PrismTask


class Taskd(PrismTask):

    def run(self, tasks, hooks):
        return "This is task D."
