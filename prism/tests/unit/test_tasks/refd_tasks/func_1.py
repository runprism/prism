from prism.task import PrismTask


class Func1(PrismTask):

    def run(self, tasks, hooks):
        return "world"
