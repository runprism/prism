from prism.task import PrismTask


class Hello(PrismTask):

    def run(self, tasks, hooks):
        return "world"
