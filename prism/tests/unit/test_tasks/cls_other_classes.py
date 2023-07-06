from prism.task import PrismTask


class OnlyPrismTask(PrismTask):

    def run(self, tasks, hooks):
        return 'hi'


class NonPrismTask():

    def run(self):
        return 'hi'
