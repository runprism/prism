from prism.task import PrismTask  # noqa


class NoPrismTask():

    def run(self, tasks, hooks):
        return 'hi'
