from prism.task import PrismTask


class World(PrismTask):

    def run(self, tasks, hooks):
        return "world"
