from prism.task import PrismTask
import prism.target as PrismTarget

class Moduleb(PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('moduleB.py')


# EOF