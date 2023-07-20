import os
from prism.task import PrismTask
from prism.target import PrismTarget
import prism.decorators


class TaskWithTarget(PrismTask):

    @prism.decorators.target(PrismTarget.txt, loc=os.path.join(os.getcwd(), 'temp'))
    def run(self, tasks, hooks):
        _ = tasks.ref('hello.py')
        _ = tasks.ref('world.py')
        return 'hi'
