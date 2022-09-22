import os
from prism.task import PrismTask
from prism.target import PrismTarget

class TaskWithTarget(PrismTask):
    
    @PrismTask.target(PrismTarget.txt, loc=os.path.join(os.getcwd(), 'temp'))
    def run(self, tasks, hooks):
        x = tasks.ref('hello.py')
        y = tasks.ref('world.py')
        return 'hi'


# EOF