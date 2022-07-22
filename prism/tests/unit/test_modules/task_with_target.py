import os
from prism.task import PrismTask
from prism.target import PrismTarget

class TaskWithTarget(PrismTask):
    
    @PrismTask.target(PrismTarget.txt, loc=os.path.join(os.getcwd(), 'temp'))
    def run(self, psm):
        x = psm.mod('hello.py')
        y = psm.mod('world.py')
        return 'hi'


# EOF