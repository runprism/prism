import os
from prism.task import PrismTask
from prism.target import PrismTarget

class TaskWithTarget(PrismTask):
    
    @PrismTask.target(PrismTarget.txt, loc=os.path.join(os.getcwd(), 'temp'))
    def run(self, mods, hooks):
        x = mods.ref('hello.py')
        y = mods.ref('world.py')
        return 'hi'


# EOF