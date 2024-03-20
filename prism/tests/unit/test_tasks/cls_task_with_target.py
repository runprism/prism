import os

import prism.decorators
from prism.target import PrismTarget
from prism.task import PrismTask


class TaskWithTarget(PrismTask):
    @prism.decorators.target(PrismTarget.txt, loc=os.path.join(os.getcwd(), "temp"))
    def run(self):
        return "hi"
