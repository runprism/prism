from prism.task import PrismTask

class OnlyPrismTask(PrismTask):
    
    def run(self, psm):
        return 'hi'


class NonPrismTask():
    
    def run(self):
        return 'hi'


# EOF