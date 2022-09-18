from prism.task import PrismTask

class OnlyPrismTask(PrismTask):
    
    def run(self, mods, hooks):
        return 'hi'


class NonPrismTask():
    
    def run(self):
        return 'hi'


# EOF