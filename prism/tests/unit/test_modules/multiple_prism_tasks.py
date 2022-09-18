from prism.task import PrismTask

class FirstPrismTask(PrismTask):
    
    def run(self, mods, hooks):
        return 'hi'


class SecondPrismTask(PrismTask):
    
    def run(self, mods, hooks):
        return 'hi'


# EOF