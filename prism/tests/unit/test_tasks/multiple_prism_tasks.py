from prism.task import PrismTask

class FirstPrismTask(PrismTask):
    
    def run(self, tasks, hooks):
        return 'hi'


class SecondPrismTask(PrismTask):
    
    def run(self, tasks, hooks):
        return 'hi'


# EOF