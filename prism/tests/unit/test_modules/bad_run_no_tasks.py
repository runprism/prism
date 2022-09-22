from prism.task import PrismTask

class BadRunNoTasks(PrismTask):
    
    def run(self, hooks, other_arg):
        return 'hi'


# EOF