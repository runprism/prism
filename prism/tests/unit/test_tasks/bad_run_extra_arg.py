from prism.task import PrismTask

class BadRunExtraArg(PrismTask):
    
    def run(self, tasks, hooks, other_arg):
        return 'hi'


# EOF