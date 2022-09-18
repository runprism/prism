from prism.task import PrismTask

class BadRunExtraArg(PrismTask):
    
    def run(self, mods, hooks, other_arg):
        return 'hi'


# EOF