from prism.task import PrismTask

class BadRunNoMods(PrismTask):
    
    def run(self, hooks, other_arg):
        return 'hi'


# EOF