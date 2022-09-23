from prism.task import PrismTask

class BadRunMissingArg(PrismTask):
    
    def run(self, tasks):
        return 'hi'


# EOF