from prism.task import PrismTask

class BadRunNoPsm(PrismTask):
    
    def run(self, other_arg):
        return 'hi'


# EOF