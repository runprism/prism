from prism.task import PrismTask

class BadRunExtraArg(PrismTask):
    
    def run(self, psm, other_arg):
        return 'hi'


# EOF