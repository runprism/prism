from prism.task import PrismTask


class BadRunExtraArg(PrismTask):

    def run(self, extra_arg):
        return "hi"
