from prism.task import PrismTask


class FirstPrismTask(PrismTask):
    def run(self):
        return "hi"


class SecondPrismTask(PrismTask):
    def run(self):
        return "hi"
