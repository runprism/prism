from prism.task import PrismTask

class PsmMod(PrismTask):
    
    def func_0(self, psm):
        return psm.mod('func_0.py')

    def run(self, psm):
        x = psm.mod('hello.py')
        y = psm.mod('world.py')
        return 'hi'

    def func_1(self, psm):
        return psm.mod('func_1.py')


# EOF