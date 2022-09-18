from prism.task import PrismTask

class ModsRefs(PrismTask):
    
    def func_0(self, mods, hooks):
        return mods.ref('func_0.py')

    def run(self, mods, hooks):
        x = mods.ref('hello.py')
        y = mods.ref('world.py')
        return 'hi'

    def func_1(self, mods, hooks):
        return mods.ref('func_1.py')


# EOF