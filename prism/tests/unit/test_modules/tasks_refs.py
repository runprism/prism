from prism.task import PrismTask

class TasksRefs(PrismTask):
    
    def func_0(self, tasks, hooks):
        return tasks.ref('func_0.py')

    def run(self, tasks, hooks):
        x = tasks.ref('hello.py')
        y = tasks.ref('world.py')
        return 'hi'

    def func_1(self, tasks, hooks):
        return tasks.ref('func_1.py')


# EOF