from prism.task import PrismTask


class TasksRefs(PrismTask):

    def func_0(self, tasks, hooks):
        return tasks.ref('func_0')

    def run(self, tasks, hooks):
        _ = tasks.ref('hello.py')
        _ = tasks.ref('world.py')
        return 'hi'

    def func_1(self, tasks, hooks):
        return tasks.ref('func_1')
