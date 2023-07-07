import prism.task


class Task09(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task05.py') + tasks.ref('task08.py') + "This is task 09. "
