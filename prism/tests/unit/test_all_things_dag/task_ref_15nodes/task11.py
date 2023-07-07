import prism.task


class Task11(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task07.Task07a') + tasks.ref('task10.py') + "This is task 11."
