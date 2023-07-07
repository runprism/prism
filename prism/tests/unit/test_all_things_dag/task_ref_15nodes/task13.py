import prism.task


class Task13(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task10.py') + "This is task 13. "
