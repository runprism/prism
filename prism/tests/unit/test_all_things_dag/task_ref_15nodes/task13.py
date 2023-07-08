import prism.task


class Task13(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task10') + "This is task 13. "
