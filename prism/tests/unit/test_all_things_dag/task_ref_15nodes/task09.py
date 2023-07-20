import prism.task


class Task09(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task05') + tasks.ref('task08') + "This is task 09. "
