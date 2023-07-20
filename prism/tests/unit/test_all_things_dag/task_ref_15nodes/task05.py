import prism.task


class Task05(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task01') + "This is task 05. "
