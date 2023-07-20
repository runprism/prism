import prism.task


class Task06(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task05') + "This is task 06. "
