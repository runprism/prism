import prism.task


class Task15(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task11.py') + "This is task 15. "
