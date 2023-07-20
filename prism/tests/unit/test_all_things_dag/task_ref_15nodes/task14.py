import prism.task


class Task14(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task11.py') + "This is task 14. "
