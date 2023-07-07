import prism.task


class Task08(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task01.py') + "This is task 08. "
