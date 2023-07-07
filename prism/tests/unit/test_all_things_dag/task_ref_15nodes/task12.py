import prism.task


class Task12(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task10.py') + "This is task 12. "
