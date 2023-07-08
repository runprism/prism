import prism.task
import prism.decorators


# Class-based task
class Task07a(prism.task.PrismTask):

    def run(self, tasks, hooks):
        return tasks.ref('task04') + tasks.ref('task06') + "This is task 07. "


# Function-based task
@prism.decorators.task()
def task_07b(tasks, hooks):
    _ = tasks.ref("Task07a", local=True)
    return "This is a local task"
