from prism.decorators import task


@task()
def task_function(tasks, hooks):
    return "hi"
