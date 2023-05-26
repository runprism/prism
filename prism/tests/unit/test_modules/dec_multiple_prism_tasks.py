from prism.decorators import task


@task()
def task_function_1(tasks, hooks):
    return "hi"


@task()
def task_function_2(tasks, hooks):
    return "hi"
