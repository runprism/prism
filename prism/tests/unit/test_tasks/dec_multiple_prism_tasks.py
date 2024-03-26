from prism.decorators import task


@task()
def task_function_1():
    return "hi"


@task()
def task_function_2():
    return "hi"
