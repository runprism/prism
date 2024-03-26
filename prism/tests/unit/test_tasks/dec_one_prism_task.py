from prism.decorators import task


@task()
def task_function():
    return "hi"
