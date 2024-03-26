from prism.decorators import task


def helper_function(): ...


@task()
def task_function():
    return "hi"
