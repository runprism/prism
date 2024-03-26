from prism.decorators import task


@task
def task_with_refs():
    return "hi"
