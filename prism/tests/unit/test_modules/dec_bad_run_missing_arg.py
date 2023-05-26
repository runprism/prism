from prism.decorators import task


@task()
def task_with_refs(tasks):
    return "hi"
