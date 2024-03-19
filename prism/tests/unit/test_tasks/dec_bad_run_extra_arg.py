from prism.decorators import task


@task()
def task_with_refs(extra_arg):
    return "hi"
