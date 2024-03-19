from prism.decorators import task


@task(task_id="dec_custom_task_id")
def task_function():
    return "hi"
