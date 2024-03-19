import prism.decorators


@prism.decorators.task()
def task_fn_different_decorator_structure():
    return "hi"
