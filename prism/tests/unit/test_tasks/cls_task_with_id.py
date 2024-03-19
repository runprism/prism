from prism.task import PrismTask


class TasksRefs(PrismTask):
    task_id = "cls_custom_task_id"

    def run(self):
        return "hi"
