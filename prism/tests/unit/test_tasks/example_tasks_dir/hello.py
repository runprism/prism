from prism.task import PrismTask


class Hello(PrismTask):
    task_id = "hello"

    def run(self):
        return "world"
