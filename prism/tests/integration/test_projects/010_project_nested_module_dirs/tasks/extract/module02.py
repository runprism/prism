# Prism imports
import prism.decorators
import prism.target
import prism.task
from prism.runtime import Context, Ref


class Task02(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=Context("OUTPUT") / "task02.txt"
    )
    def run(self):
        lines = Ref("extract/module01.Task01")
        return lines + "\n" + "Hello from task 2!"
