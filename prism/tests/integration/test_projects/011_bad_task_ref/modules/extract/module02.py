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
        lines = Ref("extract/this_is_an_error")
        return lines + "\n" + "Hello from task 2!"
