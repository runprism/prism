import prism.decorators
import prism.target
import prism.task
from prism.runtime import Context, Ref


class Task02(prism.task.PrismTask):
    # Run
    @prism.decorators.target(
        type=prism.target.Txt, loc=Context("OUTPUT") / "task01.txt"
    )
    def run(self):
        lines = Ref("module01.Task01")
        return lines[-5:]
