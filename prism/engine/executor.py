# Standard library imports
from multiprocessing.dummy import Pool
import time
from typing import Optional, Union

# Prism-specific imports
from prism.db.mixins import DbMixin
from prism.engine.compiler import _CompiledDag
from prism.engine.compiled_task import _CompiledTask
from prism.logging.execution import _ExecutionEventManager
import prism.logging.events
from prism.task import PrismTask


class _DagExecutor(DbMixin):
    """
    Class for introducing concurrency in DAG execution
    """

    compiled_dag: _CompiledDag

    def __init__(
        self,
        compiled_dag: _CompiledDag,
        user_arg_all_upstream: bool,
        user_arg_all_downstream: bool,
        threads: int,
        full_refresh: bool = False,
    ):
        self.compiled_dag = compiled_dag

        # Refresh all `done` tasks as well
        self.full_refresh = full_refresh

        # Attributes
        self.compiled_tasks = self.compiled_dag.compiled_tasks
        self.nxdag = self.compiled_dag.nxdag
        self.topological_sort = self.compiled_dag.topological_sort
        self.topological_sort_full_path = self.compiled_dag.topological_sort_full_path
        self.user_arg_task_ids = self.compiled_dag.user_arg_task_ids
        self.user_arg_all_upstream = user_arg_all_upstream
        self.user_arg_all_downstream = user_arg_all_downstream

        # Identify nodes not explicitly run and update (only if --all-upstream is False)
        if not self.user_arg_all_upstream and not self.user_arg_all_downstream:
            self.nodes_not_explicitly_run = list(
                set(self.topological_sort) - set(self.user_arg_task_ids)
            )
        else:
            self.nodes_not_explicitly_run = []

        # Number of processes used to run concurrent tasks
        self.threads = threads

    def exec_single(self, task: _CompiledTask) -> Union[PrismTask, Exception]:
        """
        Callback used to get results of task execution in Pool
        """
        task_name = task.task_id

        # Boolean for whether to fire exec event for current script. We do not want to
        # fire the exec events if the user did not explicitly include the script in
        # their arguments
        fire_exec_events = (
            task_name in self.user_arg_task_ids
            or self.user_arg_all_upstream  # noqa: W503
            or self.user_arg_all_downstream  # noqa: W503
        )

        # If all upstream tasks are to be run, the compute the idx and total using the
        # full topological sort. Otherwise, if the script is explicitly included in the
        # user's run command then compute the idx and total using the `tasks` list.
        # Otherwise, set both to None.

        # First, sort the user arg tasks in the order in which they appear in the DAG
        tasks_idx = [self.topological_sort.index(m) for m in self.user_arg_task_ids]
        tasks_sorted = [x for _, x in sorted(zip(tasks_idx, self.user_arg_task_ids))]

        # Define the idx and total
        if self.user_arg_all_upstream or self.user_arg_all_downstream:
            idx = self.topological_sort.index(task_name) + 1
            total = len(self.topological_sort)
        elif task_name in self.user_arg_task_ids:
            idx = tasks_sorted.index(task_name) + 1
            total = len(tasks_sorted)
        else:
            idx = None
            total = None

        # Execute the task with appropriate number of retries
        retries, retry_delay_seconds = task.grab_retries_metadata()
        num_runs = 0
        num_expected_runs = retries + 1
        return_cls: Optional[PrismTask] = None
        return_err: Optional[Exception] = None
        while num_runs != num_expected_runs:
            num_runs += 1
            if num_runs > 1:
                prism.logging.events.fire_console_event(
                    prism.logging.events.DelayEvent(task_name, retry_delay_seconds),
                    log_level="warning",
                )
                time.sleep(retry_delay_seconds)
                task_name = task_name + f" (RETRY {num_runs - 1})"

            task_em = _ExecutionEventManager(
                idx=idx, total=total, name=task_name, func=task.exec
            )
            try:
                task_cls: PrismTask = task_em.run(
                    fire_exec_events,
                    is_task=True,
                    explicit_run=task_name not in self.nodes_not_explicitly_run,
                )
                return_cls = task_cls
            except Exception as err:
                assert task.run_slug is not None
                self.update_task_run_status(task.run_slug, task.task_id, "FAILED")
                return_err = err

        if return_cls:
            return return_cls
        else:
            assert return_err
            return return_err

    def _cancel_connections(self, pool):
        """
        Given a pool, cancel all adapter connections and wait until all
        runners gentle terminates.
        """
        pool.close()
        pool.terminate()
        pool.join()

    def exec(self) -> Optional[Exception]:
        """
        Execute DAG. Our general approach is as follows:
            1. Create a queue of the tasks (i.e., the tasks) that need to be executed.
            2. Create a pool with `n` processes
            3. While queue is not empty
                - Get task from the top of the queue
                - If task doesn't have any refs, add it to the pool
                - If task does have refs, then wait for those refs to finish before
                  adding it to pool
                - Once task has been added to queue, remove it from queue
        """
        # Execute all statements, stopping at first error
        self._wait_and_return = False
        self.error_event: Optional[BaseException] = None

        def callback(result: Union[PrismTask | BaseException]):
            if isinstance(result, BaseException):
                self._wait_and_return = True
                self.error_event = result
            return

        # If single-threaded, just run the tasks in order
        if self.threads == 1:
            while self.compiled_tasks != []:
                curr: _CompiledTask = self.compiled_tasks.pop(0)
                result = self.exec_single(curr)
                callback(result)
                if self.error_event is not None:
                    raise self.error_event
            return None

        # If the pool has multiple threads, then iterate through tasks and add them to
        # the Pool
        else:
            async_results = {}
            with Pool(processes=self.threads) as pool:
                while self.compiled_tasks != []:

                    # Get first task to execute
                    curr = self.compiled_tasks[0]
                    refs = curr.refs

                    # If an error occurred, skip all remaining tasks
                    if self._wait_and_return:
                        break  # type: ignore
                    else:
                        # If no refs, then add task to pool
                        if len(refs) == 0:
                            res = pool.apply_async(
                                self.exec_single, args=(curr,), callback=callback
                            )
                            async_results[curr.task_id] = res
                            self.compiled_tasks.pop(0)

                        # Since DAG is run in order, refs should have been added to pool
                        # before current task. Wait for upstream tasks
                        else:
                            for ref in refs:
                                async_results[ref].wait()

                            # If an error occurred, skip all remaining tasks
                            if self._wait_and_return:
                                break  # type: ignore
                            else:
                                res = pool.apply_async(
                                    self.exec_single, args=(curr,), callback=callback
                                )
                                async_results[curr.task_id] = res
                                self.compiled_tasks.pop(0)
                pool.close()
                pool.join()

            # If error was found, then terminate pool
            if self._wait_and_return:
                self._cancel_connections(pool)
                assert isinstance(self.error_event, BaseException)
                raise self.error_event

            # Otherwise, pool should close automatically
            else:
                return None
