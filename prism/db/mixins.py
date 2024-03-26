from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Union

from sqlalchemy import delete, select

from prism.db.factory import ThreadLocalSessionFactory
from prism.db.setup import Project, Ref, Run, Target, Task, TaskRun


class DbMixin:
    """
    Mixin class used to add elements to our database
    """

    def create_new_project(
        self, project_id: str, local_path: Union[str, Path], ctx: Dict[str, Any]
    ) -> None:
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            # Check if project already exists
            stmt = (
                select(Project)
                .where(Project.id == project_id)
                .where(Project.local_path == str(local_path))
            )
            project_res = factory.execute_thread_local_stmt(stmt, session)

            # All values in context should be serializable. If it's a custom object,
            # then we'll just turn it into a string.
            # TODO: maybe we should warn the user
            ctx = {k: str(v) for k, v in ctx.items()}

            # If it doesn't exist, then add the project
            if len(project_res) == 0:
                new_project = Project(
                    id=project_id, local_path=str(local_path), ctx=ctx
                )
                session.add(new_project)
            session.commit()

        return None

    def update_tasks(self, project_id: str, task_ids: List[str]) -> None:
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            # Current tasks in the database. Compare them against the `task_ids` input
            # and update the `current` field.
            seen_task_ids: List[str] = []
            stmt = select(Project).where(Project.id == project_id)
            res = factory.execute_thread_local_stmt(stmt, session)
            project = res[0]
            current_tasks_in_db: List[Task] = project.tasks
            for t in current_tasks_in_db:
                t.current = t.task_id in task_ids
                seen_task_ids.append(t.task_id)

            # Add remaining tasks
            for tid in list(set(task_ids) - set(seen_task_ids)):
                session.add(Task(task_id=tid, project_id=project_id, current=True))
            session.commit()

        return None

    def update_project_tasks_refs_targets(
        self,
        project_id: str,
        tasks: List[str],
        refs: Dict[str, List[str]],
        targets: Dict[str, List[str]],
    ) -> None:
        self.update_tasks(project_id, tasks)
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            # Delete existing refs. Then add current ones.
            ref_stmt = delete(Ref).where(Ref.project_id == project_id)
            factory.execute_thread_local_stmt(ref_stmt, session, select_statement=False)

            # Delete existing targets
            target_stmt = delete(Target).where(Target.project_id == project_id)
            factory.execute_thread_local_stmt(
                target_stmt, session, select_statement=False
            )

            # Add current refs and targets
            for target, sources in refs.items():
                for s in sources:
                    ref = Ref(
                        target_id=target,
                        source_id=s,
                        project_id=project_id,
                    )
                    session.add(ref)

            for tid, tgts in targets.items():
                for t in tgts:
                    target_obj = Target(
                        task_id=tid,
                        loc=t,
                        project_id=project_id,
                    )
                    session.add(target_obj)

            session.commit()
        return None

    def create_new_run(
        self,
        run_slug: str,
        run_date: datetime,
        logs_path: Union[str, Path],
        status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED"],
        ctx: Dict[str, Any],
        project_id: str,
    ) -> None:
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            # The run should not exist already. If it does, then raise an error
            stmt = (
                select(Run)
                .where(Run.run_slug == run_slug)
                .where(Run.project_id == project_id)
            )
            runs = factory.execute_thread_local_stmt(stmt, session)
            if len(runs) > 0:
                raise ValueError(f"run `{run_slug}` already exists in the database")

            # All values in context should be serializable. If it's a custom object,
            # then we'll just turn it into a string.
            # TODO: maybe we should warn the user
            ctx = {k: str(v) for k, v in ctx.items()}

            # Create a new Run
            run = Run(
                run_slug=run_slug,
                run_date=run_date,
                logs_path=logs_path,
                status=status,
                ctx=ctx,
                project_id=project_id,
            )
            session.add(run)
            session.commit()
        return None

    def update_run_status(
        self,
        run_slug: str,
        project_id: str,
        status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED"],
    ) -> None:
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            stmt = (
                select(Run)
                .where(Run.run_slug == run_slug)
                .where(Run.project_id == project_id)
            )
            run = factory.execute_thread_local_stmt(stmt, session)[0]
            run.status = status
            session.commit()
        return None

    def create_task_run(self, run_slug: str, task_id: str) -> None:
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            # If the task run exists, then do nothing
            stmt = (
                select(TaskRun)
                .where(TaskRun.run_slug == run_slug)
                .where(TaskRun.task_id == task_id)
            )
            res = factory.execute_thread_local_stmt(stmt, session)
            if len(res) > 0:
                return None

            # New TaskRun — we create this TaskRun when all the tasks are compiled and
            # the run is executed. Therefore, the task should start with status
            # `PENDING`. We dynamically update this status at runtime
            tr = TaskRun(run_slug=run_slug, task_id=task_id, status="PENDING")
            session.add(tr)
            session.commit()
        return None

    def update_task_run_status(
        self,
        run_slug: str,
        task_id: str,
        status: Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"],
    ) -> None:
        factory = ThreadLocalSessionFactory()
        with factory.create_thread_local_session() as session:
            stmt = (
                select(TaskRun)
                .where(TaskRun.run_slug == run_slug)
                .where(TaskRun.task_id == task_id)
            )
            res = factory.execute_thread_local_stmt(stmt, session)
            taskrun = res[0]
            taskrun.status = status
            session.commit()
        return None
