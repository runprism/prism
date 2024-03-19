import datetime
from typing import Any, Dict, List, Literal

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, backref, mapped_column, relationship
from sqlalchemy.types import JSON

# Prism-specific imports
from prism.db.factory import ThreadLocalSessionFactory


class Base(DeclarativeBase):
    type_annotation_map = {Dict[str, Any]: JSON}


# Models
class Project(Base):
    __tablename__ = "project"
    id: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    local_path: Mapped[str] = mapped_column(nullable=False)
    ctx: Mapped[Dict[str, Any]] = mapped_column(nullable=False)
    runs: Mapped[List["Run"]] = relationship(backref=backref("project"))
    tasks: Mapped[List["Task"]] = relationship(backref=backref("project"))
    refs: Mapped[List["Ref"]] = relationship(backref=backref("project"))
    targets: Mapped[List["Target"]] = relationship(backref=backref("project"))


class Run(Base):
    __tablename__ = "runs"
    run_slug: Mapped[str] = mapped_column(primary_key=True, nullable=False)
    run_date: Mapped[datetime.datetime] = mapped_column(nullable=False)
    logs_path: Mapped[str] = mapped_column(nullable=False)
    status: Mapped[
        Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED"]
    ] = mapped_column(nullable=True)
    taskruns: Mapped[List["TaskRun"]] = relationship(backref="run")
    ctx: Mapped[Dict[str, Any]] = mapped_column(nullable=False)
    project_id: Mapped[str] = mapped_column(ForeignKey("project.id"))


class Task(Base):
    __tablename__ = "tasks"
    id: Mapped[int] = mapped_column(
        primary_key=True, nullable=False, autoincrement=True
    )  # noqa: E501
    task_id: Mapped[str] = mapped_column(nullable=False)
    current: Mapped[bool] = mapped_column(nullable=False)
    taskruns: Mapped[List["TaskRun"]] = relationship(backref="task")
    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"))


class TaskRun(Base):
    __tablename__ = "taskruns"
    run_slug: Mapped[str] = mapped_column(ForeignKey("runs.run_slug"), primary_key=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("tasks.id"), primary_key=True)
    status: Mapped[
        Literal["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "SKIPPED"]
    ] = mapped_column(
        nullable=False
    )  # noqa: E501


class Ref(Base):
    __tablename__ = "refs"
    id: Mapped[int] = mapped_column(
        nullable=False, primary_key=True, autoincrement=True
    )  # noqa: E501
    target_id: Mapped[str] = mapped_column(nullable=False)
    source_id: Mapped[str] = mapped_column(nullable=False)
    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"))


class Target(Base):
    __tablename__ = "targets"
    id: Mapped[int] = mapped_column(
        nullable=False, primary_key=True, autoincrement=True
    )  # noqa: E501
    loc: Mapped[str] = mapped_column(nullable=False)
    task_id: Mapped[str] = mapped_column(ForeignKey("tasks.id"))
    project_id: Mapped[int] = mapped_column(ForeignKey("project.id"))


def setup():
    db_factory = ThreadLocalSessionFactory()
    Base.metadata.create_all(bind=db_factory.engine)
