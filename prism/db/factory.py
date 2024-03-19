# General package imports
import contextlib
from pathlib import Path

# SQLAlchemy imports
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import (
    sessionmaker,
    scoped_session,
    Session,
)
from sqlalchemy.sql.base import Executable

# Prism imports
from prism.constants import INTERNAL_FOLDER


class ThreadLocalSessionFactory:
    db_uri: str
    engine: Engine

    def __init__(self):
        self.db_uri = f"sqlite:///{Path(INTERNAL_FOLDER).resolve()}/prism.db"
        self.engine = create_engine(self.db_uri)

    @contextlib.contextmanager
    def create_thread_local_session(self):
        session_factory = sessionmaker()
        Session = scoped_session(session_factory)
        Session.configure(bind=self.engine)
        session = Session()
        try:
            yield session
        finally:
            session.close()

    def execute_thread_local_stmt(
        self,
        stmt: Executable,
        session: Session,
        select_statement: bool = True,
        model_objects: bool = True,
    ):
        if select_statement:
            if model_objects:
                result = session.scalars(stmt).all()
            else:
                result = session.execute(stmt).all()
            return result
        else:
            session.execute(stmt)
