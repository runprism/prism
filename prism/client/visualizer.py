from http.server import SimpleHTTPRequestHandler
import os
from pathlib import Path
from typing import List, Union
import shutil
from socketserver import TCPServer
import sys
import signal
import webbrowser

from watchdog.observers.api import BaseObserver
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

# Prism-specific imports
from prism.client.parser import ProjectParser
from prism.docs import DOCS_INDEX_FILE_DIR
from prism.engine.compiler import _CompiledDag
from prism.engine.manifest import TaskManifest, Manifest
import prism.logging.execution
from prism.logging.events import (
    fire_header_events,
    fire_serving_docs_events,
    fire_empty_line_event,
    fire_tail_events,
    fire_reload_docs_event,
)
import prism.logging.loggers


class PrismVisualizer(object):
    project_id: str
    project_dir: Path
    tasks_dir: Union[str, Path]
    port: int
    open_window: bool
    hot_reload: bool

    project_parser: ProjectParser
    observer: BaseObserver

    def __init__(
        self,
        project_id: str,
        project_dir: Path,
        tasks_dir: Union[str, Path],
        port: int,
        open_window: bool,
        hot_reload: bool,
    ):
        self.project_id = project_id
        self.project_dir = project_dir
        self.tasks_dir = tasks_dir
        self.port = port
        self.open_window = open_window
        self.hot_reload = hot_reload

        # Project parser
        self.project_parser = ProjectParser(self.project_dir, self.tasks_dir, True)

        # Event handler for hot reloading. Note that we only start this observer if
        # `hot_reload` is True. Otherwise, we don't use it.
        reload_handler = PatternMatchingEventHandler(
            patterns=["*"],
            ignore_patterns=None,
            ignore_directories=False,
            case_sensitive=False,
        )

        def on_created(event):
            self._event_handler()

        def on_deleted(event):
            self._event_handler()

        def on_modified(event):
            self._event_handler()

        def on_moved(event):
            self._event_handler()

        # mypy doesn't like that we're assigning a function to a method. But it works,
        # so ignore.
        reload_handler.on_created = on_created  # type: ignore
        reload_handler.on_deleted = on_deleted  # type: ignore
        reload_handler.on_modified = on_modified  # type: ignore
        reload_handler.on_moved = on_moved  # type: ignore

        # Observer
        path = str(self.tasks_dir)
        self.observer = Observer()
        self.observer.schedule(reload_handler, path, recursive=True)

    def create_docs_dir(self, project_dir: Path) -> Path:
        """
        Create a docs/ directory in the project directory

        args:
            project_dir: Prism project directory
        returns:
            document directory
        """
        # Create compiled directory
        docs_dir = project_dir / "docs"
        if not docs_dir.is_dir():
            docs_dir.mkdir(parents=True, exist_ok=True)
        return docs_dir

    def create_manifest(
        self,
        compiled_dag: _CompiledDag,
        docs_dir: Path,
    ) -> None:
        task_manifests: List[TaskManifest] = []

        task_mods = compiled_dag.task_mods
        task_refs = compiled_dag.task_refs
        task_targets = compiled_dag.task_targets

        for tid, mod in task_mods.items():
            ref_srcs = task_refs[tid]
            target_locs = task_targets[tid]

            # Construct task manifest
            manifest = TaskManifest()
            manifest.add_task(Path(mod.module_task_relpath), tid)
            manifest.add_refs(
                target_module=Path(mod.module_task_relpath),
                target_task=tid,
                sources=ref_srcs,
            )
            manifest.add_targets(
                module_relative_path=Path(mod.module_task_relpath),
                task_name=tid,
                locs=target_locs,
            )

            # Add to list
            task_manifests.append(manifest)

        # Manifest
        full_manifest = Manifest(task_manifests)
        if Path(docs_dir / "build" / "manifest.json").is_file():
            os.unlink(Path(docs_dir / "build" / "manifest.json"))
        full_manifest.json_dump(Path(docs_dir / "build"))
        return None

    def _event_handler(self):
        """
        Every time the user updates their tasks, we should recompile the DAG and
        regenerate the manifest.
        """
        parsed_module_objs = self.project_parser.parse_all_modules()
        compiled_dag = self.project_parser.compile_dag(
            project_id=self.project_id,
            run_slug=None,
            tasks_dir=self.tasks_dir,
            parsed_module_objs=parsed_module_objs,
            user_arg_task_ids=[],
            user_arg_all_downstream=True,
        )

        # Create docs dir and copy the build directory into the docs folder.
        docs_dir = self.create_docs_dir(self.project_dir)
        build_dir = docs_dir / "build"
        shutil.copytree(  # type: ignore
            DOCS_INDEX_FILE_DIR, build_dir, dirs_exist_ok=True
        )
        self.create_manifest(compiled_dag, docs_dir)
        fire_reload_docs_event()

    def graph(self):
        try:
            parsed_module_objs = self.project_parser.parse_all_modules()
            num_modules = len(parsed_module_objs)
            num_tasks = 0
            for mod in parsed_module_objs:
                num_tasks += len(mod.prism_task_nodes.keys())
            fire_header_events(
                project_id=self.project_id,
                run_slug=None,
                num_tasks=num_tasks,
                num_modules=num_modules,
            )

            # Compile the DAG
            compiled_dag_em = prism.logging.execution._ExecutionEventManager(
                idx=None,
                total=None,
                name="Parsing task dependencies",
                func=self.project_parser.compile_dag,
            )
            compiled_dag = compiled_dag_em.run(
                fire_exec_events=True,
                project_id=self.project_id,
                run_slug=None,
                tasks_dir=self.tasks_dir,
                parsed_module_objs=parsed_module_objs,
                user_arg_task_ids=[],
                user_arg_all_downstream=True,
            )

            # Create docs dir and copy the build directory into the docs folder.
            docs_dir = self.create_docs_dir(self.project_dir)
            build_dir = docs_dir / "build"
            shutil.copytree(  # type: ignore
                DOCS_INDEX_FILE_DIR, build_dir, dirs_exist_ok=True
            )
            self.create_manifest(compiled_dag, docs_dir)

            # Send clean messages when Ctrl+C is pressed
            def handler(signum, frame):
                fire_empty_line_event()
                res = input("Shutdown the Prism docs server (y/n)? ")
                if res == "y":
                    fire_tail_events()
                    if self.hot_reload:
                        self.observer.stop()
                    sys.exit(0)
                else:
                    pass

            signal.signal(signal.SIGINT, handler)

            # Serve the docs
            os.chdir(build_dir)
            port = self.port
            address = "127.0.0.1"
            fire_serving_docs_events(address, port)

            # mypy doesn't think SimpleHTTPRequestHandler is ok here, but it is
            httpd = TCPServer((address, port), SimpleHTTPRequestHandler)

            if self.open_window:
                webbrowser.open_new_tab(f"http://{address}:{port}")
            try:
                if self.hot_reload:
                    self.observer.start()
                httpd.serve_forever()
            finally:
                httpd.shutdown()
                httpd.server_close()
            return None
        except Exception:
            prism.logging.loggers.CONSOLE.print_exception(
                show_locals=False, suppress=[prism], width=120
            )
            sys.exit(1)
