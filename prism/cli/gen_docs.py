"""
GenerateDocs class definition, called via `prism gen-docs`

Table of Contents
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import os
import webbrowser
from pathlib import Path
from typing import List
import signal
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer
import sys

# Prism-specific imports
import prism.cli.base
import prism.cli.compile
import prism.logging
from prism.logging import Event, fire_empty_line_event, fire_console_event
import prism.mixins.docs
import prism.event_managers.base as base_event_manager


class GenerateDocsTask(prism.cli.compile.CompileTask, prism.mixins.docs.DocsMixin):
    """
    Class for generating Prism documentation
    """

    def run(self):
        # Keep track of events
        event_list: List[Event] = []

        # ---------------------------------------------------------------------------------------------------------
        # Fire header events
        
        event_list, project_dir = self.fire_header_events(event_list)
        if project_dir is None:
            return prism.cli.base.TaskRunReturnResult(event_list)
        event_list = fire_empty_line_event(self.args, event_list)
        os.chdir(project_dir)

        # Compiled dir
        compiled_dir = self.create_compiled_dir(project_dir)


        # ----------------------------------------------------------------------------------------------------------
        # Create compiled DAG
        
        result = super().run_for_subclass(self.args, project_dir, compiled_dir, event_list, True)
        if isinstance(result, prism.cli.base.TaskRunReturnResult):
            return result
        
        compiled_dag = result.outputs
        compiled_dag_error_event = result.event_to_fire
        event_list = result.event_list
        
        # If no modules in DAG, return
        if compiled_dag==0:
            event_list = fire_empty_line_event(self.args, event_list)
            event_list = fire_console_event(self.args, compiled_dag_error_event, event_list)
            event_list = fire_console_event(self.args, prism.logging.SeparatorEvent(), event_list, 0)
            return prism.cli.base.TaskRunReturnResult(event_list)

        
        # ----------------------------------------------------------------------------------------------------------
        # Create / populate the docs folder

        populate_docs_build_manager = base_event_manager.BaseEventManager(
            args=self.args,
            idx=None,
            total=None,
            name='populate docs build',
            func=self.populate_docs_build
        )
        compiled_event_manager_output = populate_docs_build_manager.manage_events_during_run(
            event_list=event_list,
            project_dir=project_dir,
            compiled_dir=compiled_dir
        )
        build_path = compiled_event_manager_output.outputs
        event_to_fire = compiled_event_manager_output.event_to_fire
        event_list = compiled_event_manager_output.event_list

        # ----------------------------------------------------------------------------------------------------------
        
        # Send clean messages when Ctrl+C is pressed
        def handler(signum, frame):
            nonlocal event_list
            event_list = fire_empty_line_event(self.args, event_list)
            res = input('Shutdown the Prism docs server (y/n)? ')
            if res=="y":
                event_list = fire_empty_line_event(self.args, event_list)
                event_list = fire_console_event(self.args, prism.logging.TaskSuccessfulEndEvent(), event_list, 0)
                event_list = fire_console_event(self.args, prism.logging.SeparatorEvent(), event_list, 0)
                sys.exit(0)
            else:
                # do nothing
                pass
        
        signal.signal(signal.SIGINT, handler)

        # Serve the docs
        os.chdir(build_path)
        port = self.args.port
        address = "0.0.0.0"

        event_list = fire_empty_line_event(self.args, event_list)
        event_list = fire_console_event(self.args, prism.logging.ServingDocsEvent(address=address, port=port))
        event_list = fire_console_event(self.args, prism.logging.ServingDocsExitInfo())
        event_list = fire_empty_line_event(self.args, event_list)
        
        # mypy doesn't think SimpleHTTPRequestHandler is ok here, but it is
        httpd = TCPServer(  # type: ignore
            (address, port), SimpleHTTPRequestHandler  # type: ignore
        )  # type: ignore

        if not self.args.no_browser:
            try:
                webbrowser.open_new_tab(f"http://127.0.0.1:{port}")
            except webbrowser.Error:
                pass
        
        try:
            httpd.serve_forever()  # blocks
        finally:
            httpd.shutdown()
            httpd.server_close()

        return prism.cli.base.TaskRunReturnResult(event_list)


# EOF