"""
DagCompiler and CompiledDag classes

Table of Contents
- Imports
- Class definition
"""

###########
# Imports #
###########

# Standard library imports
import os
import networkx as nx
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Prism-specific imports
import prism.constants
import prism.exceptions
import prism.prism_logging
import prism.parsers.ast_parser as ast_parser
import prism.infra.compiled_task
from prism.infra.manifest import Manifest, TaskManifest
from prism.infra.project import PrismProject


####################
# Class definition #
####################

class CompiledDag:
    """
    Compiled DAG
    """
    def __init__(self,
        tasks_dir: Path,
        nxdag: nx.DiGraph,
        topological_sort: List[str],
        user_arg_tasks: List[str],
        task_manifests: Dict[str, TaskManifest],
        parsed_tasks: List[ast_parser.AstParser],
    ):
        self.tasks_dir = tasks_dir
        self.nxdag = nxdag
        self.topological_sort = topological_sort
        self.user_arg_tasks = user_arg_tasks
        self.task_manifests = task_manifests
        self.parsed_tasks = parsed_tasks

        # Store full paths in attribute
        self.topological_sort_full_path = []

        # Create task objects
        self.compiled_tasks = []
        for _task in self.topological_sort:
            # Task name
            task_name = _task.split('.')[1]

            # Relative path and full path
            relative_path = Path(f'{_task.split(".")[0]}.py')
            full_path = tasks_dir / relative_path

            # Current parser
            task_ast_parser = [
                _p for _p in self.parsed_tasks if _p.task_relative_path == relative_path  # noqa: E501
            ][0]

            # Update attribute
            self.topological_sort_full_path.append(full_path)

            # Compiled task
            self.compiled_tasks.append(
                prism.infra.compiled_task.CompiledTask(
                    task_name,
                    relative_path,
                    full_path,
                    self.task_manifests[_task],
                    task_ast_parser,
                )
            )


class DagCompiler:
    """
    Class for parsing mod refs, building DAG
    """

    def __init__(self,
        project_dir: Path,
        tasks_dir: Path,
        compiled_dir: Path,
        all_tasks: List[str],
        parsed_tasks: List[ast_parser.AstParser],
        user_arg_tasks: List[str],
        user_arg_all_downstream: bool,
        project: Optional[PrismProject] = None
    ):
        self.project_dir = project_dir
        self.compiled_dir = compiled_dir
        self.all_tasks = all_tasks
        self.user_arg_tasks = user_arg_tasks
        self.project = project
        os.chdir(project_dir)

        # Parsed tasks
        self.parsed_tasks = parsed_tasks

        # Tasks can only be executed if their predecessors are explicitly run or have
        # targets. For example, if our DAG is A --> B --> C and we call `prism run
        # --task C`, then Prism will parse the execution order, instantiate but NOT
        # execute tasks A and B, and then run task C. In other words, A and B will
        # always be instantiated; the --all-upstream argument controls whether A and B
        # are executed.

        # This is why we don't need to keep track of the --all-upstream argument; it
        # doesn't affect what our topological sort looks like. However, the
        # --all-downstream argument does, since successors do not need to be
        # instantiated at all.
        self.user_arg_all_downstream = user_arg_all_downstream

        # Path of tasks
        self.tasks_dir = tasks_dir

        # Task manifests
        self.task_manifests: Dict[str, TaskManifest] = {}

    def parse_task_refs(self,
        tasks: List[str],
        parsed_tasks: List[ast_parser.AstParser],
    ) -> Dict[str, List[str]]:
        """
        Parse node dictionary listed at the beginning of each python script. If
        node_dict does not exist in any script, throw an error.

        args:
            tasks: tasks to compile
            parent_path: parent path of tasks
        returns:
            task references as a dictionary
        """

        # This is only ever called on the output of `get_all_tasks`, which sorts the
        # tasks alphabetically. Therefore, all mod refs will be sorted.
        task_refs_dict: Dict[str, List[str]] = {}

        # Iterate through all of the tasks
        for _mod in tasks:
            # Current module / task
            current_module = Path(f"{_mod.split('.')[0]}.py")
            curr_task = _mod.split('.')[1]

            # Current parser
            curr_parser = [
                _p for _p in parsed_tasks if _p.task_relative_path == current_module
            ][0]

            # Other parsers
            other_parsers = [
                _p for _p in parsed_tasks if _p.task_relative_path != current_module
            ]

            # Get task refs
            task_refs = curr_parser.parse(curr_task, other_parsers)
            task_refs_dict[_mod] = task_refs

            # Keep track of task manifest
            self.task_manifests[_mod] = curr_parser.task_manifest

        return task_refs_dict

    def add_graph_elem(self,
        elem: Any,
        master: List[Any]
    ) -> List[Any]:
        """
        Add graph element `elem` to master list of elements

        args:
            elem: graph element to add (either a node or an edge)
            master: master list of elements (either nodes or edges)
        returns
            master list of elements with new elem
        """
        if elem not in master:
            master += [elem]  # type: ignore
        else:
            return master
        return master

    def add_graph_node(self,
        elem: str,
        master: List[str]
    ) -> List[str]:
        """
        To resolve mypy errors...
        """
        return self.add_graph_elem(elem, master)

    def add_graph_edge(self,
        elem: Tuple[str, str],
        master: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        To resolve mypy errors...
        """
        return self.add_graph_elem(elem, master)

    def create_nodes_edges(self,
        task_references: Dict[str, Any]
    ) -> Tuple[List[str], List[Tuple[str, str]]]:
        """
        Create nodes / edges from task connections

        args:
            task_references: connections defined via {{ mod(...) }} in tasks
        outputs:
            nodes: list of nodes (tasks)
            edges: list of edges (tuple of nodes, i.e. tasks)
        """
        # Create edges and nodes
        edges: List[Tuple[str, str]] = []
        nodes: List[str] = []

        # Iterate through task references. Keys represent distinct tasks in the DAG,
        # and values represent the tasks that feed into the key.
        for mod, refs in task_references.items():
            nodes = self.add_graph_node(mod, nodes)
            if refs is None:
                continue
            else:
                for v in refs:
                    nodes = self.add_graph_node(v, nodes)
                    edges = self.add_graph_edge((v, mod), edges)
        return nodes, edges

    def create_dag(self,
        nodes: List[str],
        edges: List[Tuple[str, str]]
    ) -> nx.DiGraph:
        """
        Create DAG from edges

        args:
            user_arg_tasks: tasks passed in user arguments
            nodes: list of nodes (tasks)
            edges: list of edges (tuple of nodes, i.e. tasks)
        outputs:
            topological sort of edges
        """
        # Instantiate graph
        graph = nx.DiGraph()

        # If there are no nodes in the DAG, then throw an error. This should never
        # happen.
        if len(nodes) == 0:
            raise prism.exceptions.RuntimeException(message='DAG has 0 nodes')

        # Add the edges/nodes
        graph.add_edges_from(edges)
        graph.add_nodes_from(nodes)

        # Check if graph is a DAG
        if not nx.is_directed_acyclic_graph(graph):
            all_scc = nx.algorithms.components.strongly_connected_components(graph)
            cycles = []
            for component in all_scc:
                component = set([str(elem) for elem in list(component)])
                if len(component) > 1:
                    cycles.append(component)
            raise prism.exceptions.DAGException(
                message=f"invalid DAG, cycle found in {str(cycles)}"
            )
        return graph

    def get_node_dependencies(self,
        graph: nx.DiGraph,
        start_nodes: List[str]
    ) -> List[str]:
        """
        Parse the DiGraph and get all nodes upstream of the `start_nodes`

        args:
            graph: DAG
            start_nodes: list of nodes for which to retrieve upstream dependencies
        returns:
            list of dependencies
        """
        # Add all start nodes to the list of dependencies with a depth of 0
        deps = [x for x in start_nodes]

        # Iterate through the start nodes and extract the predecessors
        for node in deps:
            for prenode in graph.predecessors(node):
                deps.append(prenode)

        # Get unique dependencies
        unique_deps = list(set(deps))
        return unique_deps

    def get_node_successors(self,
        graph: nx.DiGraph,
        start_nodes: List[str]
    ) -> List[str]:
        """
        Parse the DiGraph and get all nodes downstream of the `start_nodes`

        args:
            graph: DAG
            start_nodes: list of nodes for which to retrieve upstream dependencies
        returns:
            list of successors
        """
        # Add all start nodes to the list of successors with a depth of 0
        successors = [x for x in start_nodes]

        # Iterate through the start nodes and extract the predecessors
        for node in successors:
            for postnode in graph.successors(node):
                successors.append(postnode)

        # Get unique dependencies
        unique_successors = list(set(successors))
        return unique_successors

    def create_topsort(self,
        all_tasks: List[str],
        user_arg_tasks: List[str],
        parsed_tasks: List[ast_parser.AstParser],
    ) -> Any:
        """
        Parse mod refs, create the DAG, and create a topological sort of the DAG

        args:
            all_tasks: list of all tasks
            user_arg_tasks: tasks passed in user arguments
            parsed_tasks: list of AstParsers associated with each task module
        returns:
            topological sorted DAG as a list
        """

        # Create a DAG using all the tasks. We use `all_tasks` instead of
        # `user_arg_tasks`, because using `user_arg_tasks` will only compile/run the
        # tasks referenced in the tasks themselves. For example, if we have a dag A
        # --> B --> C and wish to to only compile/run script C, then our code will only
        # run script B. This will throw an error, because script B relies on script A,
        # and we will need to instantiate the script A task for the script B task to
        # execute fully.
        task_refs = self.parse_task_refs(
            all_tasks,
            parsed_tasks,
        )
        nodes, edges = self.create_nodes_edges(task_refs)
        dag = self.create_dag(nodes, edges)

        # If `user_arg_tasks` is equivalent to `all_tasks`, then create a
        # topological sorting of the full DAG. From the NetworkX documentation: A
        # topological sort is a nonunique permutation of the nodes of a directed graph
        # such that an edge from u to v implies that u appears before v in the
        # topological sort order. This ordering is valid only if the graph has no
        # directed cycles.
        if len(user_arg_tasks) == len(all_tasks):
            all_topological_sorts = nx.algorithms.dag.all_topological_sorts(dag)
            all_topological_sorts_list = next(all_topological_sorts)

        # Otherwise, the user has selected to run a subset of the tasks. Identify all
        # tasks upstream (and potentially downstream) of `user_arg_tasks`.
        else:

            # Keep only the dependencies, and create a topological sort.
            all_nodes = self.get_node_dependencies(dag, user_arg_tasks)

            # Add successors if the user wants them
            if self.user_arg_all_downstream:
                all_nodes.extend(self.get_node_successors(dag, user_arg_tasks))

            subgraph = dag.subgraph(list(set(all_nodes)))
            all_topological_sorts = nx.algorithms.dag.all_topological_sorts(subgraph)  # noqa: E501
            all_topological_sorts_list = next(all_topological_sorts)

        # Add each task to manifest
        for elem in all_topological_sorts_list:

            # Raise error if node not in project
            if elem not in all_tasks:
                raise prism.exceptions.CompileException(
                    message=f'task `{str(elem)}` not found in project'
                )

        return dag, all_topological_sorts_list

    def compile(self) -> CompiledDag:
        """
        Compile the DAG
        """
        nxdag, all_topological_sorts_list = self.create_topsort(
            self.all_tasks, self.user_arg_tasks, self.parsed_tasks
        )

        # Dump manifest
        manifest = Manifest(list(self.task_manifests.values()))

        # Add the prism project to the Manifest
        if self.project is not None:
            prism_project_py_str = self.project.prism_project_py_str
        else:
            prism_project = PrismProject(
                project_dir=self.project_dir,
                user_context={},
                which="compile",
                filename="prism_project.py"
            )
            prism_project_py_str = prism_project.prism_project_py_str
        manifest.add_prism_project(prism_project_py_str)
        manifest.json_dump(self.compiled_dir)

        # Return dag
        dag = CompiledDag(
            self.tasks_dir,
            nxdag,
            all_topological_sorts_list,
            self.user_arg_tasks,
            self.task_manifests,
            self.parsed_tasks,
        )
        return dag
