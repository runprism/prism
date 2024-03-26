from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import networkx as nx

# Prism-specific imports
import prism.exceptions
from prism.engine.compiled_task import _CompiledTask
from prism.engine.module import _PrismModule

# DiGraph type
if TYPE_CHECKING:
    DiGraphType = nx.DiGraph[str]
else:
    DiGraphType = nx.DiGraph


class _CompiledDag:
    """
    Compiled DAG. This object stores the _CompiledTask instances associated with each
    task in the project.
    """

    run_slug: Optional[str]
    tasks_dir: Union[str, Path]
    task_mods: Dict[str, _PrismModule]
    task_refs: Dict[str, List[str]]
    task_targets: Dict[str, List[str]]
    nxdag: DiGraphType
    topological_sort: List[str]
    user_arg_task_ids: List[str]
    all_task_ids: List[str]

    topological_sort_full_path: List[Path]
    compiled_tasks: List[_CompiledTask]

    def __init__(
        self,
        run_slug: Optional[str],
        tasks_dir: Union[str, Path],
        task_mods: Dict[str, _PrismModule],
        task_refs: Dict[str, List[str]],
        task_targets: Dict[str, List[str]],
        nxdag: DiGraphType,
        topological_sort: List[str],
        user_arg_task_ids: List[str],
        all_task_ids: List[str],
    ):
        self.run_slug = run_slug
        self.tasks_dir = tasks_dir
        self.task_mods = task_mods
        self.task_refs = task_refs
        self.task_targets = task_targets
        self.nxdag = nxdag
        self.topological_sort = topological_sort
        self.user_arg_task_ids = user_arg_task_ids
        self.all_task_ids = all_task_ids

        # Store full paths in attribute
        self.topological_sort_full_path: List[Path] = []

        # Create task objects
        self.compiled_tasks = []
        for task_id in self.topological_sort:
            # Get the module from which the task originates
            mod = task_mods[task_id]
            mod_fullpath = mod.module_path

            # Update attribute
            self.topological_sort_full_path.append(mod_fullpath)

            # Compiled task
            self.compiled_tasks.append(
                _CompiledTask(
                    run_slug,
                    task_id,
                    mod,
                    self.task_refs[task_id],
                )
            )


class _DagCompiler:
    """
    Class that takes the `CurrentRun.ref(...)` calls and builds a topological sort for
    the tasks. For more information on topological sorts, check out here:
    https://en.wikipedia.org/wiki/Topological_sorting
    """

    project_id: str
    run_slug: Optional[str]
    tasks_dir: Union[str, Path]
    parsed_module_objs: List[_PrismModule]
    user_arg_task_ids: List[str]
    user_arg_all_downstream: bool

    task_mods: Optional[Dict[str, _PrismModule]]
    task_refs: Optional[Dict[str, List[str]]]
    task_targets: Optional[Dict[str, List[str]]]

    def __init__(
        self,
        project_id: str,
        run_slug: Optional[str],
        tasks_dir: Union[str, Path],
        parsed_module_objs: List[_PrismModule],
        user_arg_task_ids: List[str],
        user_arg_all_downstream: bool,
    ):
        self.project_id = project_id
        self.run_slug = run_slug
        self.tasks_dir = tasks_dir
        self.parsed_module_objs = parsed_module_objs
        self.user_arg_task_ids = user_arg_task_ids
        self.user_arg_all_downstream = user_arg_all_downstream

        # Tasks can only be executed if their predecessors are explicitly run or have
        # targets. For example, if our DAG is A --> B --> C and we call
        # `PrismProject.run(..., task_ids=["C"]`, then Prism will parse the execution
        # order, instantiate but NOT execute tasks A and B, and then run task C. In
        # other words, A and B will always be instantiated; the --all-upstream argument
        # controls whether A and B are executed.

        # This is why we don't need to keep track of the --all-upstream argument; it
        # doesn't affect what our topological sort looks like. However, the
        # --all-downstream argument does, since successors do not need to be
        # instantiated at all.
        self.user_arg_all_downstream = user_arg_all_downstream

        # Task refs and task targets are initially `None`. They get populated when we
        # compile the DAG
        self.task_mods = None
        self.task_refs = None
        self.task_targets = None

    def _get_all_task_ids(
        self,
        parsed_module_objs: List[_PrismModule],
    ) -> List[str]:
        """
        Get all task IDs from all modules in the project

        args:
            parsed_module_objs: list of _PrismModule objects associated with modules
                that contain tasks
        """
        all_task_ids: List[str] = []
        for _p in parsed_module_objs:
            all_task_ids.extend(_p.prism_task_ids)
        return all_task_ids

    def parse_task_refs(
        self,
        all_task_ids: List[str],
        parsed_module_objs: List[_PrismModule],
    ) -> Tuple[Dict[str, _PrismModule], Dict[str, List[str]], Dict[str, List[str]]]:
        """
        Grab all refs from the parsed modules and create a dictionary that maps task ID
        --> the refs that they rely upon.

        args:
            all_task_ids: list of all task IDs in the project
            parsed_module_objs: list of _PrismModule objects associated with modules
                that contain tasks
        returns:
            tuple of dictionaries:
                task_id --> module from which it originates
                task_id --> list of ref'd task IDs
                task_id --> targets
        """
        task_mod_dict: Dict[str, _PrismModule] = {}
        task_refs_dict: Dict[str, List[str]] = {}
        targets_dict: Dict[str, List[str]] = {}
        for tid in all_task_ids:
            # Current module
            mods_with_task = [
                pm for pm in parsed_module_objs if tid in pm.prism_task_ids
            ]
            # TODO: clean up errors
            if len(mods_with_task) > 1:
                mod_names = [m.module_task_relpath for m in mods_with_task]
                mod_names_formatted = ", ".join(["`" + n + "`" for n in mod_names])
                raise ValueError(
                    f"Duplicate task IDs `{tid}` found in project: [{mod_names_formatted}]!"  # noqa: E501
                )
            prism_mod = mods_with_task[0]

            # Get task refs
            task_refs_targets = prism_mod.parse(tid)
            task_refs = task_refs_targets.refs
            task_targets = task_refs_targets.targets

            task_mod_dict[tid] = prism_mod
            task_refs_dict[tid] = [tr.source for tr in task_refs]
            targets_dict[tid] = task_targets

        return task_mod_dict, task_refs_dict, targets_dict

    def add_graph_elem(self, elem: Any, master: List[Any]) -> List[Any]:
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

    def add_graph_node(self, elem: str, master: List[str]) -> List[str]:
        """
        To resolve mypy errors...
        """
        return self.add_graph_elem(elem, master)

    def add_graph_edge(
        self, elem: Tuple[str, str], master: List[Tuple[str, str]]
    ) -> List[Tuple[str, str]]:
        """
        To resolve mypy errors...
        """
        return self.add_graph_elem(elem, master)

    def create_nodes_edges(
        self, task_refs: Dict[str, List[str]]
    ) -> Tuple[List[str], List[Tuple[str, str]]]:
        """
        Create nodes / edges from task connections

        args:
            task_refs: connections defined via `CurrentRun.ref(...)` in tasks
        outputs:
            nodes: list of nodes (tasks)
            edges: list of edges (tuple of nodes, i.e. tasks)
        """
        # Create edges and nodes
        edges: List[Tuple[str, str]] = []
        nodes: List[str] = []

        # Iterate through task references. Keys represent distinct tasks in the DAG, and
        # values represent the tasks that feed into the key.
        for task_id, refs in task_refs.items():
            nodes = self.add_graph_node(task_id, nodes)
            if not refs:
                continue
            else:
                for ref_id in refs:
                    nodes = self.add_graph_node(ref_id, nodes)
                    edges = self.add_graph_edge((ref_id, task_id), edges)
        return nodes, edges

    def create_dag(self, nodes: List[str], edges: List[Tuple[str, str]]) -> DiGraphType:
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
        graph: DiGraphType = nx.DiGraph()

        # If there are no nodes in the DAG, then throw an error. This should never
        # happen.
        if len(nodes) == 0:
            raise prism.exceptions.RuntimeException(message="DAG has 0 nodes")

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

    def get_node_dependencies(
        self, graph: DiGraphType, start_nodes: List[str]
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

    def get_node_successors(
        self, graph: DiGraphType, start_nodes: List[str]
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

    def create_topsort(
        self,
        all_task_ids: List[str],
        user_arg_task_ids: List[str],
        parsed_module_objs: List[_PrismModule],
    ) -> Tuple[DiGraphType, List[str]]:
        """
        Parse all `CurrentRun.ref(...) calls, create the DAG, and create a topological
        sort of the DAG

        args:
            all_task_ids: list of all task IDs in the project
            user_arg_task_ids: tasks passed in at runtime
            parsed_module_objs: list of _PrismModule objects associated with modules
                that contain tasks
        returns:
            tuple of the nx.DiGraph and associated topological sort
        """

        # Create a DAG using all the tasks. We use `all_tasks` instead of
        # `user_arg_task_ids`, because using `user_arg_task_ids` will only compile/run
        # the tasks referenced in the tasks themselves. For example, if we have a dag A
        # --> B --> C and wish to to only compile/run script C, then our code will only
        # run script B. This will throw an error, because script B relies on script A,
        # and we will need to instantiate the script A task for the script B task to
        # execute fully.
        self.task_mods, self.task_refs, self.task_targets = self.parse_task_refs(
            all_task_ids, parsed_module_objs
        )
        nodes, edges = self.create_nodes_edges(self.task_refs)
        dag = self.create_dag(nodes, edges)

        # If `user_arg_task_ids` is equivalent to `all_tasks`, then create a
        # topological sorting of the full DAG. From the NetworkX documentation: A
        # topological sort is a nonunique permutation of the nodes of a directed graph
        # such that an edge from u to v implies that u appears before v in the
        # topological sort order. This ordering is valid only if the graph has no
        # directed cycles.
        if len(user_arg_task_ids) == len(all_task_ids):
            all_topological_sorts = nx.algorithms.dag.all_topological_sorts(dag)
            topological_sort = next(all_topological_sorts)

        # Otherwise, the user has selected to run a subset of the tasks. Identify all
        # tasks upstream (and potentially downstream) of `user_arg_tasks`.
        else:
            # Keep only the dependencies, and create a topological sort.
            all_nodes = self.get_node_dependencies(dag, user_arg_task_ids)

            # Add successors if the user wants them
            if self.user_arg_all_downstream:
                all_nodes.extend(self.get_node_successors(dag, user_arg_task_ids))

            subgraph = dag.subgraph(list(set(all_nodes)))
            all_topological_sorts = nx.algorithms.dag.all_topological_sorts(subgraph)  # noqa: E501
            topological_sort = next(all_topological_sorts)

        # Add each task to manifest
        for elem in topological_sort:
            # Raise error if node not in project
            if elem not in all_task_ids:
                raise prism.exceptions.CompileException(
                    message=f"task `{elem}` not found in project"
                )
        return dag, [str(x) for x in topological_sort]

    def compile(self) -> _CompiledDag:
        """
        Compile the DAG
        """
        all_task_ids = self._get_all_task_ids(self.parsed_module_objs)
        if self.user_arg_task_ids == []:
            self.user_arg_task_ids = all_task_ids

        nxdag, topological_sort = self.create_topsort(
            all_task_ids, self.user_arg_task_ids, self.parsed_module_objs
        )

        # Check that all tasks in topological sort are in project
        for m in topological_sort:
            if m not in all_task_ids:
                raise prism.exceptions.CompileException(
                    message=f"task `{m}` not found in project"
                )

        # Return DAG
        assert self.task_mods is not None
        assert self.task_refs is not None
        assert self.task_targets is not None
        dag = _CompiledDag(
            self.run_slug,
            self.tasks_dir,
            self.task_mods,
            self.task_refs,
            self.task_targets,
            nxdag,
            topological_sort,
            self.user_arg_task_ids,
            all_task_ids,
        )
        return dag
