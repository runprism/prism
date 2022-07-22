"""
Unit testing for functions used to parse {{ mod }} references in the modules and construct the DAG.

Table of Contents:
- Imports
- Compile task instance to call functions
- Test case class definition
"""


#############
## Imports ##
#############

# Standard library imports
import re
import unittest
from pathlib import Path
from typing import Union

# Prism imports
import prism.exceptions
from prism.infra import compiler
from prism.tests.unit.test_all_things_dag import MOD_REF_TEST_CASES
from prism.tests.unit.test_all_things_dag.mod_ref_3nodes import MOD_REF_3NODES_LIST
from prism.tests.unit.test_all_things_dag.mod_ref_5nodes import MOD_REF_5NODES_LIST
from prism.tests.unit.test_all_things_dag.mod_ref_15nodes import MOD_REF_15NODES_LIST
from prism.tests.unit.test_all_things_dag.mod_ref_norefs import MOD_REF_NOREFS_LIST
from prism.tests.unit.test_all_things_dag.mod_ref_selfref import MOD_REF_SELFREF_LIST
from prism.tests.unit.test_all_things_dag.dag_cycle import DAG_CYCLE_LIST

# Make sure that list of modules are actual PosixPath objects
MOD_REF_3NODES_LIST = [Path(p) for p in MOD_REF_3NODES_LIST]
MOD_REF_5NODES_LIST = [Path(p) for p in MOD_REF_5NODES_LIST]
MOD_REF_15NODES_LIST = [Path(p) for p in MOD_REF_15NODES_LIST]
MOD_REF_NOREFS_LIST = [Path(p) for p in MOD_REF_NOREFS_LIST]
MOD_REF_SELFREF_LIST = [Path(p) for p in MOD_REF_SELFREF_LIST]
DAG_CYCLE_LIST = [Path(p) for p in DAG_CYCLE_LIST]


#############################################
## Compile task instance to call functions ##
#############################################

# Task
dag_compiler = compiler.DagCompiler(MOD_REF_TEST_CASES, None, None, None)

# Constants
MOD_REF_3NODES_DIR = Path(MOD_REF_TEST_CASES) / 'mod_ref_3nodes'
MOD_REF_5NODES_DIR = Path(MOD_REF_TEST_CASES) / 'mod_ref_5nodes'
MOD_REF_15NODES_DIR = Path(MOD_REF_TEST_CASES) / 'mod_ref_15nodes'
MOD_REF_NOREFS_DIR = Path(MOD_REF_TEST_CASES) / 'mod_ref_norefs'
MOD_REF_SELFREF_DIR = Path(MOD_REF_TEST_CASES) / 'mod_ref_selfref'
DAG_CYCLE_DIR = Path(MOD_REF_TEST_CASES) / 'dag_cycle'


################################
## Test case class definition ##
################################

class TestAllDagFunctions(unittest.TestCase):

    def _convert_mod_refs_to_str(self, mod_refs: dict):
        mod_refs_str = {}
        for k,v in mod_refs.items():
            new_k = str(k)
            if isinstance(v, Path):
                new_v = str(v)
            elif isinstance(v, list):
                new_v = [str(p) for p in v]
            elif v is None:
                new_v = None
            else:
                raise Exception
            mod_refs_str[new_k] = new_v
        return mod_refs_str


    def _convert_nodes_to_str(self, nodes: list):
        return [str(p) for p in nodes]

    def _convert_edges_to_str(self, edges: list):
        edges_str = []
        for e in edges:
            new_edge = (str(e[0]), str(e[1]))
            edges_str.append(new_edge)
        return edges_str

    def _convert_topsort_to_str(self, topsort: list):
        return [str(p) for p in topsort]
    

    ###################
    # Mod ref parsing #
    ###################

    def test_mod_ref_varying_complexity(self):
        """
        Parse mod refs with different node counts and dependency complexities. These should not result in an error.
        """
        mod_ref_3nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_3NODES_LIST, MOD_REF_3NODES_DIR)
        mod_ref_5nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_5NODES_LIST, MOD_REF_5NODES_DIR)
        mod_ref_15nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_15NODES_LIST, MOD_REF_15NODES_DIR)

        # Expected mod refs. The order here is based on the order of the `MOD_REF_XNODES_LIST` and the order of {{ mod
        # }} within the modules themselves.
        expected_3nodes_modrefs = {
            Path('module01.py'): None,
            Path('module02.py'): Path('module01.py'),
            Path('module03.py'): Path('module02.py')
        }

        expected_5nodes_modrefs = {
            Path('moduleA.py'): None,
            Path('moduleB.py'): Path('moduleA.py'),
            Path('moduleC.py'): Path('moduleA.py'),
            Path('moduleD.py'): [Path('moduleB.py'), Path('moduleA.py'), Path('moduleC.py')],    # We list psm.mod('moduleB.py') before psm.mod('moduleA.py')
            Path('moduleE.py'): [Path('moduleA.py'), Path('moduleC.py'), Path('moduleD.py')]
        }

        expected_15nodes_modrefs = {
            Path('module01.py'): None,
            Path('module02.py'): Path('module01.py'),
            Path('module03.py'): Path('module01.py'),
            Path('module04.py'): [Path('module02.py'), Path('module03.py')],
            Path('module05.py'): Path('module01.py'),
            Path('module06.py'): Path('module05.py'),
            Path('module07.py'): [Path('module04.py'), Path('module06.py')],
            Path('module08.py'): Path('module01.py'),
            Path('module09.py'): [Path('module05.py'), Path('module08.py')],
            Path('module10.py'): Path('module01.py'),
            Path('module11.py'): Path('module10.py'),
            Path('module12.py'): Path('module10.py'),
            Path('module13.py'): Path('module10.py'),
            Path('module14.py'): Path('module11.py'),
            Path('module15.py'): Path('module11.py')
        }
        
        # Sort mod ref values to test equality
        def _sort_values(mod_ref_dict: dict):
            return {k: sorted(v) if isinstance(v, list) else v for k,v in mod_ref_dict.items()}
        
        expected_3nodes_modrefs_sorted = _sort_values(expected_3nodes_modrefs)
        expected_5nodes_modrefs_sorted = _sort_values(expected_5nodes_modrefs)
        expected_15nodes_modrefs_sorted = _sort_values(expected_15nodes_modrefs)
        mod_ref_3nodes_modrefs_sorted = _sort_values(mod_ref_3nodes_modrefs)
        mod_ref_5nodes_modrefs_sorted = _sort_values(mod_ref_5nodes_modrefs)
        mod_ref_15nodes_modrefs_sorted = _sort_values(mod_ref_15nodes_modrefs)

        # Asserts
        self.assertEqual(expected_3nodes_modrefs_sorted, mod_ref_3nodes_modrefs_sorted)
        self.assertEqual(expected_5nodes_modrefs_sorted, mod_ref_5nodes_modrefs_sorted)
        self.assertEqual(expected_15nodes_modrefs_sorted, mod_ref_15nodes_modrefs_sorted)

    
    def test_no_mod_refs(self):
        """
        Modules without any mod refs will not throw an error
        """
        mod_ref_nomod_modrefs = dag_compiler.parse_mod_refs(MOD_REF_NOREFS_LIST, MOD_REF_NOREFS_DIR)
        expected_nomod_modrefs = {
            Path('moduleA.py'): None,
            Path('moduleB.py'): None,
            Path('moduleC.py'): None,
            Path('moduleD.py'): None,
            Path('moduleE.py'): None
        }

        # Asserts
        self.assertEqual(expected_nomod_modrefs, mod_ref_nomod_modrefs)

    
    def test_self_ref(self):
        """
        Self-references with mod throw a ParserException
        """
        with self.assertRaises(prism.exceptions.ParserException) as cm:
            dag_compiler.parse_mod_refs(MOD_REF_SELFREF_LIST, MOD_REF_SELFREF_DIR)
        expected_msg = "self-references found in `moduleB.py`"
        self.assertEqual(expected_msg, str(cm.exception))


    #######################
    # Create nodes, edges #
    #######################

    def test_create_nodes_edges_varying_complexity(self):
        """
        Use parsed mod refs (which we know from above should work as expected) to create nodes and edges for 3-, 5-, and
        15-node graphs. These should not throw any errors.
        """
        mod_ref_3nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_3NODES_LIST, MOD_REF_3NODES_DIR)
        mod_ref_5nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_5NODES_LIST, MOD_REF_5NODES_DIR)
        mod_ref_15nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_15NODES_LIST, MOD_REF_15NODES_DIR)

        # Create nodes and edges
        nodes_3nodes, edges_3nodes = dag_compiler.create_nodes_edges(mod_ref_3nodes_modrefs)
        nodes_5nodes, edges_5nodes = dag_compiler.create_nodes_edges(mod_ref_5nodes_modrefs)
        nodes_15nodes, edges_15nodes = dag_compiler.create_nodes_edges(mod_ref_15nodes_modrefs)
        
        # Nodes should just be the full list of modules. 
        self.assertEqual(MOD_REF_3NODES_LIST, nodes_3nodes)
        self.assertEqual(MOD_REF_5NODES_LIST, nodes_5nodes)
        self.assertEqual(MOD_REF_15NODES_LIST, nodes_15nodes)

        # Expected edges
        expected_edges_3nodes = [
            (Path('module01.py'), Path('module02.py')),
            (Path('module02.py'), Path('module03.py'))
        ]

        expected_edges_5nodes = [
            (Path('moduleA.py'), Path('moduleB.py')),
            (Path('moduleA.py'), Path('moduleC.py')),
            (Path('moduleA.py'), Path('moduleD.py')),
            (Path('moduleA.py'), Path('moduleE.py')),
            (Path('moduleB.py'), Path('moduleD.py')),
            (Path('moduleC.py'), Path('moduleD.py')),
            (Path('moduleC.py'), Path('moduleE.py')),
            (Path('moduleD.py'), Path('moduleE.py'))
        ]

        expected_edges_15nodes = [
            (Path('module01.py'), Path('module10.py')),
            (Path('module01.py'), Path('module02.py')),
            (Path('module01.py'), Path('module03.py')),
            (Path('module01.py'), Path('module05.py')),
            (Path('module01.py'), Path('module08.py')),
            (Path('module10.py'), Path('module11.py')),
            (Path('module10.py'), Path('module12.py')),
            (Path('module10.py'), Path('module13.py')),
            (Path('module02.py'), Path('module04.py')),
            (Path('module03.py'), Path('module04.py')),
            (Path('module05.py'), Path('module06.py')),
            (Path('module05.py'), Path('module09.py')),
            (Path('module08.py'), Path('module09.py')),
            (Path('module11.py'), Path('module14.py')),
            (Path('module11.py'), Path('module15.py')),
            (Path('module04.py'), Path('module07.py')),
            (Path('module06.py'), Path('module07.py'))
        ]

        # Asserts
        self.assertEqual(set(expected_edges_3nodes), set(edges_3nodes))
        self.assertEqual(set(expected_edges_5nodes), set(edges_5nodes))
        self.assertEqual(set(expected_edges_15nodes), set(edges_15nodes))

    
    def test_create_nodes_edges_no_modrefs(self):
        """
        Nodes/edges are created as expected even in situations where scripts have no mod refs
        """
        mod_ref_nomod_modrefs = dag_compiler.parse_mod_refs(MOD_REF_NOREFS_LIST, MOD_REF_NOREFS_DIR)
        nodes_nomod, edges_nomod = dag_compiler.create_nodes_edges(mod_ref_nomod_modrefs)

        # Node should just be the full list of nodes
        self.assertEqual(MOD_REF_NOREFS_LIST, nodes_nomod)

        # Edge list should be empty
        self.assertEqual([], edges_nomod)
    

    ##############
    # Create DAG #
    ##############

    def test_create_dag_varying_complexity(self):
        """
        Create topsort for DAGs with different node counts and dependency complexities. These should not result in an
        error.
        """
        # Parse mod refs
        mod_ref_3nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_3NODES_LIST, MOD_REF_3NODES_DIR)
        mod_ref_5nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_5NODES_LIST, MOD_REF_5NODES_DIR)
        mod_ref_15nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_15NODES_LIST, MOD_REF_15NODES_DIR)

        # Create nodes and edges
        nodes_3nodes, edges_3nodes = dag_compiler.create_nodes_edges(mod_ref_3nodes_modrefs)
        nodes_5nodes, edges_5nodes = dag_compiler.create_nodes_edges(mod_ref_5nodes_modrefs)
        nodes_15nodes, edges_15nodes = dag_compiler.create_nodes_edges(mod_ref_15nodes_modrefs)

        # Create DAG
        dag_3nodes = dag_compiler.create_dag(nodes_3nodes, edges_3nodes)
        dag_5nodes = dag_compiler.create_dag(nodes_5nodes, edges_5nodes)
        dag_15nodes = dag_compiler.create_dag(nodes_15nodes, edges_15nodes)
    

    def test_dag_no_modrefs(self):
        """
        DAG is created as expected even in situations where scripts have no mod refs
        """
        mod_ref_nomod_modrefs = dag_compiler.parse_mod_refs(MOD_REF_NOREFS_LIST, MOD_REF_NOREFS_DIR)
        nodes_nomod, edges_nomod = dag_compiler.create_nodes_edges(mod_ref_nomod_modrefs)
        dag = dag_compiler.create_dag(nodes_nomod, edges_nomod)
    

    def test_create_dag_cycle(self):
        """
        Cycle in DAG throws an error
        """
        # Parse mod refs
        dag_cycle_modrefs = dag_compiler.parse_mod_refs(DAG_CYCLE_LIST, DAG_CYCLE_DIR)
        expected_dag_cycle_modrefs = {
            Path('moduleA.py'): None,
            Path('moduleB.py'): [Path('moduleA.py'), Path('moduleE.py')],
            Path('moduleC.py'): Path('moduleA.py'),
            Path('moduleD.py'): [Path('moduleB.py'), Path('moduleA.py'), Path('moduleC.py')],
            Path('moduleE.py'): [Path('moduleA.py'), Path('moduleC.py'), Path('moduleD.py')]
        }
        
        # Sort values of dict for testing equality
        expected_dag_cycle_modrefs_sorted = {k: sorted(v) if isinstance(v, list) else v for k,v in expected_dag_cycle_modrefs.items()}
        dag_cycle_modrefs_sorted = {k: sorted(v) if isinstance(v, list) else v for k,v in dag_cycle_modrefs.items()}
        self.assertEqual(expected_dag_cycle_modrefs_sorted, dag_cycle_modrefs_sorted)

        # Create nodes and edges
        nodes_dag_cycle, edges_dag_cycle = dag_compiler.create_nodes_edges(dag_cycle_modrefs)
        expected_edges_dag_cycle = [
            (Path('moduleA.py'), Path('moduleB.py')),
            (Path('moduleA.py'), Path('moduleC.py')),
            (Path('moduleA.py'), Path('moduleD.py')),
            (Path('moduleA.py'), Path('moduleE.py')),
            (Path('moduleB.py'), Path('moduleD.py')),
            (Path('moduleC.py'), Path('moduleD.py')),
            (Path('moduleC.py'), Path('moduleE.py')),
            (Path('moduleD.py'), Path('moduleE.py')),
            (Path('moduleE.py'), Path('moduleB.py'))
        ]
        self.assertEqual(set(DAG_CYCLE_LIST), set(nodes_dag_cycle))
        self.assertEqual(set(expected_edges_dag_cycle), set(edges_dag_cycle))

        # Create DAG
        with self.assertRaises(prism.exceptions.DAGException) as cm:
            dag_compiler.create_dag(nodes_dag_cycle, edges_dag_cycle)
        self.assertTrue("invalid DAG, cycle found in " in str(cm.exception))
        self.assertTrue("moduleB.py" in str(cm.exception))
        self.assertTrue("moduleE.py" in str(cm.exception))

    
    #########################
    # Get node dependencies #
    #########################

    def test_node_dependencies_3nodes(self):
        """
        Get node dependencies from DAGs with 3 nodes.
        """
        mod_ref_3nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_3NODES_LIST, MOD_REF_3NODES_DIR)
        nodes_3nodes, edges_3nodes = dag_compiler.create_nodes_edges(mod_ref_3nodes_modrefs)
        dag_3nodes = dag_compiler.create_dag(nodes_3nodes, edges_3nodes)

        # Node dependencies
        node_deps_1 = dag_compiler.get_node_dependencies(dag_3nodes, [Path('module01.py')])
        node_deps_2 = dag_compiler.get_node_dependencies(dag_3nodes, [Path('module02.py')])
        node_deps_3 = dag_compiler.get_node_dependencies(dag_3nodes, [Path('module03.py')])
        node_deps_1_2 = dag_compiler.get_node_dependencies(dag_3nodes, [Path('module01.py'), Path('module02.py')])

        # Expected node dependencies
        expected_node_deps_1 = [Path('module01.py')]
        expected_node_deps_2 = [Path('module01.py'), Path('module02.py')]
        expected_node_deps_3 = [Path('module01.py'), Path('module02.py'), Path('module03.py')]
        expected_node_deps_1_2 = [Path('module01.py'), Path('module02.py')]

        # Asserts
        self.assertEqual(set(expected_node_deps_1), set(node_deps_1))
        self.assertEqual(set(expected_node_deps_2), set(node_deps_2))
        self.assertEqual(set(expected_node_deps_3), set(node_deps_3))
        self.assertEqual(set(expected_node_deps_1_2), set(node_deps_1_2))


    def test_node_dependencies_5nodes(self):
        """
        Get node dependencies from DAGs with 5 nodes.
        """
        mod_ref_5nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_5NODES_LIST, MOD_REF_5NODES_DIR)
        nodes_5nodes, edges_5nodes = dag_compiler.create_nodes_edges(mod_ref_5nodes_modrefs)
        dag_5nodes = dag_compiler.create_dag(nodes_5nodes, edges_5nodes)

        # Node dependencies
        node_deps_e = dag_compiler.get_node_dependencies(dag_5nodes, [Path('moduleE.py')])
        expected_node_deps_e = [Path('moduleA.py'), Path('moduleB.py'), Path('moduleC.py'), Path('moduleD.py'), Path('moduleE.py')]
        self.assertEqual(set(expected_node_deps_e), set(node_deps_e))
        
    
    def test_node_dependencies_15nodes(self):
        """
        Get node dependencies from DAGs with 15 nodes.
        """
        mod_ref_15nodes_modrefs = dag_compiler.parse_mod_refs(MOD_REF_15NODES_LIST, MOD_REF_15NODES_DIR)
        nodes_15nodes, edges_15nodes = dag_compiler.create_nodes_edges(mod_ref_15nodes_modrefs)
        dag_15nodes = dag_compiler.create_dag(nodes_15nodes, edges_15nodes)

        # Node dependencies
        node_deps_7 = dag_compiler.get_node_dependencies(dag_15nodes, [Path('module07.py')])
        expected_node_deps_7 = [
            Path('module07.py'),
            Path('module04.py'),
            Path('module06.py'),
            Path('module02.py'),
            Path('module03.py'),
            Path('module05.py'),
            Path('module01.py')
        ]
        self.assertEqual(set(expected_node_deps_7), set(node_deps_7))

        node_deps_all = dag_compiler.get_node_dependencies(dag_15nodes, [Path('module07.py'), Path('module14.py'), Path('module15.py'), Path('module12.py'), Path('module13.py'), Path('module09.py')])
        expected_node_deps_all = [
            Path('module01.py'),
            Path('module02.py'),
            Path('module03.py'),
            Path('module04.py'),
            Path('module05.py'),
            Path('module06.py'),
            Path('module07.py'),
            Path('module08.py'),
            Path('module09.py'),
            Path('module10.py'),
            Path('module11.py'),
            Path('module12.py'),
            Path('module13.py'),
            Path('module14.py'),
            Path('module15.py')
        ]
        self.assertEqual(set(expected_node_deps_all), set(node_deps_all))


    ##################
    # Create topsort #
    ##################


    def test_topsort_varying_complexity(self):
        """
        Create topsort for DAGs with different node counts and dependency complexities. These should not result in an
        error.
        """
        dag_topsort_3nodes = dag_compiler.create_topsort(MOD_REF_3NODES_LIST, MOD_REF_3NODES_LIST, MOD_REF_3NODES_DIR)
        dag_topsort_5nodes = dag_compiler.create_topsort(MOD_REF_5NODES_LIST, MOD_REF_5NODES_LIST, MOD_REF_5NODES_DIR)
        dag_topsort_15nodes = dag_compiler.create_topsort(MOD_REF_15NODES_LIST, MOD_REF_15NODES_LIST, MOD_REF_15NODES_DIR)
        
        # For our simple, 3-node DAG, we know what the topsort list should look like. For the others, there are multiple
        # possible orderings. We initially tried to sort the result of the NetworkX `all_topological_sorts` function
        # returns a generator, but this turned out to be too computationally expensive when the number of possible
        # orderings was high. Therefore, rather than test the value of the list itself, we will confirm that parent
        # nodes appear before their children in the list.
        expected_dag_topsort_3nodes = [Path('module01.py'), Path('module02.py'), Path('module03.py')]
        self.assertEqual(expected_dag_topsort_3nodes, dag_topsort_3nodes)

        # Helper function for testing
        def parent_before_child(parent_list, parent: str, child: Union[str, list]):
            idx1 = parent_list.index(parent)
            if isinstance(child, str):
                idx2 = parent_list.index(child)
                self.assertTrue(idx1<idx2)
            elif isinstance(child, list):
                for node in child:
                    idx2 = parent_list.index(node)
                    self.assertTrue(idx1<idx2)
        
        # 5-node DAG
        parent_before_child(dag_topsort_5nodes, Path('moduleA.py'), [Path('moduleB.py'), Path('moduleC.py'), Path('moduleD.py'), Path('moduleE.py')])
        parent_before_child(dag_topsort_5nodes, Path('moduleB.py'), Path('moduleD.py'))
        parent_before_child(dag_topsort_5nodes, Path('moduleC.py'), [Path('moduleD.py'), Path('moduleE.py')])
        parent_before_child(dag_topsort_5nodes, Path('moduleD.py'), Path('moduleE.py'))

        # 15-node DAG
        parent_before_child(dag_topsort_15nodes, Path('module01.py'), [Path('module10.py'), Path('module02.py'), Path('module03.py'), Path('module05.py'), Path('module08.py')])
        parent_before_child(dag_topsort_15nodes, Path('module10.py'), [Path('module11.py'), Path('module12.py'), Path('module13.py')])
        parent_before_child(dag_topsort_15nodes, Path('module02.py'), Path('module04.py'))
        parent_before_child(dag_topsort_15nodes, Path('module03.py'), Path('module04.py'))
        parent_before_child(dag_topsort_15nodes, Path('module05.py'), [Path('module06.py'), Path('module09.py')])
        parent_before_child(dag_topsort_15nodes, Path('module08.py'), Path('module09.py'))
        parent_before_child(dag_topsort_15nodes, Path('module11.py'), [Path('module14.py'), Path('module15.py')])
        parent_before_child(dag_topsort_15nodes, Path('module04.py'), Path('module07.py'))
        parent_before_child(dag_topsort_15nodes, Path('module06.py'), Path('module07.py'))
    

    def test_topsort_no_modrefs(self):
        """
        DAGs with no dependencies should include all modules (assuming a subset of modules is not explicitly specified).
        """
        dag_topsort_no_modrefs = dag_compiler.create_topsort(MOD_REF_NOREFS_LIST, MOD_REF_NOREFS_LIST, MOD_REF_NOREFS_DIR)

        # All modules must be in DAG
        for mod in MOD_REF_NOREFS_LIST:
            self.assertTrue(mod in dag_topsort_no_modrefs)

    
    def test_topsort_subset(self):
        """
        DAGs created with only a subset of nodes are created normally / without error
        """
        dag_topsort_15nodes_7 = dag_compiler.create_topsort(MOD_REF_15NODES_LIST, [Path('module07.py')], MOD_REF_15NODES_DIR)
        dag_topsort_15nodes_all = dag_compiler.create_topsort(MOD_REF_15NODES_LIST, [Path('module07.py'), Path('module14.py'), Path('module15.py'), Path('module12.py'), Path('module13.py'), Path('module09.py')], MOD_REF_15NODES_DIR)
        dag_topsort_no_modrefs = dag_compiler.create_topsort(MOD_REF_NOREFS_LIST, [Path('moduleB.py'), Path('moduleC.py')], MOD_REF_NOREFS_DIR)

        # In the DAG with 15 nodes, specify which nodes should be included in the DAG based on the modules for compilation
        actual_topsort_15nodes_7 = [Path('module01.py'), Path('module02.py'), Path('module03.py'), Path('module04.py'), Path('module05.py'), Path('module06.py'), Path('module07.py')]
        actual_topsort_15nodes_all = MOD_REF_15NODES_LIST
        self.assertEqual(set(actual_topsort_15nodes_7), set(dag_topsort_15nodes_7))
        self.assertEqual(set(actual_topsort_15nodes_all), set(dag_topsort_15nodes_all))

        # In the DAG without mod refs, there aren't any edges. The only nodes in the DAG will be moduleB and moduleC
        actual_topsort_no_modrefs = [Path('moduleB.py'), Path('moduleC.py')]
        self.assertEqual(set(actual_topsort_no_modrefs), set(dag_topsort_no_modrefs))
        
        
# EOF