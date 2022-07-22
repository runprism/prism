"""
Class for parsing Python ASTs.

Table of Contents:
- Imports
- Class definition
"""


#############
## Imports ##
#############

# Standard library imports
import ast
from pathlib import Path
from typing import List, Optional, Tuple, Union

# Prism imports
import prism.exceptions


######################
## Class definition ##
######################

# Constant
prism_function_alias = 'psm'

class AstParser:
    """
    Class for converting module into AST and parsing the resulting tree
    """

    def __init__(self,
        module_relative_path: Path,
        parent_path: Path
    ):
        self.module_relative_path = module_relative_path
        self.parent_path = parent_path
        self.module_path = Path(self.parent_path / self.module_relative_path)
        with open(self.module_path, 'r') as f:
            self.module_str = f.read()
        f.close()
        self.ast_module = ast.parse(self.module_str)

        # Check existence of if-name-main
        bool_if_name_main = self.check_if_name_main(self.ast_module)
        if bool_if_name_main:
            msg_list = [
                f'found `if __name__=="__main__"` in `{str(self.module_relative_path)}`',
                'all task-specific code should be placed in `run` method',
                'please fix and try again'
            ]
            raise prism.exceptions.ParserException('\n'.join(msg_list))

        # Get classes and bases
        self.classes, self.bases = self.get_classes_bases(self.ast_module)

    
    def get_classes_bases(self,
        module: ast.Module
    ) -> Tuple[List[ast.ClassDef], List[List[ast.expr]]]:
        """
        Get the classes ans associated bases
        
        args:
            module: module represented as an AST node
        returns:
            list of classes and associated bases
        """
        classes = [n for n in module.body if isinstance(n, ast.ClassDef)]
        class_bases = [class_.bases for class_ in classes]
        return classes, class_bases

    
    def get_num_prism_tasks(self,
        bases: List[List[ast.expr]]
    ) -> int:
        """
        Get number of PrismTasks from `bases`
        
        args:
            bases: list of bases associated with classes in module
        returns:
            number of PrismTasks
        """
        prism_tasks = 0
        for base_ in bases:
            for obj in base_:
                if isinstance(obj, ast.Name):
                    if obj.id=="PrismTask":
                        prism_tasks+=1
                elif isinstance(obj, ast.Attribute):
                    if obj.attr=="PrismTask":
                        prism_tasks+=1
        return prism_tasks


    def get_prism_task_node(self,
        classes: List[ast.ClassDef],
        bases: List[List[ast.expr]]
    ) -> Optional[ast.ClassDef]:
        """
        Get the node associated with the prism task from `module`
        
        args:
            module: module represented as an AST node
        returns:
            node associated with prism task
        """
        # If there are no bases, then there are no class definitions. Throw an error
        if len(bases)==0:
            return None
        for class_, base_ in zip(classes, bases):
            for obj in base_:
                if isinstance(obj, ast.Name):
                    if obj.id=="PrismTask":
                        return class_
                elif isinstance(obj, ast.Attribute):
                    if obj.attr=="PrismTask":
                        return class_
        return None
    

    def get_all_funcs(slef, prism_task: ast.ClassDef) -> List[ast.FunctionDef]:
        """
        Get all functions from PrismTask class

        args:
            prism_task: PrismTask class as an AST class
        returns:
            run function as an ast.FunctionDef
        """
        return [f for f in prism_task.body if isinstance(f, ast.FunctionDef)]
        

    def get_run_func(self, prism_task: ast.ClassDef) -> Optional[ast.FunctionDef]:
        """
        Get `run` function from PrismTask class
        
        args:
            prism_task: PrismTask class as an AST class
        returns:
            run function as an ast.FunctionDef
        """
        functions = [f for f in prism_task.body if isinstance(f, ast.FunctionDef)]
        for func in functions:
            if func.name=="run":
                return func
        return None
    

    def get_func_args(self, func: ast.FunctionDef) -> List[str]:
        """
        Get arguments of `func` as a list of strings
        
        args:
            func: function for which to retrieve arguments
        returns:
            arguments of `func` as a list of strings
        """
        results = []
        args_list = func.args.args
        for a in args_list:
            results.append(a.arg)
        return results
    

    def get_prism_mod_calls(self, func: ast.FunctionDef) -> List[Path]:
        f"""
        Get calls to `{prism_function_alias}.mod` from `func`
        
        args:
            func: run function represented as an ast.FunctionDef object
        returns:
            calls to prism.mod contained within function
        """
        mod_calls=[]
        
        # Get all function calls
        call_objs = []
        for node in ast.walk(func):
            if isinstance(node, ast.Call):
                call_objs.append(node)
        
        # Iterate through function calls
        for c in call_objs:
            if not isinstance(c.func, ast.Attribute):
                continue
            else:
                try:
                    if c.func.value.id==prism_function_alias and c.func.attr=='mod': # type: ignore
                        args = c.args
                        if len(args)>1:
                            raise prism.exceptions.ParserException(f'too many arguments in `{prism_function_alias}.mod` call')
                        if Path(args[0].s) not in mod_calls: # type: ignore
                            if args[0].s==str(self.module_relative_path): # type: ignore
                                raise prism.exceptions.ParserException(message=f'self-references found in `{str(self.module_relative_path)}`')
                            mod_calls.append(Path(args[0].s)) # type: ignore
                
                # If we encounter an Attribute error, then the call object producing the error is not of interest to us.
                # Skip.
                except AttributeError:
                    pass
        
        return mod_calls
    

    def check_if_name_main(self,
        ast_module: ast.Module
    ) -> bool:
        """
        Check if `ast_module` has an if __name__=="__main__" block.

        args:
            ast_module: module represented as an AST
        returns:
            boolean indicating if `ast_module` has an if __name__=="__main__" block 
        """
        #TODO: optimize function by removing duplicate DFS searches via ast.walk

        # If-name-main block needs to appear in main body
        if_name_main_blocks = [c for c in ast_module.body if isinstance(c, ast.If)]
        for node in if_name_main_blocks:
            compares = []
            for nested_node in ast.walk(node):
                if isinstance(nested_node, ast.Compare):
                    compares.append(nested_node)
            
            # For each compare object, iterate through all sub-nodes and examine if there is a Name object with id
            # __name__ and a string object with s "__main__".
            for comp in compares:
                has_name = 0
                has_main = 0
                for comp_subnode in ast.walk(comp):
                    if isinstance(comp_subnode, ast.Name) and comp_subnode.id=="__name__":
                        has_name = 1
                    if isinstance(comp_subnode, ast.Str) and comp_subnode.s=="__main__":
                        has_main = 1
        
                if max(has_name, has_main)==1:
                    return True
        return False
    

    def parse(self) -> Union[List[Path], Path]:
        """
        Parse module and return mod references
        """
        # This will throw an error if the number of PrismTasks!=1
        num_prism_task_classes = self.get_num_prism_tasks(self.bases)
        if num_prism_task_classes==0:
            raise prism.exceptions.ParserException(message=f"no PrismTask in `{str(self.module_relative_path)}`")
        elif num_prism_task_classes>1:
            raise prism.exceptions.ParserException(message=f"too many PrismTasks in `{str(self.module_relative_path)}`")
        
        # Get PrismTask, run function, and mod calls
        prism_task_class_node = self.get_prism_task_node(self.classes, self.bases)
        if prism_task_class_node is None:
            raise prism.exceptions.ParserException(message=f"no PrismTask in `{str(self.module_relative_path)}`")

        # Confirm run function is properly structured
        run_func = self.get_run_func(prism_task_class_node)
        if run_func is None:
            raise prism.exceptions.ParserException(message=f"no `run` function in PrismTask in `{str(self.module_relative_path)}`")
        run_args = self.get_func_args(run_func)
        if sorted(run_args)!=sorted([prism_function_alias, 'self']):
            msg_list = [
                f'invalid arguments in `run` function in PrismTask in {str(self.module_relative_path)}',
                f'should only be `self` and `{prism_function_alias}`'
            ]
            raise prism.exceptions.ParserException(message='\n'.join(msg_list))

        # Iterate through all functions and get prism mod calls
        all_funcs = self.get_all_funcs(prism_task_class_node)
        all_mods: List[Path] = []
        for func in all_funcs:
            all_mods+=self.get_prism_mod_calls(func)
        if len(all_mods)==1:
            return all_mods[0]
        else:
            return all_mods


# EOF