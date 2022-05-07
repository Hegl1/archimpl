from abc import abstractmethod
from mosaic.compiler.abstract_compile_node import AbstractCompileNode


class AbstractExpression(AbstractCompileNode):
    def __init__(self):
        pass

    @abstractmethod
    def get_result(self): # pragma: no cover
        """
        Computes the result for this expression-node and returns it.
        For that it calls the compute nodes of the child-nodes recursively.
        Returns a Table-Object
        """
        pass

    @abstractmethod
    def __str__(self): # pragma: no cover
        """
        Returns the string used to explain the execution plan for this command.
        """
        pass
