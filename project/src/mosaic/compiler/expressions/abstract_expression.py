from abc import abstractmethod

from mosaic.compiler.abstract_compile_node import AbstractCompileNode
from mosaic.table_service import Schema


class AbstractExpression(AbstractCompileNode):
    def __init__(self):
        pass

    @abstractmethod
    def get_result(self):  # pragma: no cover
        """
        Computes the result for this expression-node and returns it.
        For that it calls the compute nodes of the child-nodes recursively.
        """
        pass

    @abstractmethod
    def get_string_representation(self, schema: Schema = None):  # pragma: no cover
        """
        Generates the string-representation of this expression
        """
        pass

    def __str__(self):
        return self.get_string_representation()