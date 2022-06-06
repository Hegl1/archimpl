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
    def replace_all_column_names_by_fqn(self, schema: Schema):  # pragma: no cover
        """
        Recursively replaces all occurrences of column names in the expression by the respective fully qualified
        column names based on the given schema of a table.
        """
        pass