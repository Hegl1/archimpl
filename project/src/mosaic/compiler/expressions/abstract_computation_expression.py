from abc import abstractmethod
from .abstract_expression import AbstractExpression


class AbstractComputationExpression(AbstractExpression):
    """
    Class that represents an AbstractExpression, but calculates the result for each row of a table individually,
    by passing the table and the row_index as parameters to the get_result function.
    """

    @abstractmethod
    def get_result(self, table, row_index): # pragma: no cover
        """
        Computes the result for this expression-node for the given table and the row with the given index.
        For that it calls the compute nodes of the child-nodes recursively.
        Returns a Table-Object
        """
        pass
