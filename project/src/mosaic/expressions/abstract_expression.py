from abc import ABC
from abc import abstractmethod

class AbstractExpression(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def get_result(self):
        """
        Computes the result for this expression-node and returns it.
        For that it calls the compute nodes of the child-nodes recursively.
        Returns a Table-Object
        """
        pass

    @abstractmethod
    def __str__(self):
        """
        Returns the execution plan for this expression-node recursively.
        """
        pass
