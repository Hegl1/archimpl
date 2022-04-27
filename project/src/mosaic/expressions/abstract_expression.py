from abc import ABC
from abc import abstractmethod

class AbstractExpression(ABC):
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

    @abstractmethod
    def explain(self, rows, indent): # pragma: no cover
        """
        Method to build a list of strings for the explain command.
        Adds the representative String of the current command
        in the list of rows and then calls this method on child nodes.
        The representative string needs to be correctly indented and wrapped in a list.

        Args:
            rows: list to add the representative string to.
            indent: Describes the level of indentation at the current command.
        """
        pass
