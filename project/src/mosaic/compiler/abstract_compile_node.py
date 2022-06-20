from abc import ABC
from abc import abstractmethod


class AbstractCompileNode(ABC):

    @abstractmethod
    def __str__(self):  # pragma: no cover
        """
        Returns the string used to explain the execution plan for this command.
        """
        pass

    def simplify(self):
        """
        Tries to simplify all child-nodes and then itself.
        The resulting node is returned. If the current node is not replaced, itself is returned.
        Can/should be overridden by the inheriting class
        """
        return self
