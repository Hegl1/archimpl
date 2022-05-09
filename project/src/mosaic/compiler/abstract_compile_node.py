from abc import ABC
from abc import abstractmethod


class AbstractCompileNode(ABC):

    @abstractmethod
    def __str__(self):  # pragma: no cover
        """
        Returns the string used to explain the execution plan for this command.
        """
        pass
