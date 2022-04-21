from .abstract_expression import AbstractExpression
from enum import Enum

class ComparativeOperator(Enum):
    EQUAL=4,
    NOT_EQUAL=5,
    SMALLER=6,
    SMALLER_EQUAL=7,
    GREATER=8,
    GREATER_EQUAL=9


class ComparativeOperationExpression(AbstractExpression):
    def __init__(self, left, operator, right):
        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self):
        # TODO: implement
        pass

    def __str__(self):
        return f"ComparativeOperationExpression(left={self.left},right={self.right},operator={self.operator})"

    def explain(self, rows, indent):
        pass
