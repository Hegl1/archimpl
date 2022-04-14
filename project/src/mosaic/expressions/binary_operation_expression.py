from xmlrpc.client import Boolean
from .abstract_expression import AbstractExpression
from enum import Enum

class BinaryOperator(Enum):
    TIMES=0,
    DIVIDE=1


class BinaryOperationExpression(AbstractExpression):
    def __init__(self, left, operator, right):
        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self):
        if self.operator == BinaryOperator.TIMES:
            self.left.get_result() * self.right.get_result()
        elif self.operator == BinaryOperator.DIVIDE:
            self.left.get_result() / self.right.get_result()

    def __str__(self):
        return f"BinaryOperationExpression(left={self.left},right={self.right},operator={self.operator})"
