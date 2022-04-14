from lib2to3.pgen2.token import EQUAL
from xmlrpc.client import Boolean
from .abstract_expression import AbstractExpression
from enum import Enum

class BinaryOperator(Enum):
    TIMES=0,
    DIVIDE=1,
    ADD=2,
    SUBTRACT=3,
    EQUAL=4,
    NOT_EQUAL=5,
    SMALLER=6,
    SMALLER_EQUAL=7,
    GREATER=8,
    GREATER_EQUAL=9


class BinaryOperationExpression(AbstractExpression):
    def __init__(self, left, operator, right):
        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self):
        if self.operator == BinaryOperator.TIMES:
            return self.left.get_result() * self.right.get_result()
        elif self.operator == BinaryOperator.DIVIDE:
            return self.left.get_result() / self.right.get_result()
        elif self.operator == BinaryOperator.ADD:
            return self.left.get_result() + self.right.get_result()
        elif self.operator == BinaryOperator.SUBTRACT:
            return self.left.get_result() - self.right.get_result()
        else:
            #TODO: implement comparitive operators
            pass 

    def __str__(self):
        return f"BinaryOperationExpression(left={self.left},right={self.right},operator={self.operator})"
