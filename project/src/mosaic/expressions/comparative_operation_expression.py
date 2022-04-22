from .abstract_expression import AbstractExpression
from enum import Enum
from mosaic.table_service import SchemaType, get_schema_type
from .column_expression import ColumnExpression


class ComparativeOperator(Enum):
    EQUAL = "="
    NOT_EQUAL = "!="
    SMALLER = "<"
    SMALLER_EQUAL = "<="
    GREATER = ">"
    GREATER_EQUAL = ">="


class IncompatibleOperandTypesException(Exception):
    pass


class ComparativeOperationExpression(AbstractExpression):
    def __init__(self, left, operator, right):
        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self, table, row_index):
        left_operand = self._get_operand(
            table, row_index, expression=self.left)
        right_operand = self._get_operand(
            table, row_index, expression=self.right)

        if left_operand is None and right_operand is None and self.operator is ComparativeOperator.EQUAL:
            return True

        # handles None literals
        if left_operand is None or right_operand is None:
            return False

        if type(left_operand) != type(right_operand):
            raise IncompatibleOperandTypesException(
                "Operands of a comparison operator must be of the same type")

        if self.operator == ComparativeOperator.EQUAL:
            return left_operand == right_operand
        elif self.operator == ComparativeOperator.NOT_EQUAL:
            return left_operand != right_operand
        elif self.operator == ComparativeOperator.SMALLER:
            return left_operand < right_operand
        elif self.operator == ComparativeOperator.SMALLER_EQUAL:
            return left_operand <= right_operand
        elif self.operator == ComparativeOperator.GREATER:
            return left_operand > right_operand
        elif self.operator == ComparativeOperator.GREATER_EQUAL:
            return left_operand >= right_operand

    def _get_operand(self, table, row_index, expression):
        """
        Returns the actual operand for the given expression
        """
        if isinstance(expression, ComparativeOperationExpression):
            return expression.get_result(table, row_index)
        elif isinstance(expression, ColumnExpression):
            return table[row_index, expression.get_result()]

        return expression.get_result()

    def __str__(self):
        return f"({self.left} {self.operator.value} {self.right})"

    def explain(self, rows, indent):
        pass
