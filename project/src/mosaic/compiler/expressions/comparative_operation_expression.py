from enum import Enum

from mosaic.table_service import Schema
from .abstract_computation_expression import AbstractComputationExpression
from .literal_expression import LiteralExpression
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


class ComparativeOperationExpression(AbstractComputationExpression):
    """
    Class that represents a comparison operation.
    This class has the following properties:
    left: the left operand
    right: the right operand
    operator: the comparison operator
    """

    def __init__(self, left, operator, right):
        super().__init__()

        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self, table, row_index):
        left_operand = self._get_operand(
            table, row_index, expression=self.left)
        right_operand = self._get_operand(
            table, row_index, expression=self.right)

        return self._compute(left_operand, right_operand)

    def _compute(self, left_operand, right_operand):
        if (left_operand is None or right_operand is None) and self.operator not in [ComparativeOperator.EQUAL,
                                                                                     ComparativeOperator.NOT_EQUAL]:
            return 0

        try:
            if self.operator == ComparativeOperator.EQUAL:
                return int(left_operand == right_operand)
            elif self.operator == ComparativeOperator.NOT_EQUAL:
                return int(left_operand != right_operand)
            elif self.operator == ComparativeOperator.SMALLER:
                return int(left_operand < right_operand)
            elif self.operator == ComparativeOperator.SMALLER_EQUAL:
                return int(left_operand <= right_operand)
            elif self.operator == ComparativeOperator.GREATER:
                return int(left_operand > right_operand)
            elif self.operator == ComparativeOperator.GREATER_EQUAL:
                return int(left_operand >= right_operand)
        except TypeError:
            raise IncompatibleOperandTypesException("Operands of a comparison operation must be compatible")

    def _get_operand(self, table, row_index, expression):
        """
        Returns the actual operand for the given expression
        """
        if isinstance(expression, AbstractComputationExpression):
            return expression.get_result(table, row_index)
        elif isinstance(expression, ColumnExpression):
            return table[row_index, expression.get_result()]

        return expression.get_result()

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()

        if isinstance(self.left, LiteralExpression) and isinstance(self.right, LiteralExpression):
            return LiteralExpression(self._compute(self.left.get_result(), self.right.get_result()))

        return self

    def replace_all_column_names_by_fqn(self, schema: Schema):
        """
        Recursively replaces all occurrences of column names in the expression by the respective fully qualified
        column names based on the given schema of a table.
        """
        self.left.replace_all_column_names_by_fqn(schema)
        self.right.replace_all_column_names_by_fqn(schema)

    def __str__(self):
        return f"({self.left} {self.operator.value} {self.right})"
