from .abstract_computation_expression import AbstractComputationExpression
from .column_expression import ColumnExpression
from mosaic.table_service import SchemaType, get_schema_type
from enum import Enum

class ArithmeticOperator(Enum):
    TIMES = "*"
    DIVIDE = "/"
    ADD = "+"
    SUBTRACT = "-"


class IncompatibleOperationException(Exception):
    pass


class ArithmeticOperationExpression(AbstractComputationExpression):
    def __init__(self, left, operator, right):
        super().__init__()

        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self, table, row_index):
        """
        Returns the result for a table and the given row (with the given index)
        """
        left_operand = self._get_operand(table, row_index, expression = self.left)
        right_operand = self._get_operand(table, row_index, expression = self.right)

        if left_operand is None or right_operand is None:
            return None

        if self.operator == ArithmeticOperator.TIMES:
            return left_operand * right_operand
        elif self.operator == ArithmeticOperator.DIVIDE:
            return float(left_operand / right_operand)
        elif self.operator == ArithmeticOperator.ADD:
            if isinstance(left_operand, str) or isinstance(right_operand, str):
                left_operand = str(left_operand)
                right_operand = str(right_operand)

            return left_operand + right_operand
        elif self.operator == ArithmeticOperator.SUBTRACT:
            return left_operand - right_operand

    def get_schema_type(self, table):
        """
        Computes the schema_type for the expression by evaluating the types of
        the both operands.
        """
        left_schema = self._get_schema_type_for_expression(table, self.left)
        right_schema = self._get_schema_type_for_expression(table, self.right)

        if left_schema == SchemaType.NULL and right_schema == SchemaType.NULL:
            return SchemaType.NULL

        if left_schema == SchemaType.VARCHAR or right_schema == SchemaType.VARCHAR:
            if self.operator != ArithmeticOperator.ADD:
                raise IncompatibleOperationException("The given operation is not applicable for strings")

            return SchemaType.VARCHAR
        elif left_schema == SchemaType.FLOAT or right_schema == SchemaType.FLOAT or self.operator == ArithmeticOperator.DIVIDE:
            return SchemaType.FLOAT

        return SchemaType.INT

    def _get_schema_type_for_expression(self, table, expression):
        """
        Returns the schema_type for the given expression in the given table
        """
        if isinstance(expression, ArithmeticOperationExpression):
            return expression.get_schema_type(table)
        elif isinstance(expression, ColumnExpression):
            return table.schema.column_types[table.get_column_index(expression.get_result())]

        value = expression.get_result()

        return get_schema_type(value)

    def _get_operand(self, table, row_index, expression):
        """
        Returns the actual operand for the given expression
        """
        if isinstance(expression, AbstractComputationExpression):
            return expression.get_result(table, row_index)
        elif isinstance(expression, ColumnExpression):
            return table[row_index, expression.get_result()]

        return expression.get_result()

    def __str__(self):
        return f"{{{self.left} {self.operator.value} {self.right}}}"
