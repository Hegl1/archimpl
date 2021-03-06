from enum import Enum

from mosaic.compiler.get_string_representation import get_string_representation
from mosaic.table_service import SchemaType, get_schema_type, Schema
from .abstract_computation_expression import AbstractComputationExpression
from .literal_expression import LiteralExpression
from .column_expression import ColumnExpression


class ArithmeticOperator(Enum):
    TIMES = "*"
    DIVIDE = "/"
    ADD = "+"
    SUBTRACT = "-"


class IncompatibleOperationException(Exception):
    pass


class ArithmeticExpression(AbstractComputationExpression):
    """
    Class that represents a binary arithmetic operation.
    This implementation includes the basic arithmetic operations of addition, subtraction, multiplication and division.
    This class has the following properties:
    left: the left operand
    right: the right operand
    operator: the arithmetic operator
    """

    def __init__(self, left, operator, right):
        super().__init__()

        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self, table, row_index):
        """
        Returns the result for a table and the given row (with the given index)
        """
        left_operand = self._get_operand(table, row_index, expression=self.left)
        right_operand = self._get_operand(table, row_index, expression=self.right)

        return self._compute(left_operand, right_operand)

    def _compute(self, left_operand, right_operand):
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

    def get_schema_type(self, schema):
        """
        Computes the schema_type for the expression by evaluating the types of
        the both operands.
        """
        left_type = self._get_schema_type_for_expression(schema, self.left)
        right_type = self._get_schema_type_for_expression(schema, self.right)

        if left_type == SchemaType.NULL and right_type == SchemaType.NULL:
            return SchemaType.NULL

        if left_type == SchemaType.VARCHAR or right_type == SchemaType.VARCHAR:
            if self.operator != ArithmeticOperator.ADD:
                raise IncompatibleOperationException("The given operation is not applicable for strings")

            return SchemaType.VARCHAR
        elif left_type == SchemaType.FLOAT or right_type == SchemaType.FLOAT or self.operator == ArithmeticOperator.DIVIDE:
            return SchemaType.FLOAT

        return SchemaType.INT

    def _get_schema_type_for_expression(self, schema, expression):
        """
        Returns the schema_type for the given expression in the given table
        """
        if isinstance(expression, ArithmeticExpression):
            return expression.get_schema_type(schema)
        elif isinstance(expression, ColumnExpression):
            return schema.column_types[schema.get_column_index(expression.get_result())]

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

    def simplify(self):
        self.left = self.left.simplify()
        self.right = self.right.simplify()

        if isinstance(self.left, LiteralExpression) and isinstance(self.right, LiteralExpression):
            return LiteralExpression(self._compute(self.left.get_result(), self.right.get_result()))

        return self

    def get_string_representation(self, schema: Schema = None):
        return f"{{{get_string_representation(self.left, schema)} {self.operator.value} {get_string_representation(self.right, schema)}}}"
