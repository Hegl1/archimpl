from .abstract_expression import AbstractExpression
from .column_expression import ColumnExpression
from mosaic.table_service import SchemaType
from mosaic.table_service import WrongSchemaTypeException
from enum import Enum

class ArithmeticOperator(Enum):
    TIMES=0,
    DIVIDE=1,
    ADD=2,
    SUBTRACT=3


class IncompatibleOperationException(Exception):
    pass


class ArithmeticOperationExpression(AbstractExpression):
    def __init__(self, left, operator, right):
        self.left = left
        self.right = right
        self.operator = operator

    def get_result(self, table, row_index):
        left_operand = self._get_operand(table, row_index, expression = self.left)
        right_operand = self._get_operand(table, row_index, expression = self.right)

        # TODO: do handle null?
        if self.operator == ArithmeticOperator.TIMES:
            return left_operand * right_operand
        elif self.operator == ArithmeticOperator.DIVIDE:
            return left_operand / right_operand
        elif self.operator == ArithmeticOperator.ADD:
            if isinstance(left_operand, str) or isinstance(right_operand, str):
                left_operand = str(left_operand)
                right_operand = str(right_operand)

            return left_operand + right_operand
        elif self.operator == ArithmeticOperator.SUBTRACT:
            return left_operand - right_operand

    def get_schema_type(self, table):
        left_schema = self._get_schema_type_for_expression(table, self.left)
        right_schema = self._get_schema_type_for_expression(table, self.right)

        if left_schema == SchemaType.VARCHAR or right_schema == SchemaType.VARCHAR:
            if self.operator != ArithmeticOperator.ADD:
                raise IncompatibleOperationException("The given operation is not applicable for strings")

            return SchemaType.VARCHAR
        elif left_schema == SchemaType.FLOAT or right_schema == SchemaType.FLOAT:
            return SchemaType.FLOAT

        return SchemaType.INT

    def _get_schema_type_for_expression(self, table, expression):
        if isinstance(expression, ArithmeticOperationExpression):
            return expression.get_schema_type(table)
        elif isinstance(expression, ColumnExpression):
            return table.schema_types[table.get_column_index(expression.get_result())]

        if isinstance(expression.get_result(), int):
            return SchemaType.INT
        elif isinstance(expression.get_result(), float):
            return SchemaType.FLOAT
        elif isinstance(expression.get_result(), str):
            return SchemaType.VARCHAR
        else:
            raise WrongSchemaTypeException

    def _get_operand(self, table, row_index, expression):
        if isinstance(expression, ArithmeticOperationExpression):
            return expression.get_result(table, row_index)
        elif isinstance(expression, ColumnExpression):
            return table[row_index, expression.get_result()]

        return expression.get_result()

    def __str__(self):
        return f"ArithmeticOperationExpression(left={self.left},right={self.right},operator={self.operator})"
