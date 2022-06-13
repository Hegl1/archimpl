from mosaic.compiler.get_string_representation import get_string_representation
from mosaic.compiler.expressions.abstract_expression import AbstractExpression
from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator
from ..expressions.abstract_computation_expression import AbstractComputationExpression
from ..expressions.column_expression import ColumnExpression
from ..expressions.literal_expression import LiteralExpression


class Selection(AbstractOperator):
    """
    Class that represents a selection operation.
    It returns a table which only contains records which fulfill the given condition.
    """

    def __init__(self, node, condition):
        super().__init__()
        self.node = node
        self.condition = condition

    def get_result(self):
        table = self.node.get_result()
        result = []

        schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)

        if isinstance(self.condition, LiteralExpression):
            result = self.condition.get_result()

            if result:
                return table
            else:
                return Table(schema, [])

        for i, record in enumerate(table.records):
            if isinstance(self.condition, AbstractComputationExpression):
                condition_result = self.condition.get_result(table, i)
            elif isinstance(self.condition, ColumnExpression):
                condition_result = table[i, self.condition.get_result()]
            else:
                condition_result = self.condition.get_result()
            if condition_result:
                result.append(record)

        return Table(schema, result)

    def get_schema(self):
        return self.node.get_schema()

    def simplify(self):
        self.node = self.node.simplify()
        self.condition = self.condition.simplify()

        if isinstance(self.condition, LiteralExpression):
            result = self.condition.get_result()

            if result:
                return self.node

        return self

    def __str__(self):
        schema = self.get_schema()

        return f"Selection(condition={get_string_representation(self.condition, schema)})"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.node.explain(rows, indent + 2)
