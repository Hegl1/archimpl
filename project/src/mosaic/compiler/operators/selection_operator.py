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

    def __init__(self, table_reference, condition):
        super().__init__()
        self.table_reference = table_reference
        self.condition = condition

    def get_result(self):
        table = self.table_reference.get_result()
        result = []

        if isinstance(self.condition, LiteralExpression):
            result = self.condition.get_result()

            if result:
                return table
            else:
                schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)
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

        schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)

        return Table(schema, result)

    def get_schema(self):
        return self.table_reference.get_schema()

    def simplify(self):
        self.table_reference = self.table_reference.simplify()
        self.condition = self.condition.simplify()

        if isinstance(self.condition, LiteralExpression):
            result = self.condition.get_result()

            if result:
                return self.table_reference

        return self

    def __str__(self):
        schema = self.get_schema()
        if self.condition is not None:
            self.condition.replace_all_column_names_by_fqn(schema)
        return f"Selection(condition={self.condition.__str__()})"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
