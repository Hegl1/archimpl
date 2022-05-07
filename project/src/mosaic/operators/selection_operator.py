from mosaic.expressions.abstract_expression import AbstractExpression
from mosaic.table_service import Table, Schema


class Selection(AbstractExpression):
    def __init__(self, table_reference, condition):
        super().__init__()
        self.table_reference = table_reference
        self.condition = condition

    def get_result(self):
        table = self.table_reference.get_result()
        result = []

        for i, record in enumerate(table.records):
            if self.condition.get_result(table, i):
                result.append(record)
        schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)

        return Table(schema, result)

    def get_schema(self):
        return self.table_reference.get_schema()

    def __str__(self):
        schema = self.get_schema()
        if self.condition is not None:
            self.condition.replace_all_column_names_by_fqn(schema)
        return f"Selection(condition={self.condition.__str__()})"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
