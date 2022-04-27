from .abstract_expression import AbstractExpression
from mosaic.table_service import Table


class Selection(AbstractExpression):
    def __init__(self, table_reference, condition):
        super().__init__()
        self.table_reference = table_reference
        self.condition = condition

    def get_result(self):
        table = self.table_reference.get_result()
        row_count = len(table)
        result = []

        for i in range(0, row_count):
            if self.condition.get_result(table, i):
                result.append(table.records[i])

        return Table(table.table_name, table.schema_names, table.schema_types, result)

    def __str__(self):
        return f"Selection(condition={self.condition.__str__()})"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
