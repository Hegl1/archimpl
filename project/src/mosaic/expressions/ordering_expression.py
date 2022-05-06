from mosaic.table_service import Table, Schema
from .abstract_expression import AbstractExpression


class OrderingExpression(AbstractExpression):
    """
    Represents an ordering operation
    result can be retrieved with get_result method
    an explanation of the operation is created in the explain method
    """

    def __init__(self, column_list, table_reference):
        super().__init__()
        self.column_list = column_list
        self.table_reference = table_reference

    def get_result(self):
        table = self.table_reference.get_result()
        column_indices = _get_column_indices(self.column_list, table)

        ordered_records = sorted(table.records, key=lambda record: _get_sort_key(record, column_indices))
        schema = Schema(table.get_table_name(), table.schema.column_names, table.schema.column_types)

        return Table(schema, ordered_records)

    def get_schema(self):
        return self.table_reference.get_schema()

    def __str__(self):
        schema = self.get_schema()
        column_name_strings = [schema.get_fully_qualified_column_name(str(column)) for column in self.column_list]
        return f"OrderBy(key=[{', '.join(column_name_strings)}])"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)


def _get_column_indices(column_list, table):
    column_indices = []
    for column_name in column_list:
        column_indices.append(table.get_column_index(column_name.get_result()))
        # (non-)existence of columns is already handled in the get_column_index method
    return column_indices


def _get_sort_key(record, column_indices):
    sort_key = []
    for i in column_indices:
        sort_key.append(record[i])
    return sort_key
