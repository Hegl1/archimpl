from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class Ordering(AbstractOperator):
    """
    Class that represents an ordering operation.
    In this implementation the sorted() function of python is used for sorting which has a complexity of O(n log n).
    """

    def __init__(self, node, column_list):
        super().__init__()
        self.node = node
        self.column_list = column_list

    def get_result(self):
        table = self.node.get_result()
        column_indices = _get_column_indices(self.column_list, table)

        ordered_records = sorted(table.records, key=lambda record: _get_sort_key(record, column_indices))
        schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)

        return Table(schema, ordered_records)

    def get_schema(self):
        return self.node.get_schema()

    def simplify(self):
        self.node = self.node.simplify()

        return self

    def __str__(self):
        schema = self.get_schema()
        column_name_strings = [schema.get_fully_qualified_column_name(str(column)) for column in self.column_list]
        return f"OrderBy(key=[{', '.join(column_name_strings)}])"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.node.explain(rows, indent + 2)


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
