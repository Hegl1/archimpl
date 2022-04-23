from mosaic.table_service import Table
from .abstract_expression import AbstractExpression


class OrderBy(AbstractExpression):
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
        column_list = self.column_list
        column_indices = _get_column_indices(column_list, table)

        ordered_records = sorted(table.records, key=lambda record: _get_sort_key(record, column_indices))

        return Table(table.table_name, table.schema_names,
                     table.schema_types, ordered_records)

    def __str__(self):
        return f"OrderBy(columns={self.column_list})"

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
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
