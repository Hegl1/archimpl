from .abstract_expression import AbstractExpression
from mosaic.table_service import Table

class Projection(AbstractExpression):
    def __init__(self, column_references, table_reference):
        super().__init__()

        self.column_references = column_references
        self.table_reference = table_reference

    def get_result(self):
        table: Table = self.table_reference.get_result()

        schema_names = []
        schema_types = []
        data = []

        selected_indices = []

        for (alias, column_reference) in self.column_references:
            column = column_reference.get_result()

            column_name = table.get_simple_column_name(column)

            column_index = table.get_column_index(column)

            schema_names.append(column_name if alias is None else alias)
            schema_types.append(table.schema_types[column_index])

            selected_indices.append(column_index)
        
        for record in table.records:
            row = []
            for index in selected_indices:
                row.append(record[index])
            data.append(row)

        return Table(table.table_name, schema_names, schema_types, data)

    def __str__(self):
        column_names = list(map(lambda column: column[1].get_result(), self.column_references))
        return f"Projection(columns={column_names})"