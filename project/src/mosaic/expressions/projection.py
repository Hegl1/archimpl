from .abstract_expression import AbstractExpression
from .arithmetic_operation_expression import ArithmeticOperationExpression
from mosaic.table_service import Table

class Projection(AbstractExpression):
    def __init__(self, column_references, table_reference):
        super().__init__()

        self.column_references = column_references
        self.table_reference = table_reference

    def get_result(self):
        table: Table = self.table_reference.get_result()

        schema_names, schema_types, columns = self._build_schema(table)
        data = self._build_data(table, columns)

        return Table(table.table_name, schema_names, schema_types, data)

    def _build_schema(self, table):
        schema_names = []
        schema_types = []

        columns = []

        for (alias, column_reference) in self.column_references:

            if isinstance(column_reference, ArithmeticOperationExpression):
                column_value = column_reference

                schema_names.append(alias)
                schema_types.append(column_reference.get_schema_type(table))
            else:
                column = column_reference.get_result()
                column_name = table.get_simple_column_name(column)
                column_value = table.get_column_index(column)

                schema_names.append(column_name if alias is None else alias)
                schema_types.append(table.schema_types[column_value])

            columns.append(column_value)

        return (schema_names, schema_types, columns)

    def _build_data(self, table, columns):
        data = []

        for (index, record) in enumerate(table.records):
            row = []
            for column_value in columns:
                if isinstance(column_value, ArithmeticOperationExpression):
                    row.append(column_value.get_result(table, index))
                else:
                    row.append(record[column_value])
            data.append(row)

        return data

    def __str__(self):
        column_names = list(map(lambda column: column[1].get_result(), self.column_references))
        return f"Projection(columns={column_names})"