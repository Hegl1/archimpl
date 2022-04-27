from .abstract_expression import AbstractExpression
from .arithmetic_operation_expression import ArithmeticOperationExpression
from .literal_expression import LiteralExpression
from mosaic.table_service import Table, get_schema_type


class InvalidAliasException(Exception):
    pass


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
        """
        Builds the schema for the projection-result, including the columns.
        columns is a list containing:
        - integers (index of the value in the reference table)
        - ArithmeticOperationExpressions (get_result used for calculation of row value)
        - LiteralExpression (get_result used for value)

        Returns the following tuple: (schema_names, schema_types, columns)
        """
        schema_names = []
        schema_types = []

        columns = []

        for (alias, column_reference) in self.column_references:
            if alias is not None and "." in alias:
                raise InvalidAliasException("Aliases cannot contain \".\"")

            if isinstance(column_reference, ArithmeticOperationExpression):
                column_value = column_reference

                schema_names.append(alias)
                schema_types.append(column_reference.get_schema_type(table))
            elif isinstance(column_reference, LiteralExpression):
                column_value = column_reference

                schema_names.append(alias)
                schema_types.append(get_schema_type(column_reference.get_result()))
            else:
                column = column_reference.get_result()
                column_name = table.get_fully_qualified_column_name(column)
                column_value = table.get_column_index(column)

                schema_names.append(column_name if alias is None else alias)
                schema_types.append(table.schema_types[column_value])

            columns.append(column_value)

        return (schema_names, schema_types, columns)

    def _build_data(self, table, columns):
        """
        Builds the data (rows/records) for the projection-result.
        For this it uses the columns returned by the _build_schema method
        """
        data = []

        for (index, record) in enumerate(table.records):
            row = []
            for column_value in columns:
                if isinstance(column_value, ArithmeticOperationExpression):
                    row.append(column_value.get_result(table, index))
                elif isinstance(column_value, LiteralExpression):
                    row.append(column_value.get_result())
                else:
                    row.append(record[column_value])
            data.append(row)

        return data

    def __str__(self):
        column_names = [f"{column[0] if column[0] is not None else str(column[1])}={str(column[1])}" for column in self.column_references]
        return f"Projection(columns=[{', '.join(column_names)}])"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
