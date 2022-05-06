from .abstract_expression import AbstractExpression
from .arithmetic_operation_expression import ArithmeticOperationExpression
from .abstract_computation_expression import AbstractComputationExpression
from .literal_expression import LiteralExpression
from mosaic.table_service import Table, get_schema_type, Schema


class InvalidAliasException(Exception):
    pass


class Projection(AbstractExpression):
    def __init__(self, column_references, table_reference):
        super().__init__()

        self.column_references = column_references
        self.table_reference = table_reference

    def get_result(self):
        table: Table = self.table_reference.get_result()

        schema_names, schema_types, columns = self._build_schema(table.schema)
        data = self._build_data(table, columns)
        schema = Schema(table.get_table_name(), schema_names, schema_types)
        return Table(schema, data)

    def get_schema(self):
        old_schema = self.table_reference.get_schema()
        column_names, column_types, columns = self._build_schema(old_schema)
        return Schema(old_schema.table_name, column_names, column_types)

    def _build_schema(self, old_schema):
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
                schema_types.append(column_reference.get_schema_type(old_schema))
            elif isinstance(column_reference, LiteralExpression):
                column_value = column_reference

                schema_names.append(alias)
                schema_types.append(get_schema_type(column_reference.get_result()))
            else:
                column = column_reference.get_result()
                column_name = old_schema.get_fully_qualified_column_name(column)
                column_value = old_schema.get_column_index(column)

                schema_names.append(column_name if alias is None else alias)
                schema_types.append(old_schema.column_types[column_value])

            columns.append(column_value)

        return schema_names, schema_types, columns

    def _build_data(self, table, columns):
        """
        Builds the data (rows/records) for the projection-result.
        For this it uses the columns returned by the _build_schema method
        """
        data = []

        for (index, record) in enumerate(table.records):
            row = []
            for column_value in columns:
                if isinstance(column_value, AbstractComputationExpression):
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
