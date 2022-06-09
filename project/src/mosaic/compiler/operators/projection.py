from mosaic.compiler.get_string_representation import get_string_representation
from mosaic.compiler.expressions.abstract_computation_expression import AbstractComputationExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.alias_schema_builder import build_schema
from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator

class Projection(AbstractOperator):
    """
    Class that represents a projection operation.
    It returns a table which only contains columns which match the given attributes list.
    """

    def __init__(self, node, column_references):
        super().__init__()

        self.node = node
        self.column_references = column_references

    def get_result(self):
        table: Table = self.node.get_result()

        schema_names, schema_types, columns = self._build_schema(table.schema)
        data = self._build_data(table, columns)
        schema = Schema(table.table_name, schema_names, schema_types)
        return Table(schema, data)

    def get_schema(self):
        old_schema = self.node.get_schema()
        column_names, column_types, columns = self._build_schema(old_schema)
        return Schema(old_schema.table_name, column_names, column_types)

    def _build_schema(self, old_schema):
        return build_schema(self.column_references,old_schema)
        

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

    def simplify(self):
        self.column_references = [(alias, column_ref.simplify()) for alias, column_ref in self.column_references]
        self.node = self.node.simplify()

        return self

    def __str__(self):
        new_schema = self.get_schema()
        old_schema = self.node.get_schema()
        column_name_strings = []
        for i, (_, column_ref) in enumerate(self.column_references):
            column_name_strings.append(f"{new_schema.column_names[i]}={get_string_representation(column_ref, old_schema)}")

        return f"Projection(columns=[{', '.join(column_name_strings)}])"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.node.explain(rows, indent + 2)
