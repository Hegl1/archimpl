from .abstract_expression import AbstractExpression
from mosaic.table_service import Schema


class ColumnExpression(AbstractExpression):
    """
    Class that represents a column name.
    """
    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self):
        return self.value

    def replace_all_column_names_by_fqn(self, schema: Schema):
        """
        Replaces the column name in the expression by the respective fully qualified
        column name based on the given schema of a table.
        """
        self.value = schema.get_fully_qualified_column_name(self.value)

    def __str__(self):
        return self.value
