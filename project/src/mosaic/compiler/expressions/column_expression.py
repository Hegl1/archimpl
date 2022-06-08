from mosaic.table_service import Schema
from .abstract_expression import AbstractExpression


class ColumnExpression(AbstractExpression):
    """
    Class that represents a column name.
    """

    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self):
        return self.value

    def get_string_representation(self, schema: Schema = None):
        if schema is None:
            return self.value
        else:
            return schema.get_fully_qualified_column_name(self.value)
