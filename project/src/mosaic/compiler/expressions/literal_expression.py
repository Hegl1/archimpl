from mosaic.table_service import Schema
from .abstract_expression import AbstractExpression


class LiteralExpression(AbstractExpression):
    """
    Class that represents a literal.
    It is used to represent e.g. int, float, varchar or NULL values/literals.
    """

    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self):
        return self.value

    def get_string_representation(self, schema: Schema = None):
        if isinstance(self.value, str):
            return f"\"{self.value}\""
        elif self.value is None:
            return "NULL"

        return str(self.value)
