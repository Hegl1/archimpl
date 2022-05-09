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

    def replace_all_column_names_by_fqn(self, schema: Schema):
        """
        Recursively replaces all occurrences of column names in the expression by the respective fully qualified
        column names based on the given schema of a table.
        If the leaf of an expression node in not a column name then nothing needs to be done.
        """
        pass

    def __str__(self):
        if isinstance(self.value, str):
            return f"\"{self.value}\""
        elif self.value is None:
            return "NULL"

        return str(self.value)
