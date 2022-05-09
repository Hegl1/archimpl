from mosaic.table_service import Schema
from .abstract_computation_expression import AbstractComputationExpression


class ConjunctiveExpression(AbstractComputationExpression):
    """
    Class that represents a conjunction.
    """

    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self, table, row_index):
        for comparative in self.value:
            if isinstance(comparative, AbstractComputationExpression):
                result = comparative.get_result(table, row_index)
            else:
                result = comparative.get_result()

            if not result:
                return 0

        return 1

    def replace_all_column_names_by_fqn(self, schema: Schema):
        """
        Recursively replaces all occurrences of column names in the expression by the respective fully qualified
        column names based on the given schema of a table.
        """
        for v in self.value:
            v.replace_all_column_names_by_fqn(schema)

    def __str__(self):
        return "(" + " and ".join([str(comparative) for comparative in self.value]) + ")"
