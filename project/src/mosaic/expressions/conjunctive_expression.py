from .abstract_computation_expression import AbstractComputationExpression
from ..table_service import Schema


class ConjunctiveExpression(AbstractComputationExpression):
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
                return False

        return True

    def replace_all_column_names_by_fqn(self, schema: Schema):
        for v in self.value:
            v.replace_all_column_names_by_fqn(schema)

    def __str__(self):
        return "(" + " and ".join([str(comparative) for comparative in self.value]) + ")"
