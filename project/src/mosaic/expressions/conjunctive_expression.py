from .abstract_computation_expression import AbstractComputationExpression
from ..table_service import Schema


class ConjunctiveExpression(AbstractComputationExpression):
    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self, table, row_index):
        if isinstance(self.value, list):
            return all(map(lambda comparative: comparative.get_result(table=table, row_index=row_index), self.value))
        elif isinstance(self.value, AbstractComputationExpression):
            return self.value.get_result(table=table, row_index=row_index)

        return self.value.get_result()

    def replace_all_column_names_by_fqn(self, schema: Schema):
        self.value.replace_all_column_names_by_fqn(schema)

    def __str__(self):
        if isinstance(self.value, list):
            return "(" + " and ".join([str(comparative) for comparative in self.value]) + ")"
        return f"{self.value}"
