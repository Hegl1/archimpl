from .abstract_computation_expression import AbstractComputationExpression


class DisjunctiveExpression(AbstractComputationExpression):
    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self, table, row_index):
        if isinstance(self.value, list):
            return any(map(lambda comparative: comparative.get_result(table=table, row_index=row_index), self.value))
        elif isinstance(self.value, AbstractComputationExpression):
            return self.value.get_result(table=table, row_index=row_index)

        return self.value.get_result()

    def __str__(self):
        if isinstance(self.value, list):
            return "(" + " or ".join([str(comparative) for comparative in self.value]) + ")"
        return f"{self.value}"
