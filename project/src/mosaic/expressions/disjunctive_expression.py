from .abstract_expression import AbstractExpression
from .comparative_operation_expression import ComparativeOperationExpression
from .conjunctive_expression import ConjunctiveExpression


class DisjunctiveExpression(AbstractExpression):
    def __init__(self, value):
        self.value = value

    def get_result(self, table, row_index):
        if isinstance(self.value, list):
            return any(map(lambda comparative: comparative.get_result(table=table, row_index=row_index), self.value))
        elif isinstance(self.value, ComparativeOperationExpression) or isinstance(self.value, ConjunctiveExpression):
            return self.value.get_result(table=table, row_index=row_index)

        return self.value.get_result()

    def __str__(self):
        return f"DisjunctiveExpression(value={self.value})"

    def explain(self, rows, indent):
        pass
