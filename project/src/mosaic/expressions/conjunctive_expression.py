from .abstract_expression import AbstractExpression
from .comparative_operation_expression import ComparativeOperationExpression


class ConjunctiveExpression(AbstractExpression):
    def __init__(self, value):
        self.value = value

    def get_result(self, table, row_index):
        if isinstance(self.value, list):
            return all(map(lambda comparative: comparative.get_result(table=table, row_index=row_index), self.value))
            
        return self.value.get_result(table=table, row_index=row_index)

    def __str__(self):
        if isinstance(self.value, list):
            return "(" + " and ".join([str(comparative) for comparative in self.value]) + ")"
        return f"{self.value}"
