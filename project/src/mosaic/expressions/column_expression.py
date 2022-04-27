from .abstract_expression import AbstractExpression


class ColumnExpression(AbstractExpression):
    def __init__(self, value):
        self.value = value

    def get_result(self):
        return self.value

    def __str__(self):
        return self.value

    def explain(self, rows, indent): # pragma: no cover
        pass
