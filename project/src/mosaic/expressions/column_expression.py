from .abstract_expression import AbstractExpression


class ColumnExpression(AbstractExpression):
    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self):
        return self.value

    def __str__(self):
        return self.value
