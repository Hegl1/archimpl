from .abstract_expression import AbstractExpression


class LiteralExpression(AbstractExpression):
    def __init__(self, value):
        self.value = value

    def get_result(self):
        return self.value

    def __str__(self):
        if isinstance(self.value, str):
            return f"\"{self.value}\""

        return str(self.value)

    def explain(self, rows, indent): # pragma: no cover
        pass
