from .abstract_expression import AbstractExpression


class LiteralExpression(AbstractExpression):
    def __init__(self, value):
        super().__init__()

        self.value = value

    def get_result(self):
        return self.value

    def __str__(self):
        if isinstance(self.value, str):
            return f"\"{self.value}\""
        elif self.value is None:
            return "NULL"

        return str(self.value)
