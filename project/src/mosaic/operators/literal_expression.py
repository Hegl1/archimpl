from mosaic.operators.abstract_operator import AbstractOperator


class LiteralExpression(AbstractOperator):
    def __init__(self, value):
        self.value = value

    def get_result(self):
        return self.value

    def __str__(self):
        return f"LiteralExpression(value={self.value})"
