from .abstract_expression import AbstractExpression

class HashDistinct(AbstractExpression):
    def __init__(self, table):
        super().__init__()

        self.table = table

    def get_result(self):
        # TODO: implement
        return self.table.get_result()

    def __str__(self):
        return f"HashDistinct(table_name={self.table.table_name})"