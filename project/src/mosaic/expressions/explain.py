from .abstract_expression import AbstractExpression
from mosaic.table_service import Table, SchemaType


class Explain(AbstractExpression):

    def __init__(self, execution_plan: AbstractExpression):
        super().__init__()
        self.execution_plan = execution_plan

    def get_result(self):
        rows = []
        self.explain(rows, 0)
        return Table("Execution_plan", ["Operator"], [SchemaType.VARCHAR], rows)

    def __str__(self): # pragma: no cover
        return "Explain"

    def explain(self, rows, indent):
        self.execution_plan.explain(rows, indent + 2)