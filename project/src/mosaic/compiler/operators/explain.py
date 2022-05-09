from abc import ABC

from mosaic.table_service import Table, SchemaType, Schema
from mosaic.compiler.operators.abstract_operator import AbstractOperator


class Explain(AbstractOperator, ABC):
    """
    Class that represents the explain operator.
    It is used to explain the execution plan of a query.
    """

    def __init__(self, execution_plan: AbstractOperator):
        super().__init__()
        self.execution_plan = execution_plan

    def get_schema(self):
        pass

    def get_result(self):
        rows = []
        self.explain(rows, 0)
        schema = Schema("Execution_plan", ["Operator"], [SchemaType.VARCHAR])
        return Table(schema, rows)

    def __str__(self):
        return "Explain"

    def explain(self, rows, indent):
        self.execution_plan.explain(rows, indent + 2)
