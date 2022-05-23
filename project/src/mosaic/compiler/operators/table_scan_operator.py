from copy import deepcopy
from mosaic import table_service
from .abstract_operator import AbstractOperator


class TableScan(AbstractOperator):
    """
    Class that represents a table scan.
    """

    def __init__(self, table_name, alias=None):
        super().__init__()

        self.table_name = table_name
        self.alias = alias

    def get_result(self):
        table = table_service.retrieve(self.table_name, makeCopy=self.alias is not None)

        if self.alias is not None:
            table.rename(self.alias)

        return table

    def get_schema(self):
        schema = deepcopy(table_service.retrieve(self.table_name, makeCopy=False).schema)
        if self.alias is not None:
            schema.rename(self.alias)
        return schema

    def __str__(self):
        if self.alias is None:
            return f"TableScan({self.table_name})"

        return f"TableScan(table_name={self.table_name}, alias={self.alias})"

    def explain(self, rows, indent):
        super().explain(rows, indent)
