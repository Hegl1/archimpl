from copy import copy
from .abstract_operator import AbstractOperator
from mosaic import table_service

class TableScan(AbstractOperator):
    def __init__(self, table_name, alias = None):
        super().__init__()

        self.table_name = table_name
        self.alias = alias

    def get_result(self):
        table = table_service.retrieve(self.table_name)

        if(self.alias is not None):
            table = copy(table)

            table.rename(self.alias)

        return table

    def __str__(self):
        if(self.alias is None):
            return f"TableScan({self.table_name})"
        
        return f"TableScan(table_name={self.table_name}, alias={self.alias})"