from .abstract_operator import AbstractOperator
from mosaic import table_service

class TableScan(AbstractOperator):
    def __init__(self, table_name, alias = None):
        super().__init__()

        self.table_name = table_name
        self.alias = alias

    def get_result(self):
        table = table_service.retrieve(self.table_name, makeCopy = True)

        if(self.alias is not None):
            table.table_name = self.alias

        return table

    def __str__(self):
        if(self.alias is None):
            return f"TableScan({self.table_name})"
        
        return f"TableScan(table_name={self.table_name}, alias={self.alias})"