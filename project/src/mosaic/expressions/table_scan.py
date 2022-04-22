from .abstract_expression import AbstractExpression
from mosaic import table_service

class TableScan(AbstractExpression):
    def __init__(self, table_name, alias = None):
        super().__init__()

        self.table_name = table_name
        self.alias = alias

    def get_result(self):
        table = table_service.retrieve(self.table_name, makeCopy = True)

        if(self.alias is not None):
            table.rename(self.alias)

        return table

    def __str__(self):
        if(self.alias is None):
            return f"TableScan({self.table_name})"
        
        return f"TableScan(table_name={self.table_name}, alias={self.alias})"

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
