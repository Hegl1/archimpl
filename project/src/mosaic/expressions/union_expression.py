from .abstract_expression import AbstractExpression
from mosaic.table_service import Table


class Union(AbstractExpression):
    def __init__(self, table1_reference, table2_reference):
        super().__init__()
        self.table1_reference = table1_reference
        self.table2_reference = table2_reference

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        table_union_records = table1.records + table2.records
        table_union_name = table1.table_name + table2.table_name

        return Table(table_union_name, table1.schema_names, table1.schema_types, table_union_records)

    def __str__(self):
        print(self.get_result())
