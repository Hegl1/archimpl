from enum import Enum

from .abstract_expression import AbstractExpression
from mosaic.table_service import Table


class SetOperationType(Enum):
    UNION=0,
    INTERSECT=1,
    EXCEPT=2


class TableSchemaDoesNotMatchException(Exception):
    pass


class Union(AbstractExpression):
    def __init__(self, table1_reference, table2_reference):
        super().__init__()
        self.table1_reference = table1_reference
        self.table2_reference = table2_reference

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        if table1.schema_names != table2.schema_names:
            raise TableSchemaDoesNotMatchException(f"Schemas of {table1.table_name} and {table2.table_name} do not match.")

        table_union_records = table1.records + table2.records
        table_union_name = table1.table_name + table2.table_name

        return Table(table_union_name, table1.schema_names, table1.schema_types, table_union_records)

    def __str__(self):
        return "Union"

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


class Intersect(AbstractExpression):
    def __init__(self, table1_reference, table2_reference):
        super().__init__()
        self.table1_reference = table1_reference
        self.table2_reference = table2_reference

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        if table1.schema_names != table2.schema_names:
            raise TableSchemaDoesNotMatchException(
                f"Schemas of {table1.table_name} and {table2.table_name} do not match.")

        table_intersect_records = [x for y in table1.records for x in table2.records if x == y]
        table_intersect_name = table1.table_name + table2.table_name

        return Table(table_intersect_name, table1.schema_names, table1.schema_types, table_intersect_records)

    def __str__(self):
        return "Intersect"

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)
