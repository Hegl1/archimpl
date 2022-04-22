from enum import Enum

from .abstract_expression import AbstractExpression
from mosaic.table_service import Table


class SetOperationType(Enum):
    UNION = 0,
    INTERSECT = 1,
    EXCEPT = 2


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

        schema_names_1, schema_names_2 = _get_simple_schema_names(table1, table2)
        _check_schemas(table1, table2, schema_names_1, schema_names_2)

        table_union_records = table1.records + table2.records
        table_union_name = f"{table1.table_name}_union_{table2.table_name}"

        return Table(table_union_name, _construct_schema_names(table_union_name, schema_names_1),
                     table1.schema_types, table_union_records)

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

        schema_names_1, schema_names_2 = _get_simple_schema_names(table1, table2)
        _check_schemas(table1, table2, schema_names_1, schema_names_2)

        table_intersect_records = [x for y in table1.records for x in table2.records if x == y]
        table_intersect_name = f"{table1.table_name}_intersect_{table2.table_name}"

        return Table(table_intersect_name, _construct_schema_names(table_intersect_name, schema_names_1),
                     table1.schema_types, table_intersect_records)

    def __str__(self):
        return "Intersect"

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


class Except(AbstractExpression):
    def __init__(self, table1_reference, table2_reference):
        super().__init__()
        self.table1_reference = table1_reference
        self.table2_reference = table2_reference

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        schema_names_1, schema_names_2 = _get_simple_schema_names(table1, table2)
        _check_schemas(table1, table2, schema_names_1, schema_names_2)

        table_except_records = []

        for record in table1.records:
            if record not in table2.records:
                table_except_records.append(record)

        for record in table2.records:
            if record not in table1.records:
                table_except_records.append(record)

        table_except_name = f"{table1.table_name}_except_{table2.table_name}"

        return Table(table_except_name, _construct_schema_names(table_except_name, schema_names_1),
                     table1.schema_types, table_except_records)

    def __str__(self):
        return "Except"

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


def _get_simple_schema_names(table1, table2):
    schema_names_1 = [table1.get_simple_column_name(name) for name in table1.schema_names]
    schema_names_2 = [table2.get_simple_column_name(name) for name in table2.schema_names]
    return schema_names_1, schema_names_2


def _check_schemas(table1, table2, schema_names_1, schema_names_2):
    if schema_names_1 != schema_names_2:
        raise TableSchemaDoesNotMatchException(
            f"Schemas of {table1.table_name} and {table2.table_name} do not match.")


def _construct_schema_names(table_name, simple_schema_names):
    return [f"{table_name}.{name}" for name in simple_schema_names]