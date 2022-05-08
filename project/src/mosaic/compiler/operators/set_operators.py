from abc import ABC
from enum import Enum
from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class SetOperationType(Enum):
    UNION = 0,
    INTERSECT = 1,
    EXCEPT = 2


class TableSchemaDoesNotMatchException(Exception):
    pass


class AbstractSetOperator(AbstractOperator, ABC):

    def __init__(self, table1_reference, table2_reference):
        super().__init__()
        self.table1_reference = table1_reference
        self.table2_reference = table2_reference

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


class Union(AbstractSetOperator):
    """
    Represents a union operation.
    It returns a table containing all records from two given tables without removing duplicates.
    result can be retrieved with get_result method
    an explanation of the operation is created in the explain method
    """

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        _check_schemas(table1.schema, table2.schema)

        table_union_records = table1.records + table2.records
        table_union_name = f"{table1.table_name}_union_{table2.table_name}"
        schema = Schema(table_union_name, table1.schema.column_names, table1.schema.column_types)

        return Table(schema, table_union_records)

    def get_schema(self):
        schema1 = self.table1_reference.get_schema()
        schema2 = self.table2_reference.get_schema()
        _check_schemas(schema1, schema2)
        table_union_name = f"{schema1.table_name}_union_{schema2.table_name}"
        return Schema(table_union_name, schema1.column_names, schema1.column_types)

    def __str__(self):
        return "Union"


class Intersect(AbstractSetOperator):
    """
    Represents an intersect operation.
    It returns a table which only contains records that appear in both given tables.
    result can be retrieved with get_result method
    an explanation of the operation is created in the explain method
    """

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        _check_schemas(table1.schema, table2.schema)

        table_intersect_records = [x for y in table1.records for x in table2.records if x == y]
        table_intersect_name = f"{table1.table_name}_intersect_{table2.table_name}"
        schema = Schema(table_intersect_name, table1.schema.column_names, table1.schema.column_types)

        return Table(schema, table_intersect_records)

    def get_schema(self):
        schema1 = self.table1_reference.get_schema()
        schema2 = self.table2_reference.get_schema()
        _check_schemas(schema1, schema2)
        table_intersect_name = f"{schema1.table_name}_intersect_{schema2.table_name}"
        return Schema(table_intersect_name, schema1.column_names, schema1.column_types)

    def __str__(self):
        return "Intersect"


class Except(AbstractSetOperator):
    """
    Represents a difference operation.
    It returns a table which only contains records that appear in the first table but not in the second table.
    result can be retrieved with get_result method
    an explanation of the operation is created in the explain method
    """

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        _check_schemas(table1.schema, table2.schema)

        table_except_records = []

        for record in table1.records:
            if record not in table2.records:
                table_except_records.append(record)

        table_except_name = f"{table1.table_name}_except_{table2.table_name}"
        schema = Schema(table_except_name, table1.schema.column_names, table1.schema.column_types)

        return Table(schema, table_except_records)

    def get_schema(self):
        schema1 = self.table1_reference.get_schema()
        schema2 = self.table2_reference.get_schema()
        _check_schemas(schema1, schema2)
        table_except_name = f"{schema1.table_name}_except_{schema2.table_name}"
        return Schema(table_except_name, schema1.column_names, schema1.column_types)

    def __str__(self):
        return "Except"


def _get_simple_column_names(schema1, schema2):
    """
    constructs simple column names out of fully qualified column names
    """
    column_names_1 = [schema1.get_simple_column_name(name) for name in schema1.column_names]
    column_names_2 = [schema2.get_simple_column_name(name) for name in schema2.column_names]
    return column_names_1, column_names_2


def _check_schemas(schema1, schema2):
    """
    checks if the schemas (with simple column names) of two tables do match in order to apply a set operation
    """
    column_names_1, column_names_2 = _get_simple_column_names(schema1, schema2)

    if column_names_1 != column_names_2:
        raise TableSchemaDoesNotMatchException(
            f"Schemas of {schema1.table_name} and {schema2.table_name} do not match.")
