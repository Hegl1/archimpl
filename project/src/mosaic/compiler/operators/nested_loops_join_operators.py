from enum import Enum
from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class JoinType(Enum):
    CROSS = 0
    # TODO add more join types as needed


class NestedLoopsJoin(AbstractOperator):
    """
    Class that represents a join operation.
    It supports several kinds of joins, e.g. cross, outer, inner join etc.
    The resulting table can be retrieved with the get_result method.
    This class has the following properties:
    table1_reference and table2_reference: the two tables to be joined
    join_type: the join type
    condition: the condition for the join (optional)
    is_natural: boolean expressing if a join is natural
    """

    def __init__(self, table1_reference, table2_reference, join_type, condition, is_natural):
        super().__init__()

        self.table1_reference = table1_reference
        self.table2_reference = table2_reference
        self.join_type = join_type
        self.condition = condition
        self.is_natural = is_natural

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        _check_table_names(table1.schema, table2.schema)
        _check_condition(table1.schema, table2.schema, self.condition)

        if self.join_type == JoinType.CROSS:
            joined_table_records = []
            for record1 in table1.records:
                for record2 in table2.records:
                    joined_table_records.append(record1 + record2)
            joined_table_name = f"{table1.table_name}_cross_join_{table2.table_name}"
            schema = Schema(joined_table_name, table1.schema.column_names + table2.schema.column_names,
                            table1.schema.column_types + table2.schema.column_types)

            return Table(schema, joined_table_records)
        # TODO handle other join types correctly
        return None

    def get_schema(self):
        schema1 = self.table1_reference.get_schema()
        schema2 = self.table2_reference.get_schema()
        _check_table_names(schema1, schema2)
        _check_condition(schema1, schema2, self.condition)
        joined_table_name = f"{schema1.table_name}_cross_join_{schema2.table_name}"
        return Schema(joined_table_name, schema1.column_names + schema2.column_names,
                      schema1.column_types + schema2.column_types)

    def __str__(self):
        return f"NestedLoopsJoin(cross, natural={self.is_natural}, condition={self.condition})"
        # TODO does the condition already have to contain only fqn? yes?
        # TODO convert join type enum to string

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


def _check_table_names(schema1, schema2):
    if schema1.table_name == schema2.table_name:
        raise SelfJoinWithoutRenamingException(f"Table \"{schema1.table_name}\" can't be joined with itself "
                                               f"without renaming one of the occurrences")


def _check_condition(schema1, schema2, condition):
    if condition is None:
        return
    # TODO implement together with other join types
    # if we don't compare something with columns from both different tables, raise ConditionNotValidException
    pass


class ConditionNotValidException(Exception):
    pass


class SelfJoinWithoutRenamingException(Exception):
    pass
