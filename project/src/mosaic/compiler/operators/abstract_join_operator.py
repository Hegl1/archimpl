from enum import Enum
from abc import abstractmethod
from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class JoinType(Enum):
    CROSS = "cross_join"
    # TODO add more join types as needed


class AbstractJoin(AbstractOperator):

    def __init__(self, table1_reference, table2_reference, join_type, condition, is_natural):
        super().__init__()

        self.table1_reference = table1_reference
        self.table2_reference = table2_reference
        self.join_type = join_type
        self.condition = condition
        self.is_natural = is_natural

    @abstractmethod
    def get_result(self):
        pass

    def get_schema(self):
        schema1 = self.table1_reference.get_schema()
        schema2 = self.table2_reference.get_schema()
        check_table_names(schema1, schema2)
        check_condition(schema1, schema2, self.condition)
        joined_table_name = f"{schema1.table_name}_cross_join_{schema2.table_name}"
        return Schema(joined_table_name, schema1.column_names + schema2.column_names,
                      schema1.column_types + schema2.column_types)

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


def check_table_names(schema1, schema2):
    if schema1.table_name == schema2.table_name:
        raise SelfJoinWithoutRenamingException(f"Table \"{schema1.table_name}\" can't be joined with itself "
                                               f"without renaming one of the occurrences")


def check_condition(schema1, schema2, condition):
    if condition is None:
        return
    # TODO implement together with other join types
    # if we don't compare something with columns from both different tables, raise ConditionNotValidException
    pass


class ConditionNotValidException(Exception):
    pass


class SelfJoinWithoutRenamingException(Exception):
    pass
