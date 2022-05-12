from enum import Enum
from abc import abstractmethod
from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class JoinType(Enum):
    CROSS = "cross"
    INNER = "inner"
    LEFT_OUTER = "left_outer"
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
        return self._build_schema(schema1, schema2)

    def _build_schema(self, schema1, schema2):

        self.check_table_names(schema1, schema2)
        if self.is_natural and self.join_type != JoinType.CROSS:
            return self._get_natural_join_schema(schema1, schema2)
        else:
            self.check_condition(schema1, schema2, self.condition)
            joined_table_name = f"{schema1.table_name}_{self.join_type}_join_{schema2.table_name}"
            return Schema(joined_table_name, schema1.column_names + schema2.column_names,
                          schema1.column_types + schema2.column_types)

    def _get_natural_join_schema(self, schema1, schema2):
        joined_table_name = f"{schema1.table_name}_natural_{self.join_type}_join_{schema2.table_name}"
        schema2_col_names = []
        schema2_col_types = []
        for schema_name, schema_type in zip(schema2.column_names, schema2.column_types):
            if schema2.get_simple_column_name(schema_name) not in schema1.get_simple_column_name_list():
                schema2_col_names.append(schema_name)
                schema2_col_types.append(schema_type)
        return Schema(joined_table_name, schema1.column_names + schema2_col_names,
                      schema1.column_types + schema2_col_types)

    @abstractmethod
    def check_condition(self, schema1, schema2, condition):
        pass

    def check_table_names(self, schema1, schema2):
        if schema1.table_name == schema2.table_name:
            raise SelfJoinWithoutRenamingException(f"Table \"{schema1.table_name}\" can't be joined with itself "
                                                   f"without renaming one of the occurrences")

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)




class ConditionNotValidException(Exception):
    pass


class SelfJoinWithoutRenamingException(Exception):
    pass
