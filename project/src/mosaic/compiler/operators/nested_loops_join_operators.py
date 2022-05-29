from mosaic.table_service import Table, Schema
from .abstract_join_operator import *


class NestedLoopsJoin(AbstractJoin):
    """
    Class that represents a nested loops join operation.
    It supports several kinds of joins, e.g. cross, outer, inner join etc.
    The resulting table can be retrieved with the get_result method.
    This class has the following properties:
    table1_reference and table2_reference: the two tables to be joined
    join_type: the join type
    condition: the condition for the join (optional)
    is_natural: boolean expressing if a join is natural
    """

    def __init__(self, table1_reference, table2_reference, join_type, condition, is_natural):
        super().__init__(table1_reference, table2_reference, join_type, condition, is_natural)

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        if self.is_natural:
            remaining_column_indices = self.get_remaining_column_indices(table2.schema)

        joined_table_records = []
        for record1 in table1.records:
            found_match = False
            for record2 in table2.records:
                if self.join_type == JoinType.CROSS:  # TODO or condition applies
                    found_match = True
                    if self.is_natural:
                        record2_reduced = [record2[i] for i in remaining_column_indices]
                        joined_table_records.append(record1 + record2_reduced)
                    else:
                        joined_table_records.append(record1 + record2)
            if self.join_type == JoinType.LEFT_OUTER and not found_match:
                if self.is_natural:
                    joined_table_records.append(record1 + self._build_null_record(len(remaining_column_indices)))
                else:
                    joined_table_records.append(record1 + self._build_null_record(len(table2.schema.column_names)))

        return Table(self.schema, joined_table_records)

    def get_remaining_column_indices(self, schema2):
        remaining_indices = []
        for i, column_name in enumerate(schema2.column_names):
            if column_name in self.schema.column_names:
                remaining_indices.append(i)
        return remaining_indices

    def __str__(self):
        return f"NestedLoopsJoin({self.join_type.value}, natural={self.is_natural}, condition={self.condition})"

    def check_condition(self, schema1, schema2, condition):
        if condition is None:
            return
        # TODO what exactly needs to be checked?
        # if we don't compare something with columns from both different tables, raise ConditionNotValidException
        pass

    def check_join_type(self):
        pass
