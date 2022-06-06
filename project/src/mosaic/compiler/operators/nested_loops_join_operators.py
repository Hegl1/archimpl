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

    def _get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        remaining_column_indices = []
        if self.is_natural:
            remaining_column_indices = self.get_remaining_column_indices(self.table2_schema)

        joined_table_records = []
        aux_schema = Schema(f"{self.table1_schema.table_name}_join_{self.table2_schema.table_name}", self.table1_schema.column_names + self.table2_schema.column_names,
                            self.table1_schema.column_types + self.table2_schema.column_types)
        aux_table = Table(aux_schema, [])

        for record1 in table1.records:
            found_match = False
            for record2 in table2.records:
                if self.is_natural and self.join_type is not JoinType.CROSS:
                    record2_reduced = [record2[i] for i in remaining_column_indices]
                    new_record = record1 + record2_reduced
                    aux_table.records = [record1 + record2]
                else:
                    new_record = record1 + record2
                    aux_table.records = [new_record]

                if self.join_type == JoinType.CROSS or self.condition.get_result(aux_table, 0):
                    found_match = True
                    joined_table_records.append(new_record)

            if self.join_type == JoinType.LEFT_OUTER and not found_match:
                if self.is_natural:
                    joined_table_records.append(record1 + self._build_null_record(len(remaining_column_indices)))
                else:
                    joined_table_records.append(record1 + self._build_null_record(len(table2.schema.column_names)))
        return Table(self.schema, joined_table_records)

    def get_remaining_column_indices(self, schema2):
        """
        Returns a list of values corresponding to the indices of the columns than remain in the schema after the join.
        Used for creation of tuples in a natural join.
        """
        remaining_indices = []
        for i, column_name in enumerate(schema2.column_names):
            if column_name in self.schema.column_names:
                remaining_indices.append(i)
        return remaining_indices

    def __str__(self):
        schema = self.get_schema()
        if self.condition is not None:
            self.condition.replace_all_column_names_by_fqn(schema)
        return f"NestedLoopsJoin({self.join_type.value}, natural={self.is_natural}, condition={self.condition.__str__()})"

    def check_condition(self, schema1, schema2, condition):
        pass

    def check_join_type(self):
        pass
