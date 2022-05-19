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

        self.check_table_names(table1.schema, table2.schema)
        self.check_condition(table1.schema, table2.schema, self.condition)

        if self.join_type == JoinType.CROSS:
            joined_table_records = []
            for record1 in table1.records:
                for record2 in table2.records:
                    joined_table_records.append(record1 + record2)
            schema = self._build_schema(table1.schema, table2.schema)

            return Table(schema, joined_table_records)
        # TODO handle other join types correctly
        return None

    def __str__(self):
        return f"NestedLoopsJoin(cross, natural={self.is_natural}, condition={self.condition})"
        # TODO does the condition already have to contain only fqn? yes?

    def check_condition(self, schema1, schema2, condition):
        if condition is None:
            return
        # TODO implement together with other join types
        # if we don't compare something with columns from both different tables, raise ConditionNotValidException
        pass
