from enum import Enum

from mosaic.table_service import Table
from .abstract_expression import AbstractExpression


class JoinType(Enum):
    CROSS = 0
    # TODO add more join types as needed


class NestedLoopsJoin(AbstractExpression):
    """
    Represents a join operation
    result can be retrieved with get_result method
    an explanation of the operation is created in the explain method
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
        if self.join_type == JoinType.CROSS:
            joined_table_records = []
            for record1 in table1.records:
                for record2 in table2.records:
                    joined_table_records.append(record1 + record2)
            joined_table_name = f"{table1.table_name}_cross_join_{table2.table_name}"

            return Table(joined_table_name, table1.schema_names + table2.schema_names,
                         table1.schema_types + table2.schema_types, joined_table_records)
        # TODO handle other join types correctly
        return None

    def __str__(self):
        return f"NestedLoopsJoin(cross, natural={self.is_natural}, condition={self.condition})"
        # TODO convert join type enum to string

    def explain(self, rows, indent):
        rows.append([indent * "-" + ">" + self.__str__()])
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)

