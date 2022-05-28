import itertools
from operator import itemgetter

from .abstract_join_operator import *
from .nested_loops_join_operators import NestedLoopsJoin
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from ...table_service import TableIndexException


class MergeJoin(AbstractJoin):

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        # TODO: check if tables are sorted

        records = self._build_matching_records(table1, table2)

        return Table(self.schema, records)

    def _build_matching_records(self, table1, table2):
        table1_referenced_column_indices = self._get_join_column_indices(table1, self.condition)
        table2_referenced_column_indices = self._get_join_column_indices(table2, self.condition)
        records = []

        record_index1 = 0
        record_index2 = 0

        while record_index1 < len(table1.records) and record_index2 < len(table2.records):

            merge_condition = self._compare_records(table1.records[record_index1], table2.records[record_index2],
                                                    table1_referenced_column_indices, table2_referenced_column_indices)

            if merge_condition == 0:

                cross_joined_records, next_record_index1, next_record_index2 = \
                    self._cross_join_duplicates(table1, table2, record_index1, record_index2,
                                                table1_referenced_column_indices, table2_referenced_column_indices)

                for record in cross_joined_records:
                    records.append(record)

                record_index1 = next_record_index1
                record_index2 = next_record_index2

            elif merge_condition == 1:
                record_index1 += 1

            else:
                record_index2 += 1

        return records

    def _cross_join_duplicates(self, table1, table2, sub_record_start_index1, sub_record_start_index2,
                               table1_referenced_column_indices, table2_referenced_column_indices):

        reference1 = self._get_referenced_values(table1, sub_record_start_index1, table1_referenced_column_indices)
        reference2 = self._get_referenced_values(table2, sub_record_start_index2, table2_referenced_column_indices)

        record_end_index1 = sub_record_start_index1 + 1
        record_end_index2 = sub_record_start_index2 + 1

        for record_index1 in range(sub_record_start_index1, len(table1.records)):
            if self._get_referenced_values(table1, record_index1, table1_referenced_column_indices) != reference1:
                record_end_index1 = record_index1
                break

        for record_index2 in range(sub_record_start_index2, len(table2.records)):
            if self._get_referenced_values(table2, record_index2, table2_referenced_column_indices) != reference2:
                record_end_index2 = record_index2
                break

        records1 = table1.records[sub_record_start_index1:record_end_index1]
        records2 = table2.records[sub_record_start_index2:record_end_index2]

        cross_product_record = []
        for element in itertools.product(records1, records2):
            cross_product_record.append(element[0] + element[1])

        return cross_product_record, record_end_index1, record_end_index2

    def _get_referenced_values(self, table, record_index, table1_referenced_column_indices):
        return list(map(table.records[record_index].__getitem__, table1_referenced_column_indices))

    def _compare_records(self, tuple1, tuple2, table1_referenced_column_indices, table2_referenced_column_indices):
        match = 0

        for column_index1, column_index2 in zip(table1_referenced_column_indices, table2_referenced_column_indices):#
            if tuple1[column_index1] == tuple2[column_index2]:
                continue
            elif tuple1[column_index1] < tuple2[column_index2]:
                match = 1
            else:
                match = -1

        return match

    def check_condition(self, schema1, schema2, condition):
        if isinstance(condition, ConjunctiveExpression):
            for comparative in condition.conditions:
                self._check_comparative_condition_supported(comparative)
                self._check_comparative_condition_invalid_references(schema1, schema2, comparative)
        else:
            self._check_comparative_condition_supported(condition)
            self._check_comparative_condition_invalid_references(schema1, schema2, condition)

    def check_join_type(self):
        if self.join_type == JoinType.CROSS:
            raise JoinTypeNotSupportedException("Cross joins are not supported by MergeJoin")

    def _check_comparative_condition_supported(self, condition):
        """
        Method that checks whether the condition is a simple comparative
        (i.e. a comparative that checks the equality between columns)
        """
        if not isinstance(condition, ComparativeOperationExpression) or \
                not isinstance(condition.right, ColumnExpression) or \
                not isinstance(condition.left, ColumnExpression) or \
                condition.operator != ComparativeOperator.EQUAL:
            raise JoinConditionNotSupportedException("MergeJoin only supports conjunctions of equalities or "
                                                     "simple equalities that only contain column references")

    def _check_comparative_condition_invalid_references(self, schema1, schema2, comparative):
        """
        Checks whether the equalities in comparative always contain exactly one column reference of one table
        and column names not being ambiguous.
        Throws an exception if the condition is invalid.
        e.g:    hoeren join hoeren.MatrNr = pruefen.MatrNr pruefen -> is valid
                hoeren MatrNr = pruefen.MatrNr pruefen -> invalid
        """
        self._get_join_column_index_from_comparative(schema1, comparative)
        self._get_join_column_index_from_comparative(schema2, comparative)

    def _get_join_column_indices(self, relation, condition):
        """
        Returns a list of values corresponding to the equivalences used in the condition.
        i.e. hoeren.MatrNr = pruefen.MatrNr and hoeren.VorlNr = pruefen.VorlNr
            -> (0, 1) (indices for the left relation.)
        """
        columns = []
        if isinstance(condition, ComparativeOperationExpression):
            columns.append(self._get_join_column_index_from_comparative(relation.schema, condition))
        else:
            for comparative_condition in condition.conditions:
                columns.append(self._get_join_column_index_from_comparative(relation.schema, comparative_condition))
        return columns

    def _get_join_column_index_from_comparative(self, schema, comparative):
        """
        Method that checks whether the schema reference is left or right in the comparative
        and returns the column index of that reference for the schema.
        If no reference is found or the schema gets referenced more than once in the comparative,
        an exception is thrown.
        """
        left = None
        right = None
        try:
            left = schema.get_column_index(comparative.left.get_result())
        except TableIndexException:
            pass
        try:
            right = schema.get_column_index(comparative.right.get_result())
        except TableIndexException:
            pass

        if left is not None and right is not None:
            raise ErrorInJoinConditionException("Column of one table found in both sides of join condition")
        elif left is not None:
            return left
        elif right is not None:
            return right
        else:
            raise ErrorInJoinConditionException("No table reference found in join condition")

    def __str__(self):
        return f"MergeJoin(natural={self.is_natural}, " \
               f"condition={self.condition}, " \
               f"left={self.join_type == JoinType.LEFT_OUTER}) "
