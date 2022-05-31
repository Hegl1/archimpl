import itertools

from .abstract_join_operator import *
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from ...table_service import TableIndexException


class MergeJoin(AbstractJoin):

    def get_result(self):
        left_table = self.table1_reference.get_result()
        right_table = self.table2_reference.get_result()

        # TODO: check if tables are properly sorted

        result_records = self._build_matching_records(left_table, right_table)

        return Table(self.schema, result_records)

    def _build_matching_records(self, left_table, right_table):
        left_table_referenced_column_indices = self._get_join_column_indices(left_table, self.condition)
        right_table_referenced_column_indices = self._get_join_column_indices(right_table, self.condition)
        records = []

        left_record_index = 0
        right_record_index = 0

        right_table_finished = False

        while left_record_index < len(left_table.records) and right_record_index < len(right_table.records):

            merge_condition = \
                self._compare_records(left_table.records[left_record_index], right_table.records[right_record_index],
                                      left_table_referenced_column_indices, right_table_referenced_column_indices)

            if merge_condition == 0:
                left_record_index, right_record_index = \
                    self._insert_matching_records(records, left_table, right_table, left_record_index,
                                                  right_record_index, left_table_referenced_column_indices,
                                                  right_table_referenced_column_indices)

            elif merge_condition == 1 or right_table_finished:
                if self.join_type == JoinType.LEFT_OUTER:
                    records.append(left_table.records[left_record_index] + self._build_null_record(
                        len(right_table.records[right_record_index])))

                left_record_index += 1

            else:
                if self.join_type == JoinType.LEFT_OUTER and right_record_index >= 5:
                    right_table_finished = True
                else:
                    right_record_index += 1

        return records

    def _insert_matching_records(self, records, left_table, right_table, left_record_index, right_record_index,
                                 left_table_referenced_column_indices, right_table_referenced_column_indices):

        cross_joined_records, left_next_record_index, right_next_record_index = \
            self._build_record(left_table, right_table, left_record_index, right_record_index,
                               left_table_referenced_column_indices, right_table_referenced_column_indices)

        for record in cross_joined_records:
            records.append(record)

        left_record_index = left_next_record_index

        if self.join_type == JoinType.LEFT_OUTER:
            right_record_index = min(len(right_table.records) - 1, right_next_record_index)
        else:
            right_record_index = right_next_record_index

        return left_record_index, right_record_index

    def _build_record(self, left_table, right_table, left_sub_record_start_index, right_sub_record_start_index,
                      left_table_referenced_column_indices, right_table_referenced_column_indices):

        left_reference = self._get_referenced_values(left_table, left_sub_record_start_index,
                                                     left_table_referenced_column_indices)
        right_reference = self._get_referenced_values(right_table, right_sub_record_start_index,
                                                      right_table_referenced_column_indices)

        left_record_end_index = left_sub_record_start_index + 1
        right_record_end_index = right_sub_record_start_index + 1

        for left_record_index in range(left_sub_record_start_index, len(left_table.records)):
            if self._get_referenced_values(left_table, left_record_index,
                                           left_table_referenced_column_indices) != left_reference:
                left_record_end_index = left_record_index
                break

        for right_record_index in range(right_sub_record_start_index, len(right_table.records)):
            if self._get_referenced_values(right_table, right_record_index,
                                           right_table_referenced_column_indices) != right_reference:
                right_record_end_index = right_record_index
                break

        left_records = left_table.records[left_sub_record_start_index:left_record_end_index]
        right_records = right_table.records[right_sub_record_start_index:right_record_end_index]

        cross_product_record = []

        for left_record, right_record in itertools.product(left_records, right_records):
            if not self.is_natural:
                cross_product_record.append(left_record + right_record)
            else:
                index_to_exclude = self._get_join_column_indices(right_table, self.condition)
                new_record = []
                for i, target in enumerate(right_record):
                    if i not in index_to_exclude:
                        new_record.append(target)
                cross_product_record.append(left_record + new_record)

        return cross_product_record, left_record_end_index, right_record_end_index

    def _get_referenced_values(self, table, record_index, table1_referenced_column_indices):
        return list(map(table.records[record_index].__getitem__, table1_referenced_column_indices))

    def _compare_records(self, tuple1, tuple2, table1_referenced_column_indices, table2_referenced_column_indices):
        match = 0

        for column_index1, column_index2 in zip(table1_referenced_column_indices, table2_referenced_column_indices):
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
