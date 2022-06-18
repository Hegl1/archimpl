import itertools
from copy import deepcopy

from mosaic.compiler.get_string_representation import get_string_representation
from .abstract_join import *
from .ordering import Ordering
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_expression import ComparativeExpression, ComparativeOperator


class MergeJoin(AbstractJoin):

    def __init__(self, left_node, right_node, join_type, condition, is_natural):
        super().__init__(left_node, right_node, join_type, condition, is_natural)
        self._check_tables_sorting()
        self.right_table_finished = False
        self.left_table_referenced_column_indices = self._get_join_column_indices(self.left_schema, self.condition)
        self.right_table_referenced_column_indices = self._get_join_column_indices(self.right_schema, self.condition)

    def _get_result(self):
        left_table = self.left_node.get_result()
        right_table = self.right_node.get_result()
        result_records = self._build_records(left_table, right_table)

        return Table(self.schema, result_records)

    def _build_records(self, left_table, right_table):
        """
        Builds the result records.
        Checks for each record in the two tables if they are matching in merge_records method.
        """
        records = []
        left_record_index = 0
        right_record_index = 0

        while left_record_index < len(left_table) and right_record_index < len(right_table):
            left_record_index, right_record_index = self._merge_records(left_table, right_table, left_record_index,
                                                                        right_record_index, records)

        return records

    def _merge_records(self, left_table, right_table, left_record_index, right_record_index, records):
        """
        Checks if two records of tables are matching.
        In dependence of the merge_condition the two records are getting merged or not.
        If not, the index of the left or the right table gets incremented
        to check the next records in the next iteration.
        """
        merge_condition = self._compare_records(left_table[left_record_index], right_table[right_record_index])

        if merge_condition == "matching":
            left_record_index, right_record_index = self._build_matching_records(records, left_table, right_table,
                                                                                 left_record_index, right_record_index)

        elif merge_condition == "right is bigger" or self.right_table_finished:
            if self.join_type == JoinType.LEFT_OUTER:
                records.append(left_table[left_record_index] +
                               self._build_null_record(len(right_table[right_record_index])))

            left_record_index += 1

        else:
            if self.join_type == JoinType.LEFT_OUTER and right_record_index >= len(right_table) - 1:
                self.right_table_finished = True
            else:
                right_record_index += 1

        return left_record_index, right_record_index

    def _build_matching_records(self, records, left_table, right_table, left_record_index, right_record_index):
        """
        Builds the record for matching records.
        Returns the indices to check them next for matches.
        """
        cross_product_of_records, left_next_record_index, right_next_record_index = \
            self._build_cross_product_of_records(left_table, right_table, left_record_index, right_record_index)

        for record in cross_product_of_records:
            records.append(record)

        left_record_index = left_next_record_index

        if self.join_type == JoinType.LEFT_OUTER:
            right_record_index = min(len(right_table) - 1, right_next_record_index)
        else:
            right_record_index = right_next_record_index

        return left_record_index, right_record_index

    def _build_cross_product_of_records(self, left_table, right_table, left_sub_record_start_index,
                                        right_sub_record_start_index):
        """
        Returns the cross product of matching records.
        Retrieves matching records from left and right table
        to finally build the cross product in build_cross_product_of_records_aux method.
        """
        left_records, left_record_end_index = \
            self._get_matching_records(left_table, self.left_table_referenced_column_indices,
                                       left_sub_record_start_index, right_table)

        right_records, right_record_end_index = \
            self._get_matching_records(right_table, self.right_table_referenced_column_indices,
                                       right_sub_record_start_index, right_table)

        cross_product_record = self._build_cross_product_of_records_aux(left_records, right_records)

        return cross_product_record, left_record_end_index, right_record_end_index

    def _build_cross_product_of_records_aux(self, left_records, right_records):
        """
        Builds the cross product of the matching records.
        In case of a natural join, the right table is needed to eliminate join columns.
        """
        cross_product_record = []

        for left_record, right_record in itertools.product(left_records, right_records):
            if not self.is_natural:
                cross_product_record.append(left_record + right_record)
            else:
                index_to_exclude = self._get_join_column_indices(self.right_schema, self.condition)
                new_record = []

                for i, target in enumerate(right_record):
                    if i not in index_to_exclude:
                        new_record.append(target)

                cross_product_record.append(left_record + new_record)

        return cross_product_record

    def _get_matching_records(self, table, table_referenced_column_indices, sub_record_start_index, right_table):
        """
        Returns a list of matching records to be merged with records from the other table.
        The list consists of the records from sub_record_start_index to sub_record_end_index.
        Checks for each record if it is still matching the reference
        """
        reference = self._get_referenced_values(table, sub_record_start_index, table_referenced_column_indices)
        sub_record_end_index = sub_record_start_index + 1

        for record_index in range(sub_record_start_index, len(right_table)):
            if not self._records_are_matching(table, record_index, table_referenced_column_indices, reference):
                sub_record_end_index = record_index
                break

        return table[sub_record_start_index:sub_record_end_index], sub_record_end_index

    def _records_are_matching(self, table, record_index, table_referenced_column_indices, reference):
        """
        Checks if referenced column values of records are still matching.
        """
        return reference == self._get_referenced_values(table, record_index, table_referenced_column_indices)

    def _get_referenced_values(self, table, record_index, table_referenced_column_indices):
        """
        Returns for a record of a table only the values from referenced columns.
        """
        return list(map(table[record_index].__getitem__, table_referenced_column_indices))

    def _compare_records(self, tuple1, tuple2):
        """
        Two records get compared to know if the records can be merged or not.
        Returns 0 if records are matching, 1 if left record is smaller, -1 if right record is smaller.
        """
        match = "matching"

        for column_index1, column_index2 in zip(self.left_table_referenced_column_indices,
                                                self.right_table_referenced_column_indices):
            if tuple1[column_index1] == tuple2[column_index2]:
                continue
            elif tuple1[column_index1] < tuple2[column_index2]:
                match = "right is bigger"
            else:
                match = "left is bigger"

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
        if not isinstance(condition, ComparativeExpression) or \
                not isinstance(condition.right, ColumnExpression) or \
                not isinstance(condition.left, ColumnExpression) or \
                condition.operator != ComparativeOperator.EQUAL:
            raise JoinConditionNotSupportedException("MergeJoin only supports conjunctions of equalities or "
                                                     "simple equalities that only contain column references")

    def _check_tables_sorting(self):
        """
        Checks if the two tables are sorted correctly for the given join conditions.
        For all join conditions the referenced columns from the condition have to be sorted.
        If not an exception gets raised.
        """
        if not isinstance(self.left_node, Ordering) or not isinstance(self.right_node, Ordering):
            raise TableNotSortedException("Tables are not sorted!")

        if len(self.left_node.column_list) != len(self.right_node.column_list):
            raise TableNotSortedException("Tables are not sorted the same!")

        condition_list = []
        if isinstance(self.condition, ConjunctiveExpression):
            condition_list = deepcopy(self.condition.conditions)

        self._check_columns_sorting(condition_list)

    def _check_columns_sorting(self, condition_list):
        """
        Checks if the columns are sorted the way they are supposed to be according to the join condition.
        e.g. the join condition is "voraussetzen.Nachfolger = vorlesungen.VorlNr" these columns have to be sorted
        in the corresponding table with the same priority, so they both have to be the first column after which the
        table is sorted or both have to be the second column after which the table is sorted.
        """
        for left, right in zip(self.left_node.column_list, self.right_node.column_list):

            if isinstance(self.condition, ComparativeExpression):
                self._check_columns_sorting_comparative_condition(left, right)
            else:
                self._check_columns_sorting_conjunctive_condition(left, right, condition_list)

    def _check_columns_sorting_comparative_condition(self, left, right):
        """
        Checks if the columns are sorted the way they are supposed to be according to the join condition,
        in case the join condition is a simple comparative condition.
        """
        if not self._check_columns_in_condition(left, right, self.condition):
            raise TableNotSortedException("Tables are not sorted the same!")

    def _check_columns_sorting_conjunctive_condition(self, left, right, condition_list):
        """
        Checks if the columns are sorted the way they are supposed to be according to the join condition,
        in case the join condition is a simple conjunctive condition.
        """
        condition_found = False

        for condition in condition_list:
            condition_found = self._check_columns_in_condition(left, right, condition)
            if condition_found:
                condition_list.remove(condition)
                break

        if not condition_found:
            raise TableNotSortedException("Tables are not sorted same!")

    def _check_columns_in_condition(self, left, right, condition):
        """
        Checks if the columns of two tables occur each in one side of the join condition.
        """
        right_name = self.right_schema.get_fully_qualified_column_name(right.value) \
            if "." in condition.right.value \
            else right.value

        left_name = self.left_schema.get_fully_qualified_column_name(left.value) \
            if "." in condition.right.value \
            else left.value

        return (left_name == condition.left.value and right_name == condition.right.value or
                left_name == condition.right.value and right_name == condition.left.value)

    def __str__(self):
        schema = self.get_schema()

        return f"MergeJoin({self.join_type.value}, " \
               f"natural={self.is_natural}, " \
               f"condition={get_string_representation(self.condition, schema)})"


class TableNotSortedException(CompilerException):
    pass
