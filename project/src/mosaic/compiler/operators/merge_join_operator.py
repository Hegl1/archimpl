import itertools

from .abstract_join_operator import *
from .ordering_operator import OrderingOperator
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from ...table_service import TableIndexException


class MergeJoin(AbstractJoin):

    def __init__(self, table1_reference, table2_reference, join_type, condition, is_natural):
        super().__init__(table1_reference, table2_reference, join_type, condition, is_natural)

        self.right_table_finished = False
        self.left_table_referenced_column_indices = self._get_join_column_indices(self.table1_reference.get_schema(),
                                                                                  self.condition)
        self.right_table_referenced_column_indices = self._get_join_column_indices(self.table2_reference.get_schema(),
                                                                                   self.condition)

    def _get_result(self):
        self._check_tables_sorting()
        left_table = self.table1_reference.get_result()
        right_table = self.table2_reference.get_result()
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

        if merge_condition == 0:
            left_record_index, right_record_index = self._build_matching_records(records, left_table, right_table,
                                                                                 left_record_index, right_record_index)

        elif merge_condition == 1 or self.right_table_finished:
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

        cross_product_record = self._build_cross_product_of_records_aux(left_records, right_records, right_table)

        return cross_product_record, left_record_end_index, right_record_end_index

    def _build_cross_product_of_records_aux(self, left_records, right_records, right_table):
        """
        Builds the cross product of the matching records.
        In case of a natural join, the right table is needed to eliminate join columns.
        """
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
        match = 0

        for column_index1, column_index2 in zip(self.left_table_referenced_column_indices,
                                                self.right_table_referenced_column_indices):
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

    def _check_tables_sorting(self):
        """
        Checks if the two tables are sorted correctly for the given join conditions.
        For all join conditions the referenced columns from the condition have to be sorted.
        If not an exception gets raised.
        """
        if not isinstance(self.table1_reference, OrderingOperator) or \
                not isinstance(self.table2_reference, OrderingOperator):
            raise TableNotSortedException("Tables are not sorted!")

        if not (self.table1_reference.column_list != self.table2_reference.column_list):
            raise TableNotSortedException("Tables are not sorted the same!")

    def __str__(self):
        schema = self.get_schema()
        if self.condition is not None:
            self.condition.replace_all_column_names_by_fqn(schema)
        return f"MergeJoin({self.join_type.value}, natural={self.is_natural}, condition={self.condition.__str__()})"


class TableNotSortedException(CompilerException):
    pass
