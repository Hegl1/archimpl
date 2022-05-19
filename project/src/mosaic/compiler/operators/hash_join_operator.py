from .abstract_join_operator import *
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from ...table_service import TableIndexException


class HashJoin(AbstractJoin):

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()

        if self.is_natural and self.join_type != JoinType.CROSS:
            self.condition = self._build_natural_join_condition(table1.schema, table2.schema)

        if self.join_type == JoinType.CROSS:
            raise JoinTypeNotSupportedException("Cross joins are not supported by Hashjoin")

        result_schema = self._build_schema(table1.schema, table2.schema)
        result_records = []
        table1_hash = self._build_hash(table1, self.condition)
        used_keys = set()

        self._build_matching_records(table2, table1_hash, used_keys, result_records)

        if self.join_type == JoinType.LEFT_OUTER:
            self._build_not_matching_records(table2, table1_hash, used_keys, result_records)

        return Table(result_schema, result_records)

    def _build_matching_records(self, table2, table1_hash, used_keys, result_records):
        """
        Builds the result records that have a join partner in table1 according to table1_hash.
        Adds resulting tuples in result_records.
        Used_keys gets filled with all the keys out of table1_hash that got used to build a tuple.
        """
        table2_hash_reference = self._get_join_column_indices(table2, self.condition)
        for tab2_record in table2.records:
            tab2_key = self._get_referenced_column_values(table2_hash_reference, tab2_record)
            if tab2_key in table1_hash:
                used_keys.add(tab2_key)
                for tab1_record in table1_hash[tab2_key]:
                    result_records.append(self._build_record(tab1_record, tab2_record, table2))

    def _build_not_matching_records(self, table2, table1_hash, used_keys, result_records):
        """
        Builds null tuples for all unused keys in table1_hash and add them to result_records.
        """
        for tab1_key in table1_hash.keys():
            if tab1_key not in used_keys:
                for tab1_record in table1_hash[tab1_key]:
                    result_records.append(
                        self._build_record(tab1_record, self._build_null_record(len(table2.schema.column_names)),
                                           table2))

    def _build_record(self, left_record, right_record, right_table):
        """
        Method that builds a record if a match is found.
        In case of a natural join, the right table is needed to eliminate join columns.
        """
        if not self.is_natural:
            return left_record + right_record
        else:
            index_to_exclude = self._get_join_column_indices(right_table, self.condition)
            new_record = []
            for i, target in enumerate(right_record):
                if i not in index_to_exclude:
                    new_record.append(target)
            return left_record + new_record

    def check_condition(self, schema1, schema2, condition):
        if isinstance(condition, ConjunctiveExpression):
            for comparative in condition.value:
                self._check_comparative_condition_supported(comparative)
                self._check_comparative_condition_invalid_references(schema1, schema2, comparative)
        else:
            self._check_comparative_condition_supported(condition)
            self._check_comparative_condition_invalid_references(schema1, schema2, condition)

    def _check_comparative_condition_supported(self, condition):
        """
        Method that checks whether the condition is a simple comparative
        (i.e. a comparative that checks the equality between columns)
        """
        if not isinstance(condition, ComparativeOperationExpression) or \
                not isinstance(condition.right, ColumnExpression) or \
                not isinstance(condition.left, ColumnExpression) or \
                condition.operator != ComparativeOperator.EQUAL:
            raise JoinConditionNotSupportedException("HashJoin only supports conjunctions of equalities or "
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

    def _build_hash(self, relation, condition):
        """
        Builds a hash table of all rows of the given relation based on the join condition.
        Results in a dictionary with (column_val1, ...) as key and a list of records as value.
        """
        hash_reference_index = self._get_join_column_indices(relation, condition)
        result = dict()
        for record in relation.records:
            key = self._get_referenced_column_values(hash_reference_index, record)
            if key not in result:
                result[key] = [record]
            else:
                result[key].append(record)
        return result

    def _get_referenced_column_values(self, reference_list, record):
        """
        Returns the values of a record that got referenced by index in reference_list.
        """
        return tuple([record[reference] for reference in reference_list])

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
            for comparative_condition in condition.value:
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
        return f"HashJoin(natural={self.is_natural}, condition={self.condition}, left={self.join_type == JoinType.LEFT_OUTER})"
