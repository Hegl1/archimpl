from mosaic.compiler.get_string_representation import get_string_representation
from .abstract_join import *
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_expression import ComparativeExpression, ComparativeOperator


class HashJoin(AbstractJoin):

    def _get_result(self):
        table1 = self.left_node.get_result()
        table2 = self.right_node.get_result()

        result_records = []

        table1_hash = self._build_hash(table1, self.left_schema, self.condition)
        used_keys = set()

        self._build_matching_records(
            table2, table1_hash, used_keys, result_records)

        if self.join_type == JoinType.LEFT_OUTER:
            self._build_not_matching_records(
                table2, table1_hash, used_keys, result_records)

        return Table(self.schema, result_records)

    def _build_matching_records(self, table2, table1_hash, used_keys, result_records):
        """
        Builds the result records that have a join partner in table1 according to table1_hash.
        Adds resulting tuples in result_records.
        Used_keys gets filled with all the keys out of table1_hash that got used to build a tuple.
        """
        table2_hash_reference = self._get_join_column_indices(
            table2, self.condition)
        for tab2_record in table2.records:
            tab2_key = self._get_referenced_column_values(
                table2_hash_reference, tab2_record)
            if tab2_key in table1_hash:
                used_keys.add(tab2_key)
                for tab1_record in table1_hash[tab2_key]:
                    result_records.append(self._build_record(
                        tab1_record, tab2_record, table2))

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
            index_to_exclude = self._get_join_column_indices(
                right_table, self.condition)
            new_record = []
            for i, target in enumerate(right_record):
                if i not in index_to_exclude:
                    new_record.append(target)
            return left_record + new_record

    def check_condition(self, schema1, schema2, condition):
        if isinstance(condition, ConjunctiveExpression):
            for comparative in condition.conditions:
                self._check_comparative_condition_supported(comparative)
                self._check_comparative_condition_invalid_references(
                    schema1, schema2, comparative)
        else:
            self._check_comparative_condition_supported(condition)
            self._check_comparative_condition_invalid_references(
                schema1, schema2, condition)

    def check_join_type(self):
        if self.join_type == JoinType.CROSS:
            raise JoinTypeNotSupportedException(
                "Cross joins are not supported by Hashjoin")

    def _check_comparative_condition_supported(self, condition):
        if not is_comparative_condition_supported(condition):
            raise JoinConditionNotSupportedException("HashJoin only supports conjunctions of equalities or "
                                                     "simple equalities that only contain column references")

    def _build_hash(self, relation, schema, condition):
        """
        Builds a hash table of all rows of the given relation based on the join condition.
        Results in a dictionary with (column_val1, ...) as key and a list of records as value.
        """
        hash_reference_index = self._get_join_column_indices(schema, condition)
        result = dict()
        for record in relation.records:
            key = self._get_referenced_column_values(
                hash_reference_index, record)
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

    def __str__(self):
        schema = self.get_schema()

        return f"HashJoin({self.join_type.value}, natural={self.is_natural}, condition={get_string_representation(self.condition, schema)})"


def is_comparative_condition_supported(condition):
    """
    Method that checks whether the condition is a simple comparative
    (i.e. a comparative that checks the equality between columns)
    """
    return isinstance(condition, ComparativeExpression) and \
           isinstance(condition.right, ColumnExpression) and \
           isinstance(condition.left, ColumnExpression) and \
           condition.operator == ComparativeOperator.EQUAL
