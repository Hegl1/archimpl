from .abstract_join_operator import *
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from ...table_service import TableIndexException


class HashJoin(AbstractJoin):

    def get_result(self):
        table1 = self.table1_reference.get_result()
        table2 = self.table2_reference.get_result()
        self.check_table_names(table1.schema, table2.schema)

        if self.join_type == JoinType.CROSS:
            raise JoinTypeNotSupportedException

        if self.is_natural and self.join_type != JoinType.CROSS:
            self.condition = self._build_natural_join_condition(table1.schema, table2.schema)

        result_schema = self._build_schema(table1.schema, table2.schema)
        result_records = []
        table1_hash = self._build_hash(table1, self.condition)
        table2_hash_reference = self._get_join_column_indices(table2, self.condition)
        used_keys = set()
        for tab2_record in table2.records:
            tab2_key = self._get_referenced_columns_from_list(table2_hash_reference, tab2_record)
            if tab2_key in table1_hash:
                used_keys.add(tab2_key)
                for tab1_record in table1_hash[tab2_key]:
                    result_records.append(self._build_record(tab1_record, tab2_record, table2))
        if self.join_type == JoinType.LEFT_OUTER:
            for tab1_key in table1_hash.keys():
                if tab1_key not in used_keys:
                    for tab1_record in table1_hash[tab1_key]:
                        result_records.append(
                            self._build_record(tab1_record, self._build_null_record(len(table2.schema.column_names)),
                                               table2))

        return Table(result_schema, result_records)

    def _build_record(self, left_record, right_record, table2):
        if not self.is_natural:
            return left_record + right_record
        else:
            index_to_exclude = self._get_join_column_indices(table2, self.condition)
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

    def _check_comparative_condition_supported(self, comparative):
        if not isinstance(comparative, ComparativeOperationExpression) or \
                not isinstance(comparative.right, ColumnExpression) or \
                not isinstance(comparative.left, ColumnExpression) or \
                comparative.operator != ComparativeOperator.EQUAL:
            raise JoinConditionNotSupportedException("HashJoin only supports conjunctions of equalities or "
                                                     "simple "
                                                     "equalities")

    def _check_comparative_condition_invalid_references(self, schema1, schema2, comparative):
        self._get_join_column_index_from_comparative(schema1, comparative)
        self._get_join_column_index_from_comparative(schema2, comparative)

    def _build_hash(self, relation, condition):
        hash_reference_index = self._get_join_column_indices(relation, condition)
        result = dict()
        for record in relation.records:
            key = self._get_referenced_columns_from_list(hash_reference_index, record)
            if key not in result:
                result[key] = [record]
            else:
                result[key].append(record)
        return result

    def _get_referenced_columns_from_list(self, reference_list, record):
        return tuple([record[reference] for reference in reference_list])

    def _get_join_column_indices(self, relation, condition):
        """
        Returns a list of values corresponding to the equivalences used in the condition.
        i.e. hoeren.MatrNr = pruefen.MatrNr and hoeren.VorlNr = pruefen.VorlNr
            -> (0, 1) (indices for the left relation.)
        """
        columns = []
        if isinstance(condition, ComparativeOperationExpression):
            columns.append([self._get_join_column_index_from_comparative(relation.schema, condition)])
        else:
            for comparative_condition in condition.value:
                columns.append(self._get_join_column_index_from_comparative(relation.schema, comparative_condition))
        return columns

    def _get_join_column_index_from_comparative(self, schema, condition):
        left = None
        right = None
        try:
            left = schema.get_column_index(condition.left.get_result())
        except TableIndexException:
            pass
        try:
            right = schema.get_column_index(condition.right.get_result())
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
        return f"HashJoin(natural={self.is_natural}, condition={self.condition})"
