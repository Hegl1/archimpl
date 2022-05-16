from .abstract_join_operator import *
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator

# TODO conditions with wrong table names?
# TODO conditions with simple table names?
# TODO refactor!


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
        table2_hash_reference = table2.schema.get_column_index(self._get_hash_reference(table2, self.condition))
        used_keys = set()
        for tab2_record in table2.records:
            if tab2_record[table2_hash_reference] in table1_hash:
                used_keys.add(tab2_record[table2_hash_reference])
                for tab1_record in table1_hash[tab2_record[table2_hash_reference]]:
                    result_records.append(self._build_record(tab1_record, tab2_record, table2))
        if self.join_type == JoinType.LEFT_OUTER:
            for key in table1_hash.keys():
                if key not in used_keys:
                    for tab1_record in table1_hash[key]:
                        result_records.append(self._build_record(tab1_record, self._build_null_record(len(table2.schema.column_names)), table2))

        return Table(result_schema, result_records)

    def _build_record(self, left_record, right_record, table2):
        if not self.is_natural:
            return left_record + right_record
        else:
            index_to_exclude = table2.schema.get_column_index(self._get_hash_reference(table2, self.condition))
            return left_record + right_record[:index_to_exclude] + right_record[index_to_exclude+1:]

    def check_condition(self, schema1, schema2, condition):
        if not isinstance(condition, ComparativeOperationExpression) or \
                not isinstance(condition.left, ColumnExpression) or \
                not isinstance(condition.right, ColumnExpression) or \
                not (condition.operator == ComparativeOperator.EQUAL or \
                     condition.operator == ComparativeOperator.NOT_EQUAL):
            raise JoinConditionNotSupportedException
        # TODO add catch for columns not contained

    def _build_hash(self, relation, condition):
        hash_reference = self._get_hash_reference(relation, condition)
        hash_reference_index = relation.schema.get_column_index(hash_reference)
        result = dict()
        for record in relation.records:
            if record[hash_reference_index] not in result:
                result[record[hash_reference_index]] = [record]
            else:
                result[record[hash_reference_index]].append(record)
        return result

    def _get_hash_reference(self, relation, condition):
        hash_reference = None
        if condition.left.get_result() in relation.schema.column_names and condition.right.get_result() in relation.schema.column_names:
            raise
        elif condition.left.get_result() in relation.schema.column_names:
            hash_reference = condition.left.get_result()
        elif condition.right.get_result() in relation.schema.column_names:
            hash_reference = condition.right.get_result()
        else:
            # todo raise exception
            pass
        return hash_reference

    def __str__(self):
        return f"HashJoin(natural={self.is_natural}, condition={self.condition})"
