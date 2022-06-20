from copy import deepcopy
from mosaic import table_service
from .abstract_operator import AbstractOperator
from ..compiler_exception import CompilerException
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_expression import ComparativeExpression, ComparativeOperator
from ..expressions.literal_expression import LiteralExpression
from ..get_string_representation import get_string_representation
from ...table_service import Table, _get_index_name


class IndexSeek(AbstractOperator):
    """
    Class that represents an index seek.
    Only supports conditions which are simple equalities.
    """

    def __init__(self, table_name, index_column, condition, alias=None):
        super().__init__()
        self.table_name = table_name
        self.alias = alias
        self.condition = condition
        self.schema = deepcopy(table_service.retrieve_table(self.table_name, makeCopy=False).schema)
        if self.alias is not None:
            self.schema.rename(self.alias)
        self.index_column = self.schema.get_simple_column_name(index_column)
        self.index = table_service.retrieve_index(self.table_name, self.index_column)
        self.comparison_value = self._consume_condition()

    def get_result(self):
        result = self._get_index_records()
        return Table(self.schema, result)

    def get_schema(self):
        return self.schema

    def get_num_records(self):
        return len(self._get_index_records())

    def _get_index_records(self):
        key = self.comparison_value
        if key in self.index:
            result = self.index[key]
        else:
            result = []
        return result

    def __str__(self):
        schema = self.get_schema()
        if self.alias is None:
            return f"IndexSeek({_get_index_name(self.table_name, self.index_column)}, condition={get_string_representation(self.condition, schema)})"
        return f"IndexSeek({_get_index_name(self.table_name, self.index_column)}, table_alias={self.alias}, condition={get_string_representation(self.condition, schema)})"

    def explain(self, rows, indent):
        super().explain(rows, indent)

    def _consume_condition(self):
        if isinstance(self.condition, ComparativeExpression) and \
                self.condition.operator == ComparativeOperator.EQUAL:
            left = self.condition.left
            right = self.condition.right
            if isinstance(left, ColumnExpression):
                column_name = left.get_result()
                if isinstance(right, LiteralExpression):
                    value = right.get_result()
                else:
                    raise ErrorInIndexSeekConditionException("No literal found in IndexSeek condition")
            elif isinstance(right, ColumnExpression):
                column_name = right.get_result()
                if isinstance(left, LiteralExpression):
                    value = left.get_result()
                else:
                    raise ErrorInIndexSeekConditionException("No literal found in IndexSeek condition")
            else:
                raise ErrorInIndexSeekConditionException("No column reference found in IndexSeek condition")
        else:
            raise IndexSeekConditionNotSupportedException("IndexSeek only supports conditions which are simple "
                                                          "equalities")
        if self._column_name_is_supported(column_name):
            return value
        else:
            raise ErrorInIndexSeekConditionException(
                "Referenced column in IndexSeek condition doesn't match the actual index column")

    def _column_name_is_supported(self, column_name):
        return (column_name in self.schema.column_names or column_name in self.schema.get_simple_column_name_list()) and \
               self.schema.get_simple_column_name(column_name) == self.schema.get_simple_column_name(self.index_column)


class IndexSeekConditionNotSupportedException(CompilerException):
    pass


class ErrorInIndexSeekConditionException(CompilerException):
    pass
