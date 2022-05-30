from enum import Enum
from abc import abstractmethod
from mosaic.table_service import Table, Schema, TableIndexException
from .abstract_operator import AbstractOperator
from ..expressions.column_expression import ColumnExpression
from ..expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from ..expressions.conjunctive_expression import ConjunctiveExpression
from ..compiler_exception import CompilerException


class JoinType(Enum):
    CROSS = "cross"
    INNER = "inner"
    LEFT_OUTER = "left_outer"


class AbstractJoin(AbstractOperator):

    def __init__(self, table1_reference, table2_reference, join_type, condition, is_natural):
        super().__init__()

        self.table1_reference = table1_reference
        self.table2_reference = table2_reference
        schema1 = self.table1_reference.get_schema()
        schema2 = self.table2_reference.get_schema()
        self.join_type = join_type
        self.is_natural = is_natural
        self.check_join_type()
        if self.is_natural and self.join_type != JoinType.CROSS:
            self.condition = self._build_natural_join_condition(schema1, schema2)
        else:
            self.condition = condition
        self.schema = self._build_schema(schema1, schema2)

    @abstractmethod
    def get_result(self): # pragma: no cover
        pass

    @abstractmethod
    def check_condition(self, schema1, schema2, condition): # pragma: no cover
        """
        Method that checks whether the join can be performed with the given condition and the given schemas.
        """
        pass

    @abstractmethod
    def check_join_type(self): # pragma: no cover
        pass

    def check_table_names(self, schema1, schema2):
        if schema1.table_name == schema2.table_name:
            raise SelfJoinWithoutRenamingException(f"Table \"{schema1.table_name}\" can't be joined with itself "
                                                   f"without renaming one of the occurrences")

    def get_schema(self):
        return self.schema

    def _build_schema(self, schema1, schema2):
        self.check_table_names(schema1, schema2)
        self.check_condition(schema1, schema2, self.condition)
        if self.is_natural and self.join_type != JoinType.CROSS:
            return self._get_natural_join_schema(schema1, schema2)
        else:
            joined_table_name = f"{schema1.table_name}_{self.join_type.value}_join_{schema2.table_name}"
            return Schema(joined_table_name, schema1.column_names + schema2.column_names,
                          schema1.column_types + schema2.column_types)

    def _get_natural_join_schema(self, schema1, schema2):
        """
        Method that builds the schema for a natural join.
        In case of a natural join, the "matching" column is only emitted once.
        e.g: (hoeren.MatrNr, hoeren.VorlNr) natural join (pruefen.MatrNr, pruefen.Note)
        yields (hoeren.MatrNr, hoeren.VorlNr, pruefen.Note)
        """
        joined_table_name = f"{schema1.table_name}_natural_{self.join_type.value}_join_{schema2.table_name}"
        schema2_col_names = []
        schema2_col_types = []
        for schema_name, schema_type in zip(schema2.column_names, schema2.column_types):
            if schema2.get_simple_column_name(schema_name) not in schema1.get_simple_column_name_list():
                schema2_col_names.append(schema_name)
                schema2_col_types.append(schema_type)
        return Schema(joined_table_name, schema1.column_names + schema2_col_names,
                      schema1.column_types + schema2_col_types)

    def _build_natural_join_condition(self, schema1, schema2):
        """
        Method that builds the condition for a natural join based on the schemas of the join partners.
        Returns either a ConjunctiveExpression or a ComparativeOperationExpression (in case of only one matching column)
        If no matching pairs are found, None is returned and join type gets set to cross.
        """
        matching_pairs = self._find_matching_simple_column_names(schema1, schema2)
        if len(matching_pairs) == 0:
            self.join_type = JoinType.CROSS
            self.check_join_type()
            return None
        elif len(matching_pairs) == 1:
            # construct equivalence
            return ComparativeOperationExpression(ColumnExpression(matching_pairs[0][0]),
                                                  ComparativeOperator.EQUAL, ColumnExpression(matching_pairs[0][1]))
        else:
            # construct conjunctive
            return ConjunctiveExpression([ComparativeOperationExpression(
                ColumnExpression(pair[0]),
                ComparativeOperator.EQUAL,
                ColumnExpression(pair[1])) for pair in matching_pairs])

    def _find_matching_simple_column_names(self, schema1, schema2):
        """
        Finds matching simple column names between 2 schemas and returns a list of tuples of the corresponding
        fully qualified names.
        """
        matching_pairs = []
        for name in schema1.column_names:
            if schema1.get_simple_column_name(name) in schema2.get_simple_column_name_list():
                matching_pairs.append(
                    (name, schema2.get_fully_qualified_column_name(schema1.get_simple_column_name(name))))
        return matching_pairs

    def _build_null_record(self, num_entries):
        """
        Method that builds a null record based on the number of entries.
        """
        return [None] * num_entries

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

    def simplify(self):
        self.table1_reference = self.table1_reference.simplify()
        self.table2_reference = self.table2_reference.simplify()

        if self.condition is not None:
            self.condition = self.condition.simplify()

        return self

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table1_reference.explain(rows, indent + 2)
        self.table2_reference.explain(rows, indent + 2)


class ConditionNotValidException(CompilerException):
    pass


class SelfJoinWithoutRenamingException(CompilerException):
    pass


class JoinTypeNotSupportedException(CompilerException):
    pass


class JoinConditionNotSupportedException(CompilerException):
    pass


class ErrorInJoinConditionException(CompilerException):
    pass
