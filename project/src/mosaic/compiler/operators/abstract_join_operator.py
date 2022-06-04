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


LEFT_NATURAL_PADDING = "__left__."
RIGHT_NATURAL_PADDING = "__right__."


class AbstractJoin(AbstractOperator):

    def __init__(self, table1_reference, table2_reference, join_type, condition, is_natural):
        super().__init__()

        self.table1_reference = table1_reference
        self.table2_reference = table2_reference
        self.table1_schema = self.table1_reference.get_schema()
        self.table2_schema = self.table2_reference.get_schema()
        self.join_type = join_type
        self.is_natural = is_natural
        self.check_join_type()
        if self.is_natural and self.join_type != JoinType.CROSS:
            self._pad_natural_join_schema()
            self.condition = self._build_natural_join_condition()
        else:
            self.condition = condition
        self.schema = self._build_schema()

    @abstractmethod
    def _get_result(self): # pragma: no cover
        """
        Calculates the effective result of a join operator but keeps natural join padding.
        """
        pass

    def get_result(self):
        result = self._get_result()
        if self.is_natural and self.join_type != JoinType.CROSS:
            self._unpad_natural_join_schema()
        return result

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

    def _build_schema(self):
        self.check_table_names(self.table1_schema, self.table2_schema)
        self.check_condition(self.table1_schema, self.table2_schema, self.condition)
        if self.is_natural and self.join_type != JoinType.CROSS:
            return self._get_natural_join_schema()
        else:
            joined_table_name = f"{self.table1_schema.table_name}_{self.join_type.value}_join_{self.table2_schema.table_name}"
            return Schema(joined_table_name, self.table1_schema.column_names + self.table2_schema.column_names,
                          self.table1_schema.column_types + self.table2_schema.column_types)

    def _get_natural_join_schema(self):
        """
        Method that builds the schema for a natural join.
        In case of a natural join, the "matching" column is only emitted once.
        e.g: (hoeren.MatrNr, hoeren.VorlNr) natural join (pruefen.MatrNr, pruefen.Note)
        yields (hoeren.MatrNr, hoeren.VorlNr, pruefen.Note)
        """
        joined_table_name = f"{self.table1_schema.table_name}_natural_{self.join_type.value}_join_{self.table2_schema.table_name}"
        schema2_col_names = []
        schema2_col_types = []
        for schema_name, schema_type in zip(self.table2_schema.column_names, self.table2_schema.column_types):
            if self.table2_schema.get_simple_column_name(schema_name) not in self.table1_schema.get_simple_column_name_list():
                schema2_col_names.append(schema_name)
                schema2_col_types.append(schema_type)
        return Schema(joined_table_name, self._unpad_column_names(self.table1_schema.column_names, LEFT_NATURAL_PADDING) + self._unpad_column_names(schema2_col_names, RIGHT_NATURAL_PADDING),
                      self.table1_schema.column_types + schema2_col_types)

    def _build_natural_join_condition(self):
        """
        Method that builds the condition for a natural join based on the schemas of the join partners.
        Returns either a ConjunctiveExpression or a ComparativeOperationExpression (in case of only one matching column)
        If no matching pairs are found, None is returned and join type gets set to cross.
        """
        matching_pairs = self._find_matching_simple_column_names()
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

    def _find_matching_simple_column_names(self):
        """
        Finds matching simple column names between 2 schemas and returns a list of tuples of the corresponding
        fully qualified names.
        """
        matching_pairs = []
        for name in self.table1_schema.column_names:
            if self.table1_schema.get_simple_column_name(name) in self.table2_schema.get_simple_column_name_list():
                matching_pairs.append(
                    (name, self.table2_schema.get_fully_qualified_column_name(self.table1_schema.get_simple_column_name(name))))
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

    def _get_join_column_indices(self, schema, condition):
        """
        Returns a list of values corresponding to the equivalences used in the condition.
        i.e. hoeren.MatrNr = pruefen.MatrNr and hoeren.VorlNr = pruefen.VorlNr
            -> (0, 1) (indices for the left relation.)
        """
        columns = []
        if isinstance(condition, ComparativeOperationExpression):
            columns.append(self._get_join_column_index_from_comparative(schema, condition))
        else:
            for comparative_condition in condition.conditions:
                columns.append(self._get_join_column_index_from_comparative(schema, comparative_condition))
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

    def _pad_natural_join_schema(self):
        """
        Pads alias column names from both schemas with a constant to distinguish them in case of a natural join.
        """
        self.table1_schema.column_names = [LEFT_NATURAL_PADDING + name if "." not in name else name for name in self.table1_schema.column_names]
        self.table2_schema.column_names = [RIGHT_NATURAL_PADDING + name if "." not in name else name for name in self.table2_schema.column_names]

    def _unpad_natural_join_schema(self):
        """
        Removes padding from both schemas column names introduced with natural joins.
        """
        self.table1_schema.column_names = self._unpad_column_names(self.table1_schema.column_names, LEFT_NATURAL_PADDING)
        self.table2_schema.column_names = self._unpad_column_names(self.table2_schema.column_names, RIGHT_NATURAL_PADDING)

    def _unpad_column_names(self, column_names, prefix):
        """
        Removes the specified prefix from the given list of column names
        """
        return [name.removeprefix(prefix) for name in column_names]


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
