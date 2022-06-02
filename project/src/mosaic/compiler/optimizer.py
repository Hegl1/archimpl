from copy import deepcopy

from mosaic.table_service import Schema, TableIndexException
from .abstract_compile_node import AbstractCompileNode
from .expressions.abstract_expression import AbstractExpression
from .expressions.column_expression import ColumnExpression
from .expressions.conjunctive_expression import ConjunctiveExpression
from .expressions.disjunctive_expression import DisjunctiveExpression
from .expressions.comparative_operation_expression import ComparativeOperationExpression
from .expressions.arithmetic_operation_expression import ArithmeticOperationExpression
from .operators.abstract_operator import AbstractOperator
from .operators.selection_operator import Selection
from .operators.explain import Explain
from .operators.hash_distinct_operator import HashDistinct
from .operators.abstract_join_operator import AbstractJoin, JoinType
from .operators.ordering_operator import OrderingOperator
from .operators.projection_operator import Projection
from .operators.set_operators import AbstractSetOperator
from .operators.hash_aggregate_operator import HashAggregate


_pushed_down_selections = set() # contains the selections, that have been pushed down as far as possible


def optimize(execution_plan: AbstractOperator):
    """
    Function that optimizes the given execution plan by doing the following:

    1. Simplification (simplify()-method)
    2. Selection push-down
        2.1 Split conjunctive selections into multiple
        2.2 Selection push-down
        2.3 Join consecutive selections to one conjunctive selection
    3. Replace nested-loops-joins by best replacement join (if possible)

    Returns the optimized execution plan
    """
    execution_plan = execution_plan.simplify()

    # selection push-down
    execution_plan = _selection_access_helper(execution_plan, _split_selections)
    execution_plan = _selection_access_helper(execution_plan, _selection_push_down)
    execution_plan = _selection_access_helper(execution_plan, _join_selections)

    # TODO: replace nested-loops-joins by best replacement join (if possible)

    global _pushed_down_selections
    _pushed_down_selections = set()

    return execution_plan


def _selection_access_helper(node: AbstractCompileNode, selection_function):
    """
    Helper function to access the selection nodes recursively in the given node.
    If the current node is not a selection, its child-operators are replaced by the
    return value of the recursive call with the child-operator.
    If the current node is a selection, the selection_function is called with the selection
    as parameter and the return value is returned.
    """
    if isinstance(node, Explain):
        node.execution_plan = _selection_access_helper(node.execution_plan, selection_function)
    elif isinstance(node, (AbstractJoin, AbstractSetOperator)):
        node.table1_reference = _selection_access_helper(node.table1_reference, selection_function)
        node.table2_reference = _selection_access_helper(node.table2_reference, selection_function)
    elif isinstance(node, (OrderingOperator, Projection, HashAggregate, HashDistinct)):
        node.table_reference = _selection_access_helper(node.table_reference, selection_function)
    elif isinstance(node, Selection):
        node = selection_function(node)

    return node


def _split_selections(selection: Selection):
    """
    Splits the given selection into multiple selections (only conjunctive are split).
    Returns the resulting top-level selection, that contains the other selections as children
    """
    if isinstance(selection.condition, ConjunctiveExpression):
        conjunctive_conditions = selection.condition.conditions
        condition = conjunctive_conditions.pop(0)

        conjunctive_expression = ConjunctiveExpression(conjunctive_conditions)
        conjunctive_expression = conjunctive_expression.simplify()

        selection.table_reference = Selection(selection.table_reference, conjunctive_expression)
        selection.condition = condition

    selection.table_reference = _selection_access_helper(selection.table_reference, _split_selections)
    return selection


def _join_selections(selection: Selection):
    """
    Joins consecutive selections for the given selection.
    The resulting joined selection is returned (as conjunctive)
    """
    if isinstance(selection.table_reference, Selection):
        child_selection = selection.table_reference

        if isinstance(selection.condition, ConjunctiveExpression):
            conditions = selection.condition.conditions
        else:
            conditions = [selection.condition]

        if isinstance(child_selection.condition, ConjunctiveExpression):
            conditions = conditions + child_selection.condition.conditions
        else:
            conditions.append(child_selection.condition)

        selection.condition = ConjunctiveExpression(conditions)
        selection.table_reference = child_selection.table_reference

        selection = _join_selections(selection)

    selection.table_reference = _selection_access_helper(selection.table_reference, _join_selections)

    return selection


def _selection_push_down(selection: Selection):
    """
    Pushes down the given selection as far as possible and returns the top-level node
    that should replace the selection
    """
    if selection in _pushed_down_selections:
        return selection

    child_node = selection.table_reference
    node = selection

    if isinstance(child_node, (HashDistinct, OrderingOperator)):
        node = child_node

        selection.table_reference = child_node.table_reference
        child_node.table_reference = _selection_access_helper(selection, _selection_push_down)
    elif isinstance(child_node, Selection):
        node = child_node

        selection.table_reference = child_node.table_reference
        child_node.table_reference = _selection_access_helper(selection, _selection_push_down)

        node = _selection_access_helper(node, _selection_push_down)
    elif isinstance(child_node, Projection):
        node = _selection_push_through_projection(selection, child_node)
    elif isinstance(child_node, AbstractJoin):
        node = _selection_push_through_join_operator(selection, child_node)
    elif isinstance(child_node, AbstractSetOperator):
        node = _selection_push_through_set_operator(selection, child_node)

    if node == selection:
        node.table_reference = _selection_access_helper(node.table_reference, _selection_push_down)
        _pushed_down_selections.add(selection)

    return node


def _selection_push_through_projection(selection: Selection, projection: Projection):
    """
    Pushes the selection through the given projection by renaming the referenced columns
    in the selection and swapping the both operators.
    Returns the top-level node that contains the selection as child or the selection if
    push-through was not possible
    """
    projection_schema = projection.get_schema()
    child_schema = projection.table_reference.get_schema()

    selection_columns = _get_condition_columns(selection.condition)
    fqn_selection_columns = [projection_schema.get_fully_qualified_column_name(column) for column in selection_columns]
    selection_columns_replacements = {}

    found_columns = set()

    for alias, column_reference in projection.column_references:
        if alias is not None:
            if alias in selection_columns:
                found_columns.add(alias)

                if isinstance(column_reference, ColumnExpression):
                    selection_columns_replacements[alias] = column_reference.get_result()
                else:
                    # can't be pushed down, return selection
                    return selection
        else:
            if isinstance(column_reference, ColumnExpression):
                column = column_reference.get_result()

                if column in selection_columns:
                    found_columns.add(projection_schema.get_fully_qualified_column_name(column))
                elif column in fqn_selection_columns:
                    simple_column = child_schema.get_simple_column_name(column)
                    if simple_column in selection_columns:
                        selection_columns_replacements[simple_column] = column
                        found_columns.add(column)
                    else:
                        raise Exception("Unexpected exception in selection push through projection")
                else:
                    fqn_column = projection_schema.get_fully_qualified_column_name(column)

                    if fqn_column in fqn_selection_columns:
                        selection_columns_replacements[column] = fqn_column
                        found_columns.add(fqn_column)

    if set(fqn_selection_columns) != found_columns:
        # can't be pushed down, return selection
        # will throw an exception on execution anyways
        return selection

    _replace_condition_columns(selection.condition, selection_columns_replacements)

    selection.table_reference = projection.table_reference
    projection.table_reference = _selection_access_helper(selection, _selection_push_down)

    return projection


def _selection_push_through_join_operator(selection: Selection, join_operator: AbstractJoin):
    """
    Pushes the selection through the given join-operator by checking that the referenced columns
    are fully-covered in only one side of the join and pushing it through there (see rules on slides).
    Returns the top-level node that should replace the selection.
    """
    node = selection

    selection_columns = _get_condition_columns(selection.condition)

    table1_schema = join_operator.table1_reference.get_schema()
    table2_schema = join_operator.table2_reference.get_schema()

    is_fully_covered_table1 = False
    is_fully_covered_table2 = False
    table1_selection = selection
    table2_selection = selection

    if join_operator.is_natural and join_operator.join_type != JoinType.CROSS:
        if _are_columns_fully_covered_in_both_schemas(selection_columns, table1_schema, table2_schema):
            is_fully_covered_table1 = True
            is_fully_covered_table2 = True

            table1_selection = Selection(join_operator.table1_reference, selection.condition)
            table2_selection = Selection(join_operator.table2_reference, deepcopy(selection.condition))

    if not is_fully_covered_table1 and not is_fully_covered_table2:
        is_fully_covered_table1 = _are_columns_fully_covered_in_first_schema(selection_columns, table1_schema, table2_schema)
        is_fully_covered_table2 = _are_columns_fully_covered_in_first_schema(selection_columns, table2_schema, table1_schema)

    if is_fully_covered_table1:
        node = join_operator

        table1_selection.table_reference = join_operator.table1_reference
        join_operator.table1_reference = _selection_access_helper(table1_selection, _selection_push_down)

    if is_fully_covered_table2:
        node = join_operator

        table2_selection.table_reference = join_operator.table2_reference
        join_operator.table2_reference = _selection_access_helper(table2_selection, _selection_push_down)

    return node


def _selection_push_through_set_operator(selection: Selection, set_operator: AbstractSetOperator):
    """
    Pushes the selection through the given set-operator by pushing it through the left relation
    and renaming the columns and pushing it through the right relation.
    Returns the set-operator that should replace the selection
    """
    table1_schema = set_operator.table1_reference.get_schema()
    table2_schema = set_operator.table2_reference.get_schema()

    selection_columns = _get_condition_columns(selection.condition)
    table_2_selection_columns_replacements = {}

    for column in selection_columns:
        simple_name = table1_schema.get_simple_column_name(column)
        table_2_selection_columns_replacements[column] = table2_schema.get_fully_qualified_column_name(simple_name)

    table1_selection = Selection(set_operator.table1_reference, selection.condition)
    table2_selection = Selection(set_operator.table2_reference, deepcopy(selection.condition))
    _replace_condition_columns(table2_selection.condition, table_2_selection_columns_replacements)

    set_operator.table1_reference = _selection_access_helper(table1_selection, _selection_push_down)
    set_operator.table2_reference = _selection_access_helper(table2_selection, _selection_push_down)

    return set_operator


def _get_condition_columns(expression):
    """
    Returns the columns referenced in the given expression (recursively).
    """
    columns = []

    if isinstance(expression, (ComparativeOperationExpression, ArithmeticOperationExpression)):
        columns += _get_condition_columns(expression.left)
        columns += _get_condition_columns(expression.right)
    elif isinstance(expression, (ConjunctiveExpression, DisjunctiveExpression)):
        for term in expression.conditions:
            columns += _get_condition_columns(term)
    elif isinstance(expression, ColumnExpression):
        columns.append(expression.get_result())

    return columns


def _replace_condition_columns(expression, column_replacement):
    """
    Replaces the column-references in the given expression recursively.
    For that it uses the column_replacement, where the key is the column-name that
    should be replaced and the value with which it should be replaced
    """
    if isinstance(expression, (ComparativeOperationExpression, ArithmeticOperationExpression)):
        _replace_condition_columns(expression.left, column_replacement)
        _replace_condition_columns(expression.right, column_replacement)
    elif isinstance(expression, (ConjunctiveExpression, DisjunctiveExpression)):
        for term in expression.conditions:
            _replace_condition_columns(term, column_replacement)
    elif isinstance(expression, ColumnExpression):
        if expression.value in column_replacement:
            expression.value = column_replacement[expression.value]


def _are_columns_fully_covered_in_first_schema(columns, schema1: Schema, schema2: Schema):
    """
    Checks whether the given columns are only fully covered in the first schema.
    This means it should only reference columns of the first schema, but none of the second one
    (all referenced columns have to be inside the first schema).
    """
    for column in columns:
        try:
            schema1.get_column_index(column)
        except TableIndexException:
            return False

        try:
            schema2.get_column_index(column)
            return False
        except TableIndexException:
            pass
    return True


def _are_columns_fully_covered_in_both_schemas(columns, schema1: Schema, schema2: Schema):
    """
    Checks whether the given columns are fully covered in both schemas.
    This means each referenced column has to be contained in the first and the second schema.
    """
    for column in columns:
        try:
            schema1.get_column_index(column)
        except TableIndexException:
            return False

        try:
            schema2.get_column_index(column)
        except TableIndexException:
            return False
    return True
