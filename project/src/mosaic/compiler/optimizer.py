from copy import deepcopy

from mosaic.table_service import Schema, TableIndexException
from .abstract_compile_node import AbstractCompileNode
from .expressions.abstract_expression import AbstractExpression
from .expressions.column_expression import ColumnExpression
from .expressions.conjunctive_expression import ConjunctiveExpression
from .expressions.disjunctive_expression import DisjunctiveExpression
from .expressions.comparative_operation_expression import ComparativeOperationExpression
from .operators.abstract_operator import AbstractOperator
from .operators.selection_operator import Selection
from .operators.explain import Explain
from .operators.hash_distinct_operator import HashDistinct
from .operators.abstract_join_operator import AbstractJoin, JoinType
from .operators.ordering_operator import OrderingOperator
from .operators.projection_operator import Projection
from .operators.set_operators import AbstractSetOperator


def optimize(execution_plan: AbstractOperator):
    execution_plan = execution_plan.simplify()

    # selection push-down

    execution_plan = _selection_access_helper(execution_plan, _split_selections)
    execution_plan = _selection_access_helper(execution_plan, _selection_push_down)
    execution_plan = _selection_access_helper(execution_plan, _join_selections)

    return execution_plan


def _selection_access_helper(node: AbstractCompileNode, selection_function):
    if isinstance(node, AbstractExpression):
        return node

    if isinstance(node, Explain):
        node.execution_plan = _selection_access_helper(node.execution_plan, selection_function)
    elif isinstance(node, HashDistinct):
        node.table = _selection_access_helper(node.table, selection_function)
    elif isinstance(node, (AbstractJoin, AbstractSetOperator)):
        node.table1_reference = _selection_access_helper(node.table1_reference, selection_function)
        node.table2_reference = _selection_access_helper(node.table2_reference, selection_function)
    elif isinstance(node, (OrderingOperator, Projection)):
        node.table_reference = _selection_access_helper(node.table_reference, selection_function)
    elif isinstance(node, Selection):
        node = selection_function(node)

    # TODO: include aggregation after merge

    return node


def _split_selections(selection: Selection):
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


_pushed_down_selections = set()


def _selection_push_down(selection: Selection):
    if selection in _pushed_down_selections:
        return selection

    child_node = selection.table_reference
    node = selection

    if isinstance(child_node, HashDistinct):
        node = child_node

        selection.table_reference = child_node.table
        child_node.table = _selection_access_helper(selection, _selection_push_down)
    if isinstance(child_node, OrderingOperator):
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
        _pushed_down_selections.add(selection)

    return node


def _selection_push_through_projection(selection: Selection, projection: Projection):
    node = selection

    selection_columns = _get_comparative_columns(selection.condition)
    selection_columns_replacements = {}

    push_down = True

    # TODO check that all selection_columns were found
    for alias, column_reference in projection.column_references:
        if alias is not None and alias in selection_columns:
            if isinstance(column_reference, ColumnExpression):
                selection_columns_replacements[alias] = column_reference.get_result()
            else:
                # can't be pushed down
                push_down = False
                break

    if push_down:
        _replace_comparative_columns(selection.condition, selection_columns_replacements)

        node = projection

        selection.table_reference = projection.table_reference
        projection.table_reference = _selection_access_helper(selection, _selection_push_down)

    return node


def _selection_push_through_join_operator(selection: Selection, join_operator: AbstractJoin):
    node = selection

    selection_columns = _get_comparative_columns(selection.condition)

    table1_schema = join_operator.table1_reference.get_schema()
    table2_schema = join_operator.table2_reference.get_schema()

    is_fully_covered_table1 = _is_condition_fully_covered_in_schema(selection_columns, table1_schema, table2_schema)
    is_fully_covered_table2 = _is_condition_fully_covered_in_schema(selection_columns, table2_schema, table1_schema)

    if join_operator.is_natural and join_operator.join_type != JoinType.CROSS:
        pass # TODO: implement for natural join
    else:
        if is_fully_covered_table1:
            node = join_operator

            selection.table_reference = join_operator.table1_reference
            join_operator.table1_reference = _selection_access_helper(selection, _selection_push_down)
        elif is_fully_covered_table2:
            node = join_operator

            selection.table_reference = join_operator.table2_reference
            join_operator.table2_reference = _selection_access_helper(selection, _selection_push_down)

    return node


def _selection_push_through_set_operator(selection: Selection, set_operator: AbstractSetOperator):
    table1_schema = set_operator.table1_reference.get_schema()
    table2_schema = set_operator.table2_reference.get_schema()

    selection_columns = _get_comparative_columns(selection.condition)
    table_2_selection_columns_replacements = {}

    for column in selection_columns:
        simple_name = table1_schema.get_simple_column_name(column)
        table_2_selection_columns_replacements[column] = table2_schema.get_fully_qualified_column_name(simple_name)

    table1_selection = Selection(set_operator.table1_reference, selection.condition)
    table2_selection = Selection(set_operator.table2_reference, deepcopy(selection.condition))
    _replace_comparative_columns(table2_selection.condition, table_2_selection_columns_replacements)

    set_operator.table1_reference = _selection_access_helper(table1_selection, _selection_push_down)
    set_operator.table2_reference = _selection_access_helper(table2_selection, _selection_push_down)

    return set_operator


def _get_comparative_columns(expression):
    columns = []

    if isinstance(expression, ComparativeOperationExpression):
        if isinstance(expression.left, ColumnExpression):
            columns.append(expression.left.get_result())
        if isinstance(expression.right, ColumnExpression):
            columns.append(expression.right.get_result())
    elif isinstance(expression, (ConjunctiveExpression, DisjunctiveExpression)):
        for term in expression.conditions:
            columns += _get_comparative_columns(term)
    elif isinstance(expression, ColumnExpression):
        columns.append(expression.get_result())

    return columns


def _replace_comparative_columns(expression, column_replacement):
    if isinstance(expression, ComparativeOperationExpression):
        if isinstance(expression.left, ColumnExpression):
            if expression.left.get_result() in column_replacement:
                expression.left.value = column_replacement[expression.left.value]
        if isinstance(expression.right, ColumnExpression):
            if expression.right.get_result() in column_replacement:
                expression.right.value = column_replacement[expression.right.value]
    elif isinstance(expression, (ConjunctiveExpression, DisjunctiveExpression)):
        for term in expression.conditions:
            _replace_comparative_columns(term, column_replacement)
    elif isinstance(expression, ColumnExpression):
        expression.value = column_replacement[expression.value]


def _is_condition_fully_covered_in_schema(columns, schema1: Schema, schema2: Schema):
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