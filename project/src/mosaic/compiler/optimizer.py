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
from .operators.nested_loops_join_operators import NestedLoopsJoin
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
    elif isinstance(node, (NestedLoopsJoin, AbstractSetOperator)):
        node.table1_reference = _selection_access_helper(node.table1_reference, selection_function)
        node.table2_reference = _selection_access_helper(node.table2_reference, selection_function)
    elif isinstance(node, (OrderingOperator, Projection)):
        node.table_reference = _selection_access_helper(node.table_reference, selection_function)
    elif isinstance(node, Selection):
        node = selection_function(node)

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
    is_disjunctive = isinstance(selection.condition, DisjunctiveExpression) # TODO: also consider disjunctive

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
        selection_columns = _get_comparative_columns(selection.condition)
        selection_columns_replacements = {}

        push_down = True

        for alias, column_reference in child_node.column_references:
            if alias is not None and alias in selection_columns:
                if isinstance(column_reference, ColumnExpression):
                    selection_columns_replacements[alias] = column_reference.get_result()
                else:
                    # can't be pushed down
                    push_down = False
                    break

        if push_down:
            _replace_comparative_columns(selection.condition, selection_columns_replacements)

            node = child_node

            selection.table_reference = child_node.table_reference
            child_node.table_reference = _selection_access_helper(selection, _selection_push_down)
    elif isinstance(child_node, NestedLoopsJoin):
        # TODO: use AbstractJoin after merge
        # TODO: implement
        pass
    elif isinstance(child_node, AbstractSetOperator):
        # TODO: implement
        pass

    if node == selection:
        _pushed_down_selections.add(selection)

    return node


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
