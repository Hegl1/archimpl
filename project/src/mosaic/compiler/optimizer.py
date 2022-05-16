from project.src.mosaic.compiler.expressions.column_expression import ColumnExpression
from .abstract_compile_node import AbstractCompileNode
from .expressions.abstract_expression import AbstractExpression
from .expressions.conjunctive_expression import ConjunctiveExpression
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

    # selection pushdown

    execution_plan = _selection_access_helper(execution_plan, _split_selections)
    # execution_plan = _selection_access_helper(execution_plan, _join_selections)

    return execution_plan


def _selection_access_helper(node: AbstractCompileNode, selection_function):
    if isinstance(node, AbstractExpression):
        return node

    if isinstance(node, Explain):
        node.execution_plan = _selection_access_helper(node.execution_plan, selection_function)
    elif isinstance(node, HashDistinct):
        node.table = _selection_access_helper(node.table, selection_function)
    elif isinstance(node, NestedLoopsJoin) or isinstance(node, AbstractSetOperator):
        node.table1_reference = _selection_access_helper(node.table1_reference, selection_function)
        node.table2_reference = _selection_access_helper(node.table2_reference, selection_function)
    elif isinstance(node, OrderingOperator) or isinstance(node, Projection):
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

