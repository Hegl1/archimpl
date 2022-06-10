from copy import deepcopy
from mosaic.compiler.expressions.abstract_computation_expression import AbstractComputationExpression
from mosaic.compiler.operators import hash_join
from mosaic.compiler.operators.hash_join import HashJoin

from mosaic.table_service import AmbiguousColumnException, Schema, TableIndexException, index_exists
from .abstract_compile_node import AbstractCompileNode
from .expressions.abstract_expression import AbstractExpression
from .expressions.column_expression import ColumnExpression
from .expressions.conjunctive_expression import ConjunctiveExpression
from .expressions.disjunctive_expression import DisjunctiveExpression
from .expressions.comparative_expression import ComparativeExpression, ComparativeOperator
from .expressions.arithmetic_expression import ArithmeticExpression
from .expressions.literal_expression import LiteralExpression
from .operators.abstract_operator import AbstractOperator
from .operators.selection import Selection
from .operators.explain import Explain
from .operators.hash_distinct import HashDistinct
from .operators.abstract_join import AbstractJoin, JoinConditionNotSupportedException, JoinType, \
    JoinTypeNotSupportedException
from .operators.ordering import Ordering
from .operators.projection import Projection
from .operators.set_operators import AbstractSetOperator
from .operators.hash_aggregate import HashAggregate
from .operators.table_scan import TableScan


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

    execution_plan = _node_access_helper(
        execution_plan, _split_selections, Selection)
    execution_plan = _node_access_helper(
        execution_plan, _selection_push_down(), Selection)
    execution_plan = _node_access_helper(
        execution_plan, _apply_index_seek, Selection)
    execution_plan = _node_access_helper(
        execution_plan, _join_selections, Selection)

    # replace nested-loops-joins by best replacement join
    execution_plan = _node_access_helper(
        execution_plan, _select_optimal_join, AbstractJoin)

    return execution_plan


def _select_optimal_join(join: AbstractJoin):

    optimal_join = join

    try:
        optimal_join = HashJoin(join.left_node, join.right_node,
                                join.join_type, join.condition, join.is_natural)
    except (JoinTypeNotSupportedException, JoinConditionNotSupportedException):
        optimal_join = join

    optimal_join.left_node = _node_access_helper(
        optimal_join.left_node, _select_optimal_join, AbstractJoin)
    optimal_join.right_node = _node_access_helper(
        optimal_join.right_node, _select_optimal_join, AbstractJoin)
    return optimal_join


def _node_access_helper(node: AbstractCompileNode, function, searched_node_class):
    """
    Helper function to access the nodes of the specified class recursively in the given node.
    If the current node is not of the specified class, its child-operators are replaced by the
    return value of the recursive call with the child-operator.
    If the current node is of the specified class, the given function is called with the node
    as parameter and it's return value is returned.
    """
    if isinstance(node, searched_node_class):
        node = function(node)
    elif isinstance(node, Explain):
        node.node = _node_access_helper(
            node.node, function, searched_node_class)
    elif isinstance(node, (AbstractJoin, AbstractSetOperator)):
        node.left_node = _node_access_helper(
            node.left_node, function, searched_node_class)
        node.right_node = _node_access_helper(
            node.right_node, function, searched_node_class)
    elif isinstance(node, (Ordering, Projection, HashAggregate, HashDistinct, Selection)):
        node.node = _node_access_helper(
            node.node, function, searched_node_class)

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

        selection.node = Selection(
            selection.node, conjunctive_expression)
        selection.condition = condition

    selection.node = _node_access_helper(
        selection.node, _split_selections, Selection)
    return selection


def _join_selections(selection: Selection):
    """
    Joins consecutive selections for the given selection.
    The resulting joined selection is returned (as conjunctive)
    """
    if isinstance(selection.node, Selection):
        child_selection = selection.node

        if isinstance(selection.condition, ConjunctiveExpression):
            conditions = selection.condition.conditions
        else:
            conditions = [selection.condition]

        if isinstance(child_selection.condition, ConjunctiveExpression):
            conditions = conditions + child_selection.condition.conditions
        else:
            conditions.append(child_selection.condition)

        selection.condition = ConjunctiveExpression(conditions)
        selection.node = child_selection.node

        selection = _join_selections(selection)

    selection.node = _node_access_helper(
        selection.node, _join_selections, Selection)

    return selection


def _selection_push_down(pushed_down_selections=set()):
    """
    Pushes down the given selection as far as possible and returns the top-level node
    that should replace the selection.
    Helper function that returns the effective selection-push-down function to be able
    to keep track of the pushed-down selections (argument)
    """
    def func(selection: Selection):
        if selection in pushed_down_selections:
            # ignore pushed down selections
            return selection

        child_node = selection.node
        node = selection

        if isinstance(child_node, (HashDistinct, Ordering)):
            node = child_node

            selection.node = child_node.node
            child_node.node = _node_access_helper(
                selection, _selection_push_down(pushed_down_selections), Selection)
        elif isinstance(child_node, Selection):
            node = child_node

            selection.node = child_node.node
            child_node.node = _node_access_helper(
                selection, _selection_push_down(pushed_down_selections), Selection)

            node = _node_access_helper(node, _selection_push_down(
                pushed_down_selections), Selection)
        elif isinstance(child_node, Projection):
            node = _selection_push_through_projection(
                selection, child_node, pushed_down_selections)
        elif isinstance(child_node, AbstractJoin):
            node = _selection_push_through_join_operator(
                selection, child_node, pushed_down_selections)
        elif isinstance(child_node, AbstractSetOperator):
            node = _selection_push_through_set_operator(
                selection, child_node, pushed_down_selections)

        if node == selection:
            # selection can not be pushed down any further -> add to pushed_down_selections and do recursive call
            pushed_down_selections.add(node)
            node.node = _node_access_helper(
                node.node, _selection_push_down(pushed_down_selections), Selection)

        return node

    return func


def _selection_push_through_projection(selection: Selection, projection: Projection, pushed_down_selections=set()):
    """
    Pushes the selection through the given projection by renaming the referenced columns
    in the selection and swapping the both operators.
    Returns the top-level node that contains the selection as child or the selection if
    push-through was not possible
    """
    projection_schema = projection.get_schema()
    child_schema = projection.node.get_schema()

    selection_columns = _get_condition_columns(selection.condition)
    fqn_selection_columns = [projection_schema.get_fully_qualified_column_name(
        column) for column in selection_columns]
    selection_columns_replacements = {}

    found_columns = set()

    for alias, column_reference in projection.column_references:
        if alias is not None:
            # column-reference has an alias

            if alias in selection_columns:
                # alias is contained in the selection-columns

                found_columns.add(alias)

                if isinstance(column_reference, ColumnExpression):
                    # columns-reference is a column expression
                    # -> replace the alias-reference by the actual column-name

                    selection_columns_replacements[alias] = column_reference.get_result(
                    )
                else:
                    # can't be pushed down, return selection
                    # -> e.g. ArithmeticOperationExpression or LiteralExpression (could be optimized in some cases though)
                    return selection
        else:
            # no alias

            if isinstance(column_reference, ColumnExpression):
                # column-reference is a column expression (like above other types are not allowed)

                column = column_reference.get_result()

                if column in selection_columns:
                    # column found in selection-columns

                    found_columns.add(
                        projection_schema.get_fully_qualified_column_name(column))
                elif column in fqn_selection_columns:
                    # column found in fully-qualified-selection-columns
                    # -> replace simple column name by fully-qualified-name

                    simple_column = child_schema.get_simple_column_name(column)
                    if simple_column in selection_columns:
                        selection_columns_replacements[simple_column] = column
                        found_columns.add(column)
                    else:
                        raise Exception(
                            "Unexpected exception in selection push through projection")
                else:
                    # check if fully-qualified-column name is contained in fully-qualified-selection-columns

                    fqn_column = projection_schema.get_fully_qualified_column_name(
                        column)

                    if fqn_column in fqn_selection_columns:
                        # -> replace by fully-qualified-column-name

                        selection_columns_replacements[column] = fqn_column
                        found_columns.add(fqn_column)

    if set(fqn_selection_columns) != found_columns:
        # can't be pushed down, return selection
        # will throw an exception on execution anyways
        # -> e.g. a selected column does not exist
        return selection

    _replace_condition_columns(
        selection.condition, selection_columns_replacements)

    selection.node = projection.node
    projection.node = _node_access_helper(
        selection, _selection_push_down(pushed_down_selections), Selection)

    return projection


def _selection_push_through_join_operator(selection: Selection, join_operator: AbstractJoin, pushed_down_selections=set()):
    """
    Pushes the selection through the given join-operator by checking that the referenced columns
    are fully-covered in only one side of the join and pushing it through there (see rules on slides).
    Returns the top-level node that should replace the selection.
    """
    node = selection

    selection_columns = _get_condition_columns(selection.condition)

    table1_schema = join_operator.left_node.get_schema()
    table2_schema = join_operator.right_node.get_schema()

    is_fully_covered_table1 = False
    is_fully_covered_table2 = False
    table1_selection = selection
    table2_selection = selection

    if join_operator.is_natural and join_operator.join_type != JoinType.CROSS:
        # natural join: if condition is fully covered in both -> push selection to both children

        join_schema = join_operator.get_schema()
        # simple_selection_columns = [join_schema.get_simple_column_name(column) for column in selection_columns]

        if _are_columns_fully_covered_in_both_schemas(selection_columns, table1_schema, table2_schema):
            is_fully_covered_table1 = True
            is_fully_covered_table2 = True

            # replace all column-reference for the right child
            table_2_selection_columns_replacements = {}

            for column in selection_columns:
                table_2_selection_columns_replacements[column] = table2_schema.get_fully_qualified_column_name(
                    join_schema.get_simple_column_name(column))

            table1_selection = Selection(
                join_operator.left_node, selection.condition)
            table2_selection = Selection(
                join_operator.right_node, deepcopy(selection.condition))
            _replace_condition_columns(
                table2_selection.condition, table_2_selection_columns_replacements)

    if not is_fully_covered_table1 and not is_fully_covered_table2:
        # check if condition is fully covered in only one child

        is_fully_covered_table1 = _are_columns_fully_covered_in_first_schema(
            selection_columns, table1_schema, table2_schema)
        is_fully_covered_table2 = _are_columns_fully_covered_in_first_schema(
            selection_columns, table2_schema, table1_schema)

    if is_fully_covered_table1:
        # push onto left table-reference

        node = join_operator

        table1_selection.node = join_operator.left_node
        join_operator.left_node = _node_access_helper(
            table1_selection, _selection_push_down(pushed_down_selections), Selection)

    if is_fully_covered_table2:
        # push onto right table-reference

        node = join_operator

        table2_selection.node = join_operator.right_node
        join_operator.right_node = _node_access_helper(
            table2_selection, _selection_push_down(pushed_down_selections), Selection)

    return node


def _selection_push_through_set_operator(selection: Selection, set_operator: AbstractSetOperator, pushed_down_selections=set()):
    """
    Pushes the selection through the given set-operator by pushing it through the left relation
    and renaming the columns and pushing it through the right relation.
    Returns the set-operator that should replace the selection
    """
    table1_schema = set_operator.left_node.get_schema()
    table2_schema = set_operator.right_node.get_schema()

    selection_columns = _get_condition_columns(selection.condition)
    table_2_selection_columns_replacements = {}

    # replace all column-reference for the right child
    for column in selection_columns:
        simple_name = table1_schema.get_simple_column_name(column)
        table_2_selection_columns_replacements[column] = table2_schema.get_fully_qualified_column_name(
            simple_name)

    table1_selection = Selection(
        set_operator.left_node, selection.condition)
    table2_selection = Selection(
        set_operator.right_node, deepcopy(selection.condition))
    _replace_condition_columns(
        table2_selection.condition, table_2_selection_columns_replacements)

    # push to left and right child
    set_operator.left_node = _node_access_helper(
        table1_selection, _selection_push_down(pushed_down_selections), Selection)
    set_operator.right_node = _node_access_helper(
        table2_selection, _selection_push_down(pushed_down_selections), Selection)

    return set_operator


def _get_condition_columns(expression):
    """
    Returns the columns referenced in the given expression (recursively).
    """
    columns = []

    if isinstance(expression, (ComparativeExpression, ArithmeticExpression)):
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
    if isinstance(expression, (ComparativeExpression, ArithmeticExpression)):
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
            schema2.get_column_index(schema1.get_simple_column_name(column))
        except TableIndexException:
            return False
    return True


def _apply_index_seek(selection: Selection):
    """
    Checks for the given selection and direct child selections if they can be used for an index seek. Merges one of
    the selections with a subjacent table scan to produce an index seek if possible. Returns the top-level node that
    should replace the selection.
    """
    potential_candidate_selections = set()

    parent = None
    node = selection
    while isinstance(node, Selection):
        if _is_condition_suitable_for_index_seek(node.condition):
            potential_candidate_selections.add((node, parent))
        parent = node
        node = node.node

    if isinstance(node, TableScan):
        table_name = node.table_name
        candidate_selections = []
        for potential_candidate_selection, parent in potential_candidate_selections:
            column_name = _get_simple_column_name_from_condition_for_index_seek(
                potential_candidate_selection.condition)
            if index_exists(table_name, column_name):
                candidate_selections.append((potential_candidate_selection, parent))
        if candidate_selections:
            best_selection, bs_parent = _choose_best_candidate_selection_for_index_seek(candidate_selections)
            # TODO merge with one of the candidate selections
            # TODO handle rebuilding rest of branch correctly
    else:
        node = _node_access_helper(node, _apply_index_seek, Selection)

    return selection


def _is_condition_suitable_for_index_seek(condition):
    """
    Checks whether the condition is a simple comparative that checks the equality between a column and a literal.
    """
    if isinstance(condition, ComparativeExpression) and condition.operator == ComparativeOperator.EQUAL:
        column_eq_literal = isinstance(condition.left, ColumnExpression) and isinstance(condition.right, LiteralExpression)
        literal_eq_column = isinstance(condition.left, LiteralExpression) and isinstance(condition.right, ColumnExpression)
        return column_eq_literal or literal_eq_column
    return False


def _get_simple_column_name_from_condition_for_index_seek(condition):
    """
    Retrieves the column name from a condition that is suitable for an index seek.
    """
    if isinstance(condition.left, ColumnExpression):
        column_name = condition.left.get_result()
    else:
        column_name = condition.right.get_result()
    if "." in column_name:
        column_name = column_name.split(".")[1]  # remove FQN
    return column_name


def _choose_best_candidate_selection_for_index_seek(candidate_selections):
    # TODO optional improvement: choose best candidate based on more useful criteria
    return candidate_selections[0]
