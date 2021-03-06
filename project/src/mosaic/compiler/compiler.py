"""This file contains the implementation of our query compiler.

The compiler takes the AST produced by the parser and turns it into an
execution plan.
"""

from parsimonious.nodes import NodeVisitor
from mosaic.compiler.compiler_exception import CompilerException
from mosaic.compiler.expressions.arithmetic_expression import ArithmeticExpression, \
    ArithmeticOperator
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, \
    ComparativeOperator
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.operators.explain import Explain
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.operators.hash_distinct import HashDistinct
from mosaic.compiler.operators.nested_loops_join import NestedLoopsJoin
from mosaic.compiler.operators.abstract_join import JoinType
from mosaic.compiler.operators.ordering import Ordering
from mosaic.compiler.operators.projection import Projection
from mosaic.compiler.operators.selection import Selection
from mosaic.compiler.operators.set_operators import SetOperationType, Union, Intersect, Except
from mosaic.compiler.operators.table_scan import TableScan
from mosaic.compiler.operators.hash_aggregate import AggregateFunction
from mosaic.compiler.operators.hash_aggregate import HashAggregate


###########################################################
# Visitor for traversing the AST.
###########################################################

class ASTVisitor(NodeVisitor):
    """Our visitor implementation."""

    # We define QueryExecutionError as an unwrapped exception,
    # so that parsimonious does not wrap it inside a VisitationError.
    unwrapped_exceptions = (CompilerException,)

    ####################
    # Literals
    ####################
    def visit_int_literal(self, node, visited_children):
        return LiteralExpression(int(node.text.strip()))

    def visit_float_literal(self, node, visited_children):
        return LiteralExpression(float(node.text.strip()))

    def visit_varchar_literal(self, node, visited_children):
        return LiteralExpression(node.text.strip().strip("\""))

    def visit_null_literal(self, node, visited_children):
        return LiteralExpression(None)

    def visit_literal(self, node, visited_children):
        return visited_children[0]

    ####################
    # Operators
    ####################
    def visit_comparison_operator(self, node, visited_children):
        return node.text.strip()

    def visit_addition_operator(self, node, visited_children):
        return node.text.strip()

    def visit_multiplication_operator(self, node, visited_children):
        return node.text.strip()

    ####################
    # Expressions
    ####################
    def visit_parens(self, node, visited_children):
        expression = visited_children[2]

        return expression

    def visit_term(self, node, visited_children):
        term = visited_children[0]

        return term

    def visit_multiplicative(self, node, visited_children):
        operator = visited_children[0]
        right = visited_children[2]

        return (operator, right)

    def visit_multiplicative_term(self, node, visited_children):
        if len(visited_children[1]) > 0:
            left = visited_children[0]

            for operator, right in visited_children[1]:
                if operator == '*':
                    left = ArithmeticExpression(left,
                                                ArithmeticOperator.TIMES,
                                                right)
                    break
                elif operator == '/':
                    left = ArithmeticExpression(left,
                                                ArithmeticOperator.DIVIDE,
                                                right)
                    break

            return left
        else:
            term = visited_children[0]
            return term

    def visit_additive(self, node, visited_children):
        operator = visited_children[0]
        right = visited_children[2]

        return (operator, right)

    def visit_additive_term(self, node, visited_children):
        if len(visited_children[1]) > 0:
            left = visited_children[0]

            for operator, right in visited_children[1]:
                if operator == '+':
                    left = ArithmeticExpression(left,
                                                ArithmeticOperator.ADD,
                                                right)
                    break
                elif operator == '-':
                    left = ArithmeticExpression(left,
                                                ArithmeticOperator.SUBTRACT,
                                                right)
                    break

            return left
        else:
            term = visited_children[0]
            return term

    def visit_comparative(self, node, visited_children):
        operator = visited_children[0]
        right = visited_children[2]

        return (operator, right)

    def visit_comparative_term(self, node, visited_children):
        if len(visited_children[1]) > 0:
            left = visited_children[0]

            for operator, right in visited_children[1]:
                if operator == '=':
                    left = ComparativeExpression(left,
                                                 ComparativeOperator.EQUAL,
                                                 right)
                    break
                elif operator == '!=':
                    left = ComparativeExpression(left,
                                                 ComparativeOperator.NOT_EQUAL,
                                                 right)
                    break
                elif operator == '<':
                    left = ComparativeExpression(left,
                                                 ComparativeOperator.SMALLER,
                                                 right)
                    break
                elif operator == '<=':
                    left = ComparativeExpression(left,
                                                 ComparativeOperator.SMALLER_EQUAL,
                                                 right)
                    break
                elif operator == '>':
                    left = ComparativeExpression(left,
                                                 ComparativeOperator.GREATER,
                                                 right)
                    break
                elif operator == '>=':
                    left = ComparativeExpression(left,
                                                 ComparativeOperator.GREATER_EQUAL,
                                                 right)
                    break

            return left
        else:
            term = visited_children[0]
            return term

    def visit_conjunctive(self, node, visited_children):
        term = visited_children[2]
        return term

    def visit_conjunctive_term(self, node, visited_children):
        if len(visited_children[1]) > 0:
            conjunctive_list = []
            conjunctive_list.append(visited_children[0])
            conjunctive_list += visited_children[1]
            return ConjunctiveExpression(conjunctive_list)
        else:
            term = visited_children[0]
            return term

    def visit_disjunctive(self, node, visited_children):
        term = visited_children[2]
        return term

    def visit_expression(self, node, visited_children):
        if len(visited_children[1]) > 0:
            disjunctive_list = []
            disjunctive_list.append(visited_children[0])
            disjunctive_list += visited_children[1]
            return DisjunctiveExpression(disjunctive_list)
        else:
            term = visited_children[0]
            return term

    ####################
    # References
    ####################
    def visit_name(self, node, visited_children):
        return node.text.strip()

    def visit_column_name(self, node, visited_children):
        return ColumnExpression(node.text.strip())

    def visit_table_name(self, node, visited_children):
        return node.text.strip()

    def visit_column_reference(self, node, visited_children):
        # If visited_children is a string, it is a simple column reference.
        if type(visited_children[0]) == str:
            return (None, ColumnExpression(visited_children[0]))
        # If it is a list, it is an aliased reference.
        elif type(visited_children[0]) == list:
            return (visited_children[0][0], visited_children[0][3])

    def visit_column_list(self, node, visited_children):
        # If the child is a tuple, we have just a single column.
        if type(visited_children[0]) == tuple:
            return [visited_children[0]]
        # If the children are a list, we have multiple names.
        elif type(visited_children[0]) == list:
            columns = [visited_children[0][0]] + visited_children[0][2]
            return columns

    def visit_simple_column_list(self, node, visited_children):
        # If the children are a list, we have multiple names.
        if type(visited_children[0]) == list:
            columns = [visited_children[0][0]] + visited_children[0][2]
            return columns
        # Otherwise, we have just a single column.
        else:
            return [visited_children[0]]

    def visit_aggregate_list(self, node, visited_children):
        return visited_children

    def visit_aggregate_column(self, node, visited_children):
        name = visited_children[0]
        aggregate_function = visited_children[3]
        expression = visited_children[7]

        return (name, aggregate_function, expression)

    ####################
    # Commands
    ####################
    def visit_projection(self, node, visited_children):
        # Check if DISTINCT was specified. Depending on that, different
        # children contain the information we need.
        if len(visited_children[0]) == 6:
            table_reference = visited_children[0][5]
            column_expressions = visited_children[0][4]

            return HashDistinct(Projection(table_reference, column_expressions))
        else:
            table_reference = visited_children[0][3]
            column_expressions = visited_children[0][2]

            return Projection(table_reference, column_expressions)

    def visit_selection(self, node, visited_children):
        condition = visited_children[2]
        input_node = visited_children[3]

        return Selection(input_node, condition)

    def visit_aggregate_function(self, node, visited_children):
        function_name = node.text.strip().lower()
        if function_name == 'sum':
            return AggregateFunction.SUM
        elif function_name == 'avg':
            return AggregateFunction.AVG
        elif function_name == 'min':
            return AggregateFunction.MIN
        elif function_name == 'max':
            return AggregateFunction.MAX
        elif function_name == 'count':
            return AggregateFunction.COUNT

    def visit_grouping(self, node, visited_children):
        children = visited_children[0]

        # If there are seven children, we have group columns.
        if len(children) == 7:
            group_columns = children[2]
            aggregate_columns = children[5]
            input_node = children[6]

            return HashAggregate(input_node, group_columns, aggregate_columns)
        # Otherwise, we want to compute aggregates over the super group.
        else:
            aggregate_columns = children[4]
            input_node = children[5]

            return HashAggregate(input_node, [], aggregate_columns)

    def visit_ordering(self, node, visited_children):
        return Ordering(visited_children[3], visited_children[2])

    def visit_relation_reference(self, node, visited_children):
        # If there are two children, we have a simple reference.
        if len(visited_children[0]) == 3:
            return TableScan(visited_children[0][1])
        # Otherwise, we have an aliased reference.
        else:
            return TableScan(visited_children[0][1], visited_children[0][5])

    def visit_paren_query(self, node, visited_children):
        return visited_children[3]

    def visit_join_factor(self, node, visited_children):
        return visited_children[0]

    def visit_set_operator(self, node, visited_children):
        operator = node.text.strip()
        if operator == "union":
            return SetOperationType.UNION
        elif operator == "intersect":
            return SetOperationType.INTERSECT
        elif operator == "except":
            return SetOperationType.EXCEPT

    def visit_join_operator(self, node, visited_children):
        if node.text == "join":
            return JoinType.INNER
        else:
            return JoinType.LEFT_OUTER

    def visit_natural_join_operator(self, node, visited_children):
        if node.text == "natural join":
            return JoinType.INNER
        else:
            return JoinType.LEFT_OUTER

    def visit_cross_join_operator(self, node, visited_children):
        return JoinType.CROSS

    def visit_join(self, node, visited_children):
        # If there are three children, we have a join without a condition.
        if len(visited_children[0]) == 3:
            join_type = visited_children[0][0]
            condition = None
            right = visited_children[0][2]
        # Otherwise, we have a join with a condition.
        else:
            join_type = visited_children[0][0]
            condition = visited_children[0][2]
            right = visited_children[0][3]

        return join_type, condition, right

    def visit_set_factor(self, node, visited_children):
        if len(visited_children[1]) > 0:
            left = visited_children[0]

            for join_type, condition, right in visited_children[1]:
                left = NestedLoopsJoin(left,
                                       right,
                                       join_type,
                                       condition=condition,
                                       is_natural=(condition is None))

            return left
        else:
            term = visited_children[0]
            return term

    def visit_set_operation(self, node, visited_children):
        return visited_children[0], visited_children[2]

    def visit_query(self, node, visited_children):
        if len(visited_children[1]) > 0:
            left = visited_children[0]

            for operation_type, right in visited_children[1]:
                if operation_type == SetOperationType.UNION:
                    left = Union(left, right)
                elif operation_type == SetOperationType.INTERSECT:
                    left = Intersect(left, right)
                elif operation_type == SetOperationType.EXCEPT:
                    left = Except(left, right)

            return left
        else:
            term = visited_children[0]
            return term

    def visit_explain_command(self, node, visited_children):
        return Explain(visited_children[2])

    def visit_command(self, node, visited_children):
        return visited_children[0]

    ####################
    # Generic
    ####################
    def generic_visit(self, node, visited_children):
        return visited_children


# Compile

_visitor = ASTVisitor()


def compile(ast):
    return _visitor.visit(ast)
