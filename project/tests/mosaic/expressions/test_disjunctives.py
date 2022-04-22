from mosaic.expressions.column_expression import ColumnExpression
from mosaic.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.expressions.literal_expression import LiteralExpression
from mosaic.expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
import comparative_helper


def test_simple_disjunction():
    comparison_number = 25403
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(comparison_number))
    comparison_string = "Schopenhauer"
    comparative_operation_two = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.EQUAL, ColumnExpression("Name"))
    disjunctive = DisjunctiveExpression(
        [comparative_operation, comparative_operation_two])
    sum, _ = comparative_helper.evaluate(disjunctive)
    assert(sum == 2)


def test_nested_disjunction():

    comparison_string = "Aristoxenos"
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        "Name"), ComparativeOperator.EQUAL, LiteralExpression(comparison_string))
    comparison_string_two = "Theophrastos"
    comparative_operation_two = ComparativeOperationExpression(ColumnExpression(
        "Name"), ComparativeOperator.EQUAL, LiteralExpression(comparison_string_two))

    disjunction = DisjunctiveExpression(
        [comparative_operation, comparative_operation_two])

    comparison_number = 6
    comparative_operation_three = ComparativeOperationExpression(ColumnExpression(
        "Semester"), ComparativeOperator.EQUAL, LiteralExpression(comparison_number))
    comparison_number_two = 0
    comparative_operation_four = ComparativeOperationExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.GREATER, LiteralExpression(comparison_number_two))

    conjunction = ConjunctiveExpression(
        [comparative_operation_three, comparative_operation_four])

    nested_disjunction = DisjunctiveExpression([disjunction, conjunction])

    sum, _ = comparative_helper.evaluate(nested_disjunction)
    assert(sum == 3)


def test_explain():
    comparison_string = "1234"
    column = "Name"
    comparative_operation_two = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.EQUAL, ColumnExpression("Name"))
    comparison_number = 3256
    column2 = "MatrNr"
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        column2), ComparativeOperator.GREATER, LiteralExpression(comparison_number))
    conjunctive = ConjunctiveExpression(
        [comparative_operation, comparative_operation_two])

    comparison_string2 = "Something"
    comparative_operation_three = ComparativeOperationExpression(ColumnExpression(
        column), ComparativeOperator.EQUAL, LiteralExpression(comparison_string2))

    disjunctive = DisjunctiveExpression(
        [comparative_operation_three, conjunctive])

    assert str(
        disjunctive) == f'(({column} = "{comparison_string2}") or (({column2} > {comparison_number}) and ("{comparison_string}" = {column})))'
