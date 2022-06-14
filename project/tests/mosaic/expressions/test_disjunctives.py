from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, ComparativeOperator
import comparative_helper


def test_simple_disjunction():
    comparison_number = 25403
    comparative_operation = ComparativeExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(comparison_number))
    comparison_string = "Schopenhauer"
    comparative_operation_two = ComparativeExpression(LiteralExpression(
        comparison_string), ComparativeOperator.EQUAL, ColumnExpression("Name"))
    disjunctive = DisjunctiveExpression(
        [comparative_operation, comparative_operation_two])
    sum, _ = comparative_helper.evaluate(disjunctive)
    assert(sum == 2)


def test_nested_disjunction():

    comparison_string = "Aristoxenos"
    comparative_operation = ComparativeExpression(ColumnExpression(
        "Name"), ComparativeOperator.EQUAL, LiteralExpression(comparison_string))
    comparison_string_two = "Theophrastos"
    comparative_operation_two = ComparativeExpression(ColumnExpression(
        "Name"), ComparativeOperator.EQUAL, LiteralExpression(comparison_string_two))

    disjunction = DisjunctiveExpression(
        [comparative_operation, comparative_operation_two])

    comparison_number = 6
    comparative_operation_three = ComparativeExpression(ColumnExpression(
        "Semester"), ComparativeOperator.EQUAL, LiteralExpression(comparison_number))
    comparison_number_two = 0
    comparative_operation_four = ComparativeExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.GREATER, LiteralExpression(comparison_number_two))

    conjunction = ConjunctiveExpression(
        [comparative_operation_three, comparative_operation_four])

    nested_disjunction = DisjunctiveExpression([disjunction, conjunction])

    sum, _ = comparative_helper.evaluate(nested_disjunction)
    assert(sum == 3)


def test_explain():
    comparison_string = "1234"
    column = "Name"
    comparative_operation_two = ComparativeExpression(LiteralExpression(
        comparison_string), ComparativeOperator.EQUAL, ColumnExpression("Name"))
    comparison_number = 3256
    column2 = "MatrNr"
    comparative_operation = ComparativeExpression(ColumnExpression(
        column2), ComparativeOperator.GREATER, LiteralExpression(comparison_number))
    conjunctive = ConjunctiveExpression(
        [comparative_operation, comparative_operation_two])

    comparison_string2 = "Something"
    comparative_operation_three = ComparativeExpression(ColumnExpression(
        column), ComparativeOperator.EQUAL, LiteralExpression(comparison_string2))

    disjunctive = DisjunctiveExpression(
        [comparative_operation_three, conjunctive])

    assert str(
        disjunctive) == f'(({column} = "{comparison_string2}") OR (({column2} > {comparison_number}) AND ("{comparison_string}" = {column})))'


def test_disjunction_with_literal():
    table = comparative_helper.retrieve_table("studenten")

    conjunction = DisjunctiveExpression([
        LiteralExpression(1),
        LiteralExpression(0)
    ])

    for i in range(len(table)):
        assert conjunction.get_result(table, i) == 1
