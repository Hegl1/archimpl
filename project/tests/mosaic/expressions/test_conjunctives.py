from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
import comparative_helper


def test_simple_conjunction():
    comparison_number = 25403
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.GREATER, LiteralExpression(comparison_number))
    comparison_string = "T"
    comparative_operation_two = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.GREATER, ColumnExpression("Name"))
    conjunctive = ConjunctiveExpression(
        [comparative_operation, comparative_operation_two])
    sum, entries = comparative_helper.evaluate(conjunctive)
    assert(sum == (entries - 3))


def test_nested_conjunction():
    comparison_number = 28106
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.SMALLER_EQUAL, LiteralExpression(comparison_number))
    comparison_string = "T"
    comparative_operation_two = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.GREATER, ColumnExpression("Name"))
    conjunctive = ConjunctiveExpression(
        [comparative_operation, comparative_operation_two])
    name = "Carnap"
    comparative_operation_three = ComparativeOperationExpression(
        LiteralExpression(name), ComparativeOperator.EQUAL, ColumnExpression("Name"))
    nested = ConjunctiveExpression([comparative_operation_three, conjunctive])
    sum, _ = comparative_helper.evaluate(nested)
    assert(sum == 1)


def test_explain():
    comparison_string = "fun"
    column = "Name"
    comparative_operation_two = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.EQUAL, ColumnExpression("Name"))
    comparison_number = 25403
    column2 = "MatrNr"
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        column2), ComparativeOperator.GREATER, LiteralExpression(comparison_number))
    conjunctive = ConjunctiveExpression(
        [comparative_operation, comparative_operation_two])
    assert str(
        conjunctive) == f'(({column2} > {comparison_number}) and ("{comparison_string}" = {column}))'
