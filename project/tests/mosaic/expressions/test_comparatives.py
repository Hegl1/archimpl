from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator, IncompatibleOperandTypesException
import pytest
import comparative_helper


def test_nested_comparisons():
    comparison_number = 24002
    comparative_operation = ComparativeOperationExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(
        comparison_number))

    comparison_number2 = 25403
    comparative_operation2 = ComparativeOperationExpression(ColumnExpression(
        "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(
        comparison_number2))

    nested_compare = ComparativeOperationExpression(
        comparative_operation, ComparativeOperator.EQUAL, comparative_operation2)
    print(nested_compare)
    sum, entries = comparative_helper.evaluate(nested_compare)
    assert (sum == (entries - 2))


def test_smaller_numbers():
    comparison_number = 25403
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_number), ComparativeOperator.SMALLER, ColumnExpression("MatrNr"))
    sum, entries = comparative_helper.evaluate(comparative_operation)
    assert(sum == (entries - 2))


def test_smaller_equal_numbers():
    comparison_number = 25403
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_number), ComparativeOperator.SMALLER_EQUAL, ColumnExpression("MatrNr"))
    sum, entries = comparative_helper.evaluate(comparative_operation)
    assert(sum == (entries - 1))


def test_greater_equal_numbers():
    comparison_number = 26120
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_number), ComparativeOperator.GREATER_EQUAL, ColumnExpression("MatrNr"))
    sum, __annotations__ = comparative_helper.evaluate(comparative_operation)
    assert(sum == 3)


def test_greater_numbers():
    comparison_number = 26120
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_number), ComparativeOperator.GREATER, ColumnExpression("MatrNr"))
    sum, _ = comparative_helper.evaluate(comparative_operation)
    assert(sum == 2)


def test_equal_numbers():
    comparison_number = 29120
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_number), ComparativeOperator.EQUAL, ColumnExpression("MatrNr"))
    sum, _ = comparative_helper.evaluate(comparative_operation)
    assert(sum == 1)


def test_not_equal_numbers():
    comparison_number = 29120
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_number), ComparativeOperator.NOT_EQUAL, ColumnExpression("MatrNr"))
    sum, entries = comparative_helper.evaluate(comparative_operation)
    assert(sum == (entries - 1))


def test_wrong_type_comparison():
    comparison_string = "hello"
    comparative_operation = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.GREATER, ColumnExpression("MatrNr"))
    table = comparative_helper.retrieve_table("studenten")
    with pytest.raises(IncompatibleOperandTypesException):
        comparative_operation.get_result(table=table, row_index=0)


def test_none_literal_comparison():
    comparative_operation = ComparativeOperationExpression(
        LiteralExpression(None), ComparativeOperator.GREATER, ColumnExpression("Name"))
    sum, _ = comparative_helper.evaluate(comparative_operation)
    assert(sum == 0)


def test_explain():
    comparison_string = "hello"
    expression = ComparativeOperationExpression(LiteralExpression(
        comparison_string), ComparativeOperator.GREATER, ColumnExpression("MatrNr"))
    assert str(expression) == f'("{comparison_string}" > MatrNr)'
