from mosaic.compiler.operators.explain import Explain
from mosaic.compiler.operators.table_scan_operator import TableScan
from mosaic.compiler.operators.hash_distinct_operator import HashDistinct
from mosaic.compiler.operators.ordering_operator import OrderingOperator
from mosaic.compiler.operators.selection_operator import Selection
from mosaic.compiler.operators.set_operators import Union
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.comparative_operation_expression import ComparativeOperationExpression, \
    ComparativeOperator
from mosaic.compiler.expressions.arithmetic_operation_expression import ArithmeticOperationExpression, \
    ArithmeticOperator


def test_simplify_arithmetic_to_literal():
    expression = ArithmeticOperationExpression(
        ArithmeticOperationExpression(
            LiteralExpression(2),
            ArithmeticOperator.ADD,
            LiteralExpression(4)
        ),
        ArithmeticOperator.TIMES,
        LiteralExpression(6)
    )

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 36


def test_simplify_arithmetic():
    expression = ArithmeticOperationExpression(
        ArithmeticOperationExpression(
            LiteralExpression(2),
            ArithmeticOperator.ADD,
            LiteralExpression(4)
        ),
        ArithmeticOperator.ADD,
        ColumnExpression("MatrNr")
    )

    expression = expression.simplify()

    assert isinstance(expression, ArithmeticOperationExpression)
    assert isinstance(expression.left, LiteralExpression) 
    assert expression.left.get_result() == 6
    assert expression.operator == ArithmeticOperator.ADD
    assert isinstance(expression.right, ColumnExpression)


def test_simplify_comparative_to_literal():
    expression = ComparativeOperationExpression(LiteralExpression("test"), ComparativeOperator.EQUAL, LiteralExpression("test"))

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 1


def test_simplify_comparative():
    expression = ComparativeOperationExpression(
        ComparativeOperationExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(51135)),
        ComparativeOperator.EQUAL,
        ComparativeOperationExpression(LiteralExpression(2), ComparativeOperator.EQUAL, LiteralExpression(2))
    )

    expression = expression.simplify()

    assert isinstance(expression, ComparativeOperationExpression)
    assert isinstance(expression.left, ComparativeOperationExpression) 
    assert expression.operator == ComparativeOperator.EQUAL
    assert isinstance(expression.right, LiteralExpression)
    assert expression.right.get_result() == 1


def test_simplify_conjunctive_to_literal():
    expression = ConjunctiveExpression([LiteralExpression(1), LiteralExpression(1)])

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 1

    expression = ConjunctiveExpression([LiteralExpression(0), LiteralExpression(1)])

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 0


def test_simplify_conjunctive_to_comparative():
    expression = ConjunctiveExpression([LiteralExpression(1), ComparativeOperationExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr"))])

    expression = expression.simplify()

    assert isinstance(expression, ComparativeOperationExpression)
    assert isinstance(expression.left, LiteralExpression) 
    assert expression.left.get_result() == 123
    assert expression.operator == ComparativeOperator.EQUAL
    assert isinstance(expression.right, ColumnExpression)


def test_simplify_conjunctive():
    expression = ConjunctiveExpression([
        LiteralExpression(1),
        ComparativeOperationExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
        ComparativeOperationExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("VorlNr"))
    ])

    expression = expression.simplify()

    assert len(expression.conditions) == 2


def test_simplify_disjunctive_to_literal():
    expression = DisjunctiveExpression([LiteralExpression(1), LiteralExpression(0)])

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 1

    expression = DisjunctiveExpression([LiteralExpression(0), LiteralExpression(0)])

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 0


def test_simplify_disjunctive_to_comparative():
    expression = DisjunctiveExpression([LiteralExpression(0), ComparativeOperationExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr"))])

    expression = expression.simplify()

    assert isinstance(expression, ComparativeOperationExpression)
    assert isinstance(expression.left, LiteralExpression) 
    assert expression.left.get_result() == 123
    assert expression.operator == ComparativeOperator.EQUAL
    assert isinstance(expression.right, ColumnExpression)


def test_simplify_disjunctive():
    expression = DisjunctiveExpression([
        LiteralExpression(0),
        ComparativeOperationExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
        ComparativeOperationExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("VorlNr"))
    ])

    expression = expression.simplify()

    assert len(expression.conditions) == 2


def test_simplify_explain():
    explain = Explain(HashDistinct(TableScan("hoeren")))

    explain = explain.simplify()


def test_simplify_ordering():
    ordering = OrderingOperator([ColumnExpression("MatrNr")], TableScan("hoeren"))

    ordering = ordering.simplify()

    assert isinstance(ordering, OrderingOperator)
    assert len(ordering.column_list) == 1


def test_simplify_selection_to_table():
    selection = Selection(TableScan("hoeren"), ComparativeOperationExpression(LiteralExpression(1), ComparativeOperator.EQUAL, LiteralExpression(1)))

    selection = selection.simplify()

    assert isinstance(selection, TableScan)


def test_simplify_selection():
    selection = Selection(
        TableScan("hoeren"),
        ComparativeOperationExpression(
            ArithmeticOperationExpression(LiteralExpression(1500), ArithmeticOperator.ADD, LiteralExpression(1000)),
            ComparativeOperator.SMALLER,
            ColumnExpression("MatrNr")
        )
    )

    selection = selection.simplify()

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ComparativeOperationExpression)
    assert isinstance(selection.condition.left, LiteralExpression)
    assert selection.condition.left.get_result() == 2500


def test_simplify_union():
    union = Union(
        TableScan("hoeren"),
        Selection(
            TableScan("hoeren"),
            LiteralExpression(1)
        )
    )

    union = union.simplify()

    assert isinstance(union, Union)
    assert isinstance(union.table2_reference, TableScan)
    assert union.table2_reference.table_name == "hoeren"