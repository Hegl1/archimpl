from mosaic.compiler.operators.explain import Explain
from mosaic.compiler.operators.table_scan import TableScan
from mosaic.compiler.operators.hash_distinct import HashDistinct
from mosaic.compiler.operators.ordering import Ordering
from mosaic.compiler.operators.selection import Selection
from mosaic.compiler.operators.set_operators import Union
from mosaic.compiler.operators.nested_loops_join import NestedLoopsJoin
from mosaic.compiler.operators.abstract_join import JoinType
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, \
    ComparativeOperator
from mosaic.compiler.expressions.arithmetic_expression import ArithmeticExpression, \
    ArithmeticOperator


def test_simplify_arithmetic_to_literal():
    expression = ArithmeticExpression(
        ArithmeticExpression(
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
    expression = ArithmeticExpression(
        ArithmeticExpression(
            LiteralExpression(2),
            ArithmeticOperator.ADD,
            LiteralExpression(4)
        ),
        ArithmeticOperator.ADD,
        ColumnExpression("MatrNr")
    )

    expression = expression.simplify()

    assert isinstance(expression, ArithmeticExpression)
    assert isinstance(expression.left, LiteralExpression)
    assert expression.left.get_result() == 6
    assert expression.operator == ArithmeticOperator.ADD
    assert isinstance(expression.right, ColumnExpression)


def test_simplify_comparative_to_literal():
    expression = ComparativeExpression(LiteralExpression("test"), ComparativeOperator.EQUAL, LiteralExpression("test"))

    expression = expression.simplify()

    assert isinstance(expression, LiteralExpression)
    assert expression.get_result() == 1


def test_simplify_comparative():
    expression = ComparativeExpression(
        ComparativeExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(51135)),
        ComparativeOperator.EQUAL,
        ComparativeExpression(LiteralExpression(2), ComparativeOperator.EQUAL, LiteralExpression(2))
    )

    expression = expression.simplify()

    assert isinstance(expression, ComparativeExpression)
    assert isinstance(expression.left, ComparativeExpression)
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
    expression = ConjunctiveExpression([LiteralExpression(1),
                                        ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL,
                                                              ColumnExpression("MatrNr"))])

    expression = expression.simplify()

    assert isinstance(expression, ComparativeExpression)
    assert isinstance(expression.left, LiteralExpression)
    assert expression.left.get_result() == 123
    assert expression.operator == ComparativeOperator.EQUAL
    assert isinstance(expression.right, ColumnExpression)


def test_simplify_conjunctive():
    expression = ConjunctiveExpression([
        LiteralExpression(1),
        ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
        ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("VorlNr"))
    ])

    expression = expression.simplify()

    assert len(expression.conditions) == 2


def test_simplify_flatten_conjunctive():
    expression = ConjunctiveExpression([
        ConjunctiveExpression([
            ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
            ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("VorlNr"))
        ]),
        ComparativeExpression(LiteralExpression(124), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
    ])

    expression = expression.simplify()

    assert len(expression.conditions) == 3
    assert isinstance(expression.conditions[0], ComparativeExpression)
    assert isinstance(expression.conditions[1], ComparativeExpression)
    assert isinstance(expression.conditions[2], ComparativeExpression)


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
    expression = DisjunctiveExpression([LiteralExpression(0),
                                        ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL,
                                                              ColumnExpression("MatrNr"))])

    expression = expression.simplify()

    assert isinstance(expression, ComparativeExpression)
    assert isinstance(expression.left, LiteralExpression)
    assert expression.left.get_result() == 123
    assert expression.operator == ComparativeOperator.EQUAL
    assert isinstance(expression.right, ColumnExpression)


def test_simplify_disjunctive():
    expression = DisjunctiveExpression([
        LiteralExpression(0),
        ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
        ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("VorlNr"))
    ])

    expression = expression.simplify()

    assert len(expression.conditions) == 2


def test_simplify_flatten_disjunctive():
    expression = DisjunctiveExpression([
        DisjunctiveExpression([
            ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
            ComparativeExpression(LiteralExpression(123), ComparativeOperator.EQUAL, ColumnExpression("VorlNr"))
        ]),
        ComparativeExpression(LiteralExpression(124), ComparativeOperator.EQUAL, ColumnExpression("MatrNr")),
    ])

    expression = expression.simplify()

    assert len(expression.conditions) == 3
    assert isinstance(expression.conditions[0], ComparativeExpression)
    assert isinstance(expression.conditions[1], ComparativeExpression)
    assert isinstance(expression.conditions[2], ComparativeExpression)


def test_simplify_explain():
    explain = Explain(HashDistinct(TableScan("hoeren")))

    explain = explain.simplify()


def test_simplify_ordering():
    ordering = Ordering(TableScan("hoeren"), [ColumnExpression("MatrNr")])

    ordering = ordering.simplify()

    assert isinstance(ordering, Ordering)
    assert len(ordering.column_list) == 1


def test_simplify_selection_to_table():
    selection = Selection(TableScan("hoeren"),
                          ComparativeExpression(LiteralExpression(1), ComparativeOperator.EQUAL, LiteralExpression(1)))

    selection = selection.simplify()

    assert isinstance(selection, TableScan)


def test_simplify_selection():
    selection = Selection(
        TableScan("hoeren"),
        ComparativeExpression(
            ArithmeticExpression(LiteralExpression(1500), ArithmeticOperator.ADD, LiteralExpression(1000)),
            ComparativeOperator.SMALLER,
            ColumnExpression("MatrNr")
        )
    )

    selection = selection.simplify()

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ComparativeExpression)
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
    assert isinstance(union.right_node, TableScan)
    assert union.right_node.table_name == "hoeren"


def test_simplify_nested_loops_join():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("voraussetzen.Vorgaenger"))
    conjunctive = ConjunctiveExpression([comparative, LiteralExpression(1)])
    join = NestedLoopsJoin(table1, table2, JoinType.INNER, conjunctive, False)

    join = join.simplify()

    assert isinstance(join.condition, ComparativeExpression)
