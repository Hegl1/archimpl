import pytest
from mosaic import table_service
from mosaic.compiler.operators.abstract_join import JoinType, JoinTypeNotSupportedException, \
    JoinConditionNotSupportedException, ErrorInJoinConditionException
from mosaic.compiler.operators.merge_join import MergeJoin, TableNotSortedException
from mosaic.compiler.operators.ordering import Ordering
from mosaic.compiler.operators.table_scan import TableScan
from mosaic.compiler.operators.projection import Projection
from mosaic.compiler.operators.explain import Explain
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, ComparativeOperator
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_mergejoin_comparative():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger")])
    comparative = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("voraussetzen.Vorgaenger"))

    print(table1)
    join = MergeJoin(table1, table2, JoinType.INNER, comparative, False)
    result = join.get_result()

    assert len(result) == 10
    assert result.schema.column_names == ["vorlesungen.VorlNr", "vorlesungen.Titel",
                                          "vorlesungen.SWS", "vorlesungen.gelesenVon",
                                          "voraussetzen.Vorgaenger", "voraussetzen.Nachfolger"]
    assert [5001, "Grundzuege", 4, 2137, 5001, 5041] in result.records
    assert [5043, "Erkenntnistheorie", 3, 2126, 5043, 5052] in result.records
    assert [5049] not in [res[0] for res in result.records]
    assert ["Der Wiener"] not in [res[1] for res in result.records]


def test_mergejoin_conjunctive():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr"), ColumnExpression("gelesenVon")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger"), ColumnExpression("Nachfolger")])
    comparative1 = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                         ComparativeOperator.EQUAL,
                                         ColumnExpression("voraussetzen.Vorgaenger"))
    comparative2 = ComparativeExpression(ColumnExpression("vorlesungen.gelesenVon"),
                                         ComparativeOperator.EQUAL,
                                         ColumnExpression("voraussetzen.Nachfolger"))
    conjunctive = ConjunctiveExpression([comparative1, comparative2])

    join = MergeJoin(table1, table2, JoinType.INNER, conjunctive, False)
    result = join.get_result()

    assert len(result) == 1
    assert len(result.schema.column_names) == 6
    assert [5001, "Grundzuege", 4, 5041, 5001, 5041] in result.records


def test_mergejoin_left_comparative():
    table1 = Ordering(TableScan("professoren"), [ColumnExpression("PersNr")])
    table2 = Ordering(Projection(TableScan("assistenten"), [(None, ColumnExpression("Boss"))]),
                      [ColumnExpression("Boss")])
    comparative = ComparativeExpression(ColumnExpression("professoren.PersNr"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("assistenten.Boss"))

    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, comparative, False)
    result = join.get_result()

    assert len(result) == 9
    assert [2125, 'Sokrates', 'C4', '226', 2125] in result.records
    assert [2134, "Augustinus", "C3", '309', 2134] in result.records
    assert [2137, "Kant", "C4", '7', None] in result.records


def test_mergejoin_left_conjunctive():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr"), ColumnExpression("gelesenVon")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger"), ColumnExpression("Nachfolger")])
    comparative1 = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                         ComparativeOperator.EQUAL,
                                         ColumnExpression("voraussetzen.Vorgaenger"))
    comparative2 = ComparativeExpression(ColumnExpression("vorlesungen.gelesenVon"),
                                         ComparativeOperator.EQUAL,
                                         ColumnExpression("voraussetzen.Nachfolger"))
    conjunctive = ConjunctiveExpression([comparative1, comparative2])

    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, conjunctive, False)
    result = join.get_result()

    assert len(result) == 11
    assert [5041, "Ethik", 4, 2125, None, None]
    assert sum([res[4] is None for res in result.records]) == 10


def test_mergejoin_simple_column_name():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger")])
    comparative = ComparativeExpression(ColumnExpression("VorlNr"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("Vorgaenger"))

    join = MergeJoin(table1, table2, JoinType.INNER, comparative, False)
    result = join.get_result()

    assert len(result) == 10
    assert result.schema.column_names == ["vorlesungen.VorlNr", "vorlesungen.Titel",
                                          "vorlesungen.SWS", "vorlesungen.gelesenVon",
                                          "voraussetzen.Vorgaenger", "voraussetzen.Nachfolger"]
    assert [5001, "Grundzuege", 4, 2137, 5001, 5041] in result.records
    assert [5043, "Erkenntnistheorie", 3, 2126, 5043, 5052] in result.records
    assert [5049] not in [res[0] for res in result.records]
    assert ["Der Wiener"] not in [res[1] for res in result.records]


# TODO: fix (no table reference found in join condition)
def test_mergejoin_natural():
    table1 = Ordering(Projection(TableScan("professoren"), [("Boss", ColumnExpression("PersNr"))]),
                      [ColumnExpression("Boss")])
    table2 = Ordering(Projection(TableScan("assistenten"), [(None, ColumnExpression("Boss"))]),
                      [ColumnExpression("Boss")])

    join = MergeJoin(table1, table2, JoinType.INNER, None, True)
    result = join.get_result()

    assert len(result) == 6


# TODO: fix (no table reference found in join condition)
def test_mergejoin_left_natural():
    table1 = Ordering(Projection(TableScan("professoren"), [("Boss", ColumnExpression("PersNr"))]),
                      [ColumnExpression("Boss")])
    table2 = Ordering(TableScan("assistenten"), [ColumnExpression("Boss")])

    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, None, True)
    result = join.get_result()

    assert len(result) == 9
    assert [2125, 3002, "Platon", "Ideenlehre"] in result.records


def test_unsorted_tables():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("voraussetzen.Vorgaenger"))

    with pytest.raises(TableNotSortedException):
        MergeJoin(table1, table2, JoinType.INNER, comparative, False).get_result()


# TODO: fix (_check_table_sorting in merge_join_operator is faulty)
def test_different_sorted_tables():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Nachfolger")])
    comparative = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("voraussetzen.Vorgaenger"))

    with pytest.raises(TableNotSortedException):
        MergeJoin(table1, table2, JoinType.INNER, comparative, False).get_result()


def test_mergejoin_wrong_condition_type():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger")])
    comparative_literal = ComparativeExpression(LiteralExpression("vorlesungen.VorlNr"),
                                                ComparativeOperator.EQUAL,
                                                ColumnExpression("voraussetzen.Vorgaenger"))

    disjunctive = DisjunctiveExpression(None)
    with pytest.raises(JoinConditionNotSupportedException):
        MergeJoin(table1, table2, JoinType.INNER, comparative_literal, False)
        MergeJoin(table1, table2, JoinType.INNER, disjunctive, False)


def test_mergejoin_natural_no_column_match():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger")])
    with pytest.raises(JoinTypeNotSupportedException):
        MergeJoin(table1, table2, JoinType.INNER, None, True)


def test_mergejoin_wrong_jointype():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger")])
    with pytest.raises(JoinTypeNotSupportedException):
        MergeJoin(table1, table2, JoinType.CROSS, None, True)


def test_mergejoin_double_reference_in_condition():
    table1 = TableScan("voraussetzen")
    table2 = TableScan("vorlesungen")
    comparative = ComparativeExpression(ColumnExpression("Vorgaenger"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("Nachfolger"))

    with pytest.raises(ErrorInJoinConditionException):
        MergeJoin(table1, table2, JoinType.INNER, comparative, False)


def test_mergejoin_no_reference_in_condition():
    table1 = Ordering(TableScan("vorlesungen"), [ColumnExpression("VorlNr")])
    table2 = Ordering(TableScan("voraussetzen"), [ColumnExpression("Vorgaenger")])
    comparative = ComparativeExpression(ColumnExpression("voraussetzen.Vorgaengerer"),
                                        ComparativeOperator.EQUAL,
                                        ColumnExpression("voraussetzen.Vorgaengerer"))

    with pytest.raises(ErrorInJoinConditionException):
        MergeJoin(table1, table2, JoinType.INNER, comparative, False)


def test_mergejoin_not_equal_in_condition():
    col1 = ColumnExpression("vorlesungen.VorlNr")
    col2 = ColumnExpression("voraussetzen.Vorgaenger")
    table1 = Ordering(TableScan("vorlesungen"), [col1])
    table2 = Ordering(TableScan("voraussetzen"), [col2])

    comparative_smaller = ComparativeExpression(col1,
                                                ComparativeOperator.SMALLER,
                                                col2)
    comparative_smaller_equal = ComparativeExpression(col1,
                                                      ComparativeOperator.SMALLER_EQUAL,
                                                      col2)
    comparative_greater = ComparativeExpression(col1,
                                                ComparativeOperator.GREATER,
                                                col2)
    comparative_greater_equal = ComparativeExpression(col1,
                                                      ComparativeOperator.GREATER_EQUAL,
                                                      col2)
    comparative_not_equal = ComparativeExpression(col1,
                                                  ComparativeOperator.NOT_EQUAL,
                                                  col2)
    with pytest.raises(JoinConditionNotSupportedException):
        MergeJoin(table1, table2, JoinType.INNER, comparative_smaller_equal, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_smaller, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_greater, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_greater_equal, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_not_equal, False)


def test_mergejoin_explain():
    table1 = Ordering(TableScan("studenten"), [ColumnExpression("Name")])
    table2 = Ordering(TableScan("assistenten"), [ColumnExpression("Name")])

    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, None, True)
    expl = Explain(join)
    result = expl.get_result()

    assert len(result) == 5
    assert "MergeJoin" in result.records[0][0]
    assert "left_outer" in result.records[0][0]
    assert "condition=(studenten.Name = assistenten.Name)" in result.records[0][0]
    assert "natural=True" in result.records[0][0]
