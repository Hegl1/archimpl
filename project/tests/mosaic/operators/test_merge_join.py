import pytest
from mosaic import table_service
from mosaic.compiler.operators.abstract_join_operator import JoinType, JoinTypeNotSupportedException, JoinConditionNotSupportedException, ErrorInJoinConditionException
from mosaic.compiler.operators.merge_join_operator import MergeJoin
from mosaic.compiler.operators.ordering_operator import OrderingOperator
from mosaic.compiler.operators.table_scan_operator import TableScan
from mosaic.compiler.operators.projection_operator import Projection
from mosaic.compiler.operators.explain import Explain
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.comparative_operation_expression import ComparativeOperationExpression, \
    ComparativeOperator
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_mergejoin_comparative():
    table1 = OrderingOperator([ColumnExpression("VorlNr")], TableScan("vorlesungen"))
    table2 = OrderingOperator([ColumnExpression("Vorgaenger")], TableScan("voraussetzen"))
    comparative = ComparativeOperationExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaenger"))
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
    table1 = OrderingOperator([ColumnExpression("VorlNr"), ColumnExpression("gelesenVon")], TableScan("vorlesungen"))
    table2 = OrderingOperator([ColumnExpression("Vorgaenger"), ColumnExpression("Nachfolger")], TableScan("voraussetzen"))
    comparative1 = ComparativeOperationExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Vorgaenger"))
    comparative2 = ComparativeOperationExpression(ColumnExpression("vorlesungen.gelesenVon"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Nachfolger"))
    conjunctive = ConjunctiveExpression([comparative1, comparative2])
    join = MergeJoin(table1, table2, JoinType.INNER, conjunctive, False)
    result = join.get_result()
    assert len(result) == 1
    assert len(result.schema.column_names) == 6
    assert [5001, "Grundzuege", 4, 5041, 5001, 5041] in result.records


def test_mergejoin_left_comparative():
    table1 = OrderingOperator([ColumnExpression("PersNr")], TableScan("professoren"))
    table2 = OrderingOperator([ColumnExpression("Boss")], Projection([(None, ColumnExpression("Boss"))], TableScan("assistenten")))
    comparative = ComparativeOperationExpression(ColumnExpression("professoren.PersNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("assistenten.Boss"))
    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, comparative, False)
    result = join.get_result()
    assert len(result) == 9
    assert [5001, "Grundzuege", 4, 2137, 5001, 5041] in result.records
    assert [5043, "Erkenntnistheorie", 3, 2126, 5043, 5052] in result.records
    assert [5259, "Der Wiener Kreis", 2, 2133, None, None] in result.records


def test_mergejoin_left_conjunctive():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative1 = ComparativeOperationExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Vorgaenger"))
    comparative2 = ComparativeOperationExpression(ColumnExpression("vorlesungen.gelesenVon"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Nachfolger"))
    conjunctive = ConjunctiveExpression([comparative1, comparative2])
    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, conjunctive, False)
    result = join.get_result()
    assert len(result) == 11
    assert [5001, "Grundzuege", 4, 5041, 5001, 5041]
    assert sum([res[4] == None for res in result.records]) == 10


def test_meregjoin_simple_column_name():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeOperationExpression(ColumnExpression("VorlNr"),
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


def test_mergejoin_natural():
    table1 = TableScan("vorlesungen")
    table2 = Projection([(None, ColumnExpression("VorlNr")), (None, ColumnExpression("Titel")),
                         ("const", LiteralExpression("literal"))], TableScan("vorlesungen", "vos"))
    join = MergeJoin(table1, table2, JoinType.INNER, None, True)
    result = join.get_result()
    assert len(result) == 13
    assert len(result.schema.column_names) == 5
    assert [5001, "Grundzuege", 4, 2137, "literal"] in result.records


def test_mergejoin_left_natural():
    table1 = TableScan("studenten")
    table2 =  TableScan("assistenten")
    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, None, True)
    result = join.get_result()
    assert len(result) == 8
    assert [24002, "Xenokrates", 18, None, None, None] in result.records
    assert [29555, "Feuerbach", 2, None, None, None] in result.records


def test_mergejoin_wrong_condition_type():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative_literal = ComparativeOperationExpression(LiteralExpression("vorlesungen.VorlNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaenger"))

    disjunctive = DisjunctiveExpression(None)
    with pytest.raises(JoinConditionNotSupportedException):
        MergeJoin(table1, table2, JoinType.INNER, comparative_literal, False)
        MergeJoin(table1, table2, JoinType.INNER, disjunctive, False)


def test_mergejoin_natural_no_column_match():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    with pytest.raises(JoinTypeNotSupportedException):
        MergeJoin(table1, table2, JoinType.INNER, None, True)


def test_mergejoin_wrong_jointype():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    with pytest.raises(JoinTypeNotSupportedException):
        MergeJoin(table1, table2, JoinType.CROSS, None, True)


def test_mergejoin_double_reference_in_condition():
    table1 = TableScan("voraussetzen")
    table2 = TableScan("vorlesungen")
    comparative = ComparativeOperationExpression(ColumnExpression("Vorgaenger"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("Nachfolger"))

    with pytest.raises(ErrorInJoinConditionException):
        MergeJoin(table1, table2, JoinType.INNER, comparative, False)


def test_mergejoin_no_reference_in_condition():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeOperationExpression(ColumnExpression("voraussetzen.Vorgaengerer"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaengerer"))

    with pytest.raises(ErrorInJoinConditionException):
        MergeJoin(table1, table2, JoinType.INNER, comparative, False)


def test_mergejoin_not_equal_in_condition():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    col1 = ColumnExpression("vorlesungen.VorlNr")
    col2 = ColumnExpression("voraussetzen.Vorgaenger")
    comparative_smaller = ComparativeOperationExpression(col1,
                                                         ComparativeOperator.SMALLER,
                                                         col2)
    comparative_smaller_equal = ComparativeOperationExpression(col1,
                                                               ComparativeOperator.SMALLER_EQUAL,
                                                               col2)
    comparative_greater = ComparativeOperationExpression(col1,
                                                         ComparativeOperator.GREATER,
                                                         col2)
    comparative_greater_equal = ComparativeOperationExpression(col1,
                                                         ComparativeOperator.GREATER_EQUAL,
                                                         col2)
    comparative_not_equal = ComparativeOperationExpression(col1,
                                                               ComparativeOperator.NOT_EQUAL,
                                                               col2)
    with pytest.raises(JoinConditionNotSupportedException):
        MergeJoin(table1, table2, JoinType.INNER, comparative_smaller_equal, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_smaller, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_greater, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_greater_equal, False)
        MergeJoin(table1, table2, JoinType.INNER, comparative_not_equal, False)


def test_mergejoin_explain():
    table1 = TableScan("studenten")
    table2 = TableScan("assistenten")
    join = MergeJoin(table1, table2, JoinType.LEFT_OUTER, None, True)
    expl = Explain(join)
    result = expl.get_result()
    assert len(result) == 3
    assert "MergeJoin" in result.records[0][0]
    assert "left=True" in result.records[0][0]
    assert "condition=(studenten.Name = assistenten.Name)" in result.records[0][0]
    assert "natural=True" in result.records[0][0]
