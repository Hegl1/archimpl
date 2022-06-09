import pytest
from mosaic import table_service
from mosaic.compiler.operators.abstract_join import JoinType, JoinTypeNotSupportedException, JoinConditionNotSupportedException, ErrorInJoinConditionException
from mosaic.compiler.operators.hash_join import HashJoin
from mosaic.compiler.operators.table_scan import TableScan
from mosaic.compiler.operators.projection import Projection
from mosaic.compiler.operators.explain import Explain
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, \
    ComparativeOperator
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_hashjoin_comparative():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaenger"))
    join = HashJoin(table1, table2, JoinType.INNER, comparative, False)
    result = join.get_result()
    assert len(result) == 10
    assert result.schema.column_names == ["vorlesungen.VorlNr", "vorlesungen.Titel",
                                          "vorlesungen.SWS", "vorlesungen.gelesenVon",
                                          "voraussetzen.Vorgaenger", "voraussetzen.Nachfolger"]
    assert [5001, "Grundzuege", 4, 2137, 5001, 5041] in result.records
    assert [5043, "Erkenntnistheorie", 3, 2126, 5043, 5052] in result.records
    assert [5049] not in [res[0] for res in result.records]
    assert ["Der Wiener"] not in [res[1] for res in result.records]


def test_hashjoin_conjunctive():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative1 = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Vorgaenger"))
    comparative2 = ComparativeExpression(ColumnExpression("vorlesungen.gelesenVon"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Nachfolger"))
    conjunctive = ConjunctiveExpression([comparative1, comparative2])
    join = HashJoin(table1, table2, JoinType.INNER, conjunctive, False)
    result = join.get_result()
    assert len(result) == 1
    assert len(result.schema.column_names) == 6
    assert [5001, "Grundzuege", 4, 5041, 5001, 5041] in result.records


def test_hashjoin_left_comparative():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaenger"))
    join = HashJoin(table1, table2, JoinType.LEFT_OUTER, comparative, False)
    result = join.get_result()
    assert len(result) == 16
    assert [5001, "Grundzuege", 4, 2137, 5001, 5041] in result.records
    assert [5043, "Erkenntnistheorie", 3, 2126, 5043, 5052] in result.records
    assert [5259, "Der Wiener Kreis", 2, 2133, None, None] in result.records


def test_hashjoin_left_conjunctive():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative1 = ComparativeExpression(ColumnExpression("vorlesungen.VorlNr"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Vorgaenger"))
    comparative2 = ComparativeExpression(ColumnExpression("vorlesungen.gelesenVon"),
                                                  ComparativeOperator.EQUAL,
                                                  ColumnExpression("voraussetzen.Nachfolger"))
    conjunctive = ConjunctiveExpression([comparative1, comparative2])
    join = HashJoin(table1, table2, JoinType.LEFT_OUTER, conjunctive, False)
    result = join.get_result()
    assert len(result) == 11
    assert [5001, "Grundzuege", 4, 5041, 5001, 5041]
    assert sum([res[4] == None for res in result.records]) == 10


def test_hashjoin_simple_column_name():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeExpression(ColumnExpression("VorlNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("Vorgaenger"))
    join = HashJoin(table1, table2, JoinType.INNER, comparative, False)
    result = join.get_result()
    assert len(result) == 10
    assert result.schema.column_names == ["vorlesungen.VorlNr", "vorlesungen.Titel",
                                          "vorlesungen.SWS", "vorlesungen.gelesenVon",
                                          "voraussetzen.Vorgaenger", "voraussetzen.Nachfolger"]
    assert [5001, "Grundzuege", 4, 2137, 5001, 5041] in result.records
    assert [5043, "Erkenntnistheorie", 3, 2126, 5043, 5052] in result.records
    assert [5049] not in [res[0] for res in result.records]
    assert ["Der Wiener"] not in [res[1] for res in result.records]


def test_hashjoin_natural():
    table1 = TableScan("vorlesungen")
    table2 = Projection(TableScan("vorlesungen", "vos"),
                        [(None, ColumnExpression("VorlNr")), (None, ColumnExpression("Titel")),
                         ("const", LiteralExpression("literal"))])
    join = HashJoin(table1, table2, JoinType.INNER, None, True)
    result = join.get_result()
    assert len(result) == 13
    assert len(result.schema.column_names) == 5
    assert [5001, "Grundzuege", 4, 2137, "literal"] in result.records


def test_hashjoin_left_natural():
    table1 = TableScan("studenten")
    table2 =  TableScan("assistenten")
    join = HashJoin(table1, table2, JoinType.LEFT_OUTER, None, True)
    result = join.get_result()
    assert len(result) == 8
    assert [24002, "Xenokrates", 18, None, None, None] in result.records
    assert [29555, "Feuerbach", 2, None, None, None] in result.records


def test_hashjoin_wrong_condition_type():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative_literal = ComparativeExpression(LiteralExpression("vorlesungen.VorlNr"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaenger"))
    disjunctive = DisjunctiveExpression(None)
    with pytest.raises(JoinConditionNotSupportedException):
        HashJoin(table1, table2, JoinType.INNER, comparative_literal, False)
        HashJoin(table1, table2, JoinType.INNER, disjunctive, False)


def test_hashjoin_natural_no_column_match():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    with pytest.raises(JoinTypeNotSupportedException):
        HashJoin(table1, table2, JoinType.INNER, None, True)


def test_hashjoin_wrong_jointype():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    with pytest.raises(JoinTypeNotSupportedException):
        HashJoin(table1, table2, JoinType.CROSS, None, True)


def test_hashjoin_double_reference_in_condition():
    table1 = TableScan("voraussetzen")
    table2 = TableScan("vorlesungen")
    comparative = ComparativeExpression(ColumnExpression("Vorgaenger"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("Nachfolger"))

    with pytest.raises(ErrorInJoinConditionException):
        HashJoin(table1, table2, JoinType.INNER, comparative, False)


def test_hashjoin_no_reference_in_condition():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    comparative = ComparativeExpression(ColumnExpression("voraussetzen.Vorgaengerer"),
                                                 ComparativeOperator.EQUAL,
                                                 ColumnExpression("voraussetzen.Vorgaengerer"))

    with pytest.raises(ErrorInJoinConditionException):
        HashJoin(table1, table2, JoinType.INNER, comparative, False)


def test_hashjoin_not_equal_in_condition():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("voraussetzen")
    col1 = ColumnExpression("vorlesungen.VorlNr")
    col2 = ColumnExpression("voraussetzen.Vorgaenger")
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
    comperative_not_equal = ComparativeExpression(col1,
                                                               ComparativeOperator.NOT_EQUAL,
                                                               col2)
    with pytest.raises(JoinConditionNotSupportedException):
        HashJoin(table1, table2, JoinType.INNER, comparative_smaller_equal, False)
        HashJoin(table1, table2, JoinType.INNER, comparative_smaller, False)
        HashJoin(table1, table2, JoinType.INNER, comparative_greater, False)
        HashJoin(table1, table2, JoinType.INNER, comparative_greater_equal, False)
        HashJoin(table1, table2, JoinType.INNER, comperative_not_equal, False)


def test_hashjoin_explain():
    table1 = TableScan("studenten")
    table2 = TableScan("assistenten")
    join = HashJoin(table1, table2, JoinType.LEFT_OUTER, None, True)
    expl = Explain(join)
    result = expl.get_result()
    assert len(result) == 3
    assert "HashJoin" in result.records[0][0]
    assert "left_outer" in result.records[0][0]
    assert "condition=(studenten.Name = assistenten.Name)" in result.records[0][0]
    assert "natural=True" in result.records[0][0]


def test_natural_join_alias_projection():
    table1 = TableScan("vorlesungen")
    table2 = TableScan("professoren")
    projection = Projection(table1, [("PersNr", ColumnExpression("gelesenVon"))])
    join = HashJoin(projection, table2, JoinType.INNER, None, True)
    result = join.get_result()
    assert len(result) == 10
    assert result.schema.column_names == ["PersNr", "professoren.Name", "professoren.Rang", "professoren.Raum"]
