from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, ComparativeOperator
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.operators.index_seek import IndexSeek, ErrorInIndexSeekConditionException, \
    IndexSeekConditionNotSupportedException
from mosaic.table_service import Table, IndexNotFoundException
from mosaic.table_service import TableNotFoundException
import pytest


def test_index_seek_no_alias():
    operator = IndexSeek("correctIndex", "MatrNr", _get_nice_condition(), alias=None)
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "correctIndex"
    assert result.schema.column_names == ["correctIndex.MatrNr", "correctIndex.VorlNr"]
    assert len(result.records) == 4
    for record in result.records:
        assert record[0] == 28106
    assert str(operator) == "IndexSeek(correctIndex_MatrNr, condition=(correctIndex.MatrNr = 28106))"


def test_index_seek_no_alias_condition_mirrored():
    left = LiteralExpression(28106)
    right = ColumnExpression("MatrNr")
    condition = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    operator = IndexSeek("correctIndex", "MatrNr", condition, alias=None)
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "correctIndex"
    assert result.schema.column_names == ["correctIndex.MatrNr", "correctIndex.VorlNr"]
    assert len(result.records) == 4
    for record in result.records:
        assert record[0] == 28106


def test_index_seek_alias():
    left = ColumnExpression("test.MatrNr")
    right = LiteralExpression(28106)
    condition = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    operator = IndexSeek("correctIndex", "test.MatrNr", condition, alias="test")
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "test"
    assert result.schema.column_names == ["test.MatrNr", "test.VorlNr"]
    assert len(result.records) == 4
    for record in result.records:
        assert record[0] == 28106
    assert str(operator) == "IndexSeek(correctIndex_MatrNr, table_alias=test, condition=(test.MatrNr = 28106))"


def test_index_seek_table_non_existent():
    with pytest.raises(TableNotFoundException):
        IndexSeek("non_existent_table", "whatever_column", _get_nice_condition())


def test_index_seek_index_non_existent():
    with pytest.raises(IndexNotFoundException):
        IndexSeek("correctIndex", "non_existent_column", _get_nice_condition())


def test_index_seek_no_result_records():
    left = ColumnExpression("MatrNr")
    right = LiteralExpression(11111)
    condition = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    operator = IndexSeek("correctIndex", "MatrNr", condition)
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "correctIndex"
    assert result.schema.column_names == ["correctIndex.MatrNr", "correctIndex.VorlNr"]
    assert result.records == []


def _get_nice_condition():
    left = ColumnExpression("MatrNr")
    operator = ComparativeOperator.EQUAL
    right = LiteralExpression(28106)
    return ComparativeExpression(left, operator, right)


def test_index_seek_table_bad_condition_wrong_fqn_of_column():
    left = ColumnExpression("wrongTable.MatrNr")
    operator = ComparativeOperator.EQUAL
    right = LiteralExpression(28106)
    condition = ComparativeExpression(left, operator, right)
    with pytest.raises(ErrorInIndexSeekConditionException):
        IndexSeek("correctIndex", "MatrNr", condition)


def test_index_seek_table_bad_condition_wrong_comparative_operator():
    left = ColumnExpression("MatrNr")
    right = LiteralExpression(28106)
    condition = ComparativeExpression(left, ComparativeOperator.GREATER_EQUAL, right)
    with pytest.raises(IndexSeekConditionNotSupportedException):
        IndexSeek("correctIndex", "MatrNr", condition)


def test_index_seek_table_bad_condition_no_literal():
    left = ColumnExpression("MatrNr")
    right = ColumnExpression("false")
    condition = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    with pytest.raises(ErrorInIndexSeekConditionException):
        IndexSeek("correctIndex", "MatrNr", condition)
    left = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    right = ColumnExpression("MatrNr")
    condition = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    with pytest.raises(ErrorInIndexSeekConditionException):
        IndexSeek("correctIndex", "MatrNr", condition)


def test_index_seek_table_bad_condition_no_column_reference():
    left = LiteralExpression("foo")
    right = LiteralExpression(42)
    condition = ComparativeExpression(left, ComparativeOperator.EQUAL, right)
    with pytest.raises(ErrorInIndexSeekConditionException):
        IndexSeek("correctIndex", "MatrNr", condition)
