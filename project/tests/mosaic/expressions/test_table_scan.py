from mosaic.expressions.table_scan import TableScan
from mosaic.table_service import Table
from mosaic import table_service
from mosaic.table_service import TableNotFoundException
import pytest

def test_retrieve_table():
    expression = TableScan("#tables")
    result = expression.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "#tables"
    assert result.schema_names == ["#tables.table_name"]

def test_retrieve_alias_table():
    expression = TableScan("#tables", "test")
    result = expression.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "test"
    assert result.schema_names == ["test.table_name"]

    assert table_service.retrieve("#tables").table_name == "#tables"

def test_retrieve_non_existent_table():
    expression = TableScan("non_existent")
    with pytest.raises(TableNotFoundException):
        expression.get_result()