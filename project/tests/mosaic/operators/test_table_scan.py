from mosaic.expressions.table_scan import TableScan
from mosaic.table_service import Table
from mosaic import table_service

def test_retrieve_table():
    expression = TableScan("#tables")
    result = expression.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "#tables"
    assert result.schema_names == ["table_name"]

def test_retrieve_alias_table():
    expression = TableScan("#tables", "test")
    result = expression.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "test"
    assert result.schema_names == ["table_name"]

    assert table_service.retrieve("#tables").table_name == "#tables"