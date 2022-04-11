from mosaic.operators.table_scan import TableScan
from mosaic.table_service import Table
from mosaic import table_service

def test_retrieve_table():
    operator = TableScan("#tables")
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "#tables"
    assert result.schema_names == ["table_name"]

def test_retrieve_alias_table():
    operator = TableScan("#tables", "test")
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "test"
    assert result.schema_names == ["table_name"]

    assert table_service.retrieve("#tables").table_name == "#tables"