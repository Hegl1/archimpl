from mosaic.compiler.operators.table_scan_operator import TableScan
from mosaic.table_service import Table
from mosaic import table_service
from mosaic.table_service import TableNotFoundException
import pytest


def test_retrieve_table():
    operator = TableScan("#tables")
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "#tables"
    assert result.schema.column_names == ["#tables.table_name"]


def test_retrieve_alias_table():
    operator = TableScan("#tables", "test")
    result = operator.get_result()

    assert isinstance(result, Table)
    assert result.table_name == "test"
    assert result.schema.column_names == ["test.table_name"]

    assert table_service.retrieve("#tables").table_name == "#tables"


def test_retrieve_non_existent_table():
    operator = TableScan("non_existent")
    with pytest.raises(TableNotFoundException):
        operator.get_result()
