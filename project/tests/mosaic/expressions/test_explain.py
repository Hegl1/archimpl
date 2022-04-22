import pytest
from mosaic.query_executor import execute_query
from mosaic import table_service

# TODO expand tests when more commands are implemented


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_explain_table_scan():
    result, _ = execute_query("explain #tables;")[0]
    assert len(result.records) == 1
    assert result.records[0][0] == "-->TableScan(#tables)"


def test_explain_table_scan_rename():
    result, _ = execute_query("explain #tables as tabs;")[0]
    assert len(result.records) == 1
    assert result.records[0][0] == "-->TableScan(table_name=#tables, alias=tabs)"


def test_explain_projection():
    result, _ = execute_query("explain pi MatrNr, Name studenten;")[0]
    assert len(result.records) == 2
    assert result.records[0][0] == "-->Projection(columns=[MatrNr=MatrNr, Name=Name])"
    assert result.records[1][0] == "---->TableScan(studenten)"


def test_explain_projection_distinct():
    result, _ = execute_query("explain pi distinct MatrNr, Name studenten;")[0]
    assert len(result.records) == 3
    assert result.records[0][0] == "-->HashDistinct"


def test_explain_selection():
    pass


def test_explain_union():
    pass


def test_explain_intersect():
    pass


def test_explain_difference():
    pass


def test_explain_cross_join():
    pass


def test_explain_ordering():
    pass
