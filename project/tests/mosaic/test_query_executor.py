import pytest
from mosaic import query_executor
from mosaic import table_service
from mosaic import cli


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


@pytest.mark.parametrize(
    'optimized',
    [False, True],
)
def test_execute_query(optimized):
    try:
        assert len(query_executor.execute_query("#tables;", optimized)) == 1
        assert len(query_executor.execute_query("#tables; #tables;", optimized)) == 2

        table_result = query_executor.execute_query("studenten;", optimized)
        assert len(table_result) == 1
        assert len(table_result[0]) == 2
        assert table_result[0][1] < 10
        assert len(query_executor.execute_query("pi studenten.MatrNr studenten;", optimized)) == 1
    except Exception:
        assert False, "Exception raised despite valid input"
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query("tableNotFound;", optimized)
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query("this is no valid query;", optimized)


@pytest.mark.parametrize(
    'optimized',
    [False, True],
)
def test_execute_query_file(optimized):
    try:
        assert len(query_executor.execute_query_file("./tests/mosaic/testqueries/valid_query.mql", optimized)) == 2
    except Exception as e:
        assert False, f"Exception raised despite valid query file: {e}"


@pytest.mark.parametrize(
    'path',
    [
        "./notaValidFile",
        "./tests/",
        "./tests/mosaic/testqueries/incomplete_query.mql",
        "./tests/mosaic/testqueries/wrong_query.mql",
        "./tests/mosaic/testqueries",
    ],
)
def test_execute_bad_query_file(path):
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query_file(path)
