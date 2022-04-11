import pytest
from mosaic import query_executor
from mosaic import table_service
from mosaic import cli


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_execute_query():
    try:
        assert len(query_executor.execute_query("#tables;")) == 1
        assert len(query_executor.execute_query("#tables; #tables;")) == 2
        assert len(query_executor.execute_query("studenten;")) == 1
        # TODO: re-enable test after projection was implemented
        #assert len(query_executor.execute_query("pi studenten.MatrNr MatrNr;")) == 1
    except Exception:
        assert False, "Exception raised despite valid input"
    with pytest.raises(cli.CliErrorMessageException):
       query_executor.execute_query("tableNotFound;")
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query("this is no valid query;")


def test_execute_query_file():
    # TODO: re-enable test after projection was implemented
    #try:
    #    assert len(query_executor.execute_query_file("./tests/mosaic/testqueries/valid_query.mql")) == 2
    #except Exception as e:
    #    assert False, f"Exception raised despite valid query file: {e}"
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query_file("./notaValidFile")
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query_file("./tests/")
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query_file("./tests/mosaic/testqueries/incomplete_query.mql")
    with pytest.raises(cli.CliErrorMessageException):
        query_executor.execute_query_file("./tests/mosaic/testqueries/wrong_query.mql")