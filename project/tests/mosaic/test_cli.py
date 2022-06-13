import pytest
from mosaic import cli
from mosaic import table_service
from mosaic.compiler import optimizer
from unittest.mock import patch
from io import StringIO
from click.testing import CliRunner


def mock_stdout(func):
    """
    Decorator that redirects stdout to mock_output.
    Output can be checked with mock_output.getvalue()
    """
    def wrapper():
        with patch('sys.stdout', new = StringIO()) as mock_output:
            func(mock_output)
    return wrapper


def mock_prompt_input(input_list):
    """
    Decorator that mocks input functionality in mosaic.cli.
    param input_list: a list of strings, representing the commands/queries to execute
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            with patch("mosaic.cli._get_prompt_session", return_value=None):
                with patch("mosaic.cli._get_prompt_input", side_effect=input_list):
                    return func(*args, **kwargs)
        return wrapper
    return decorator


@mock_stdout
def test_execute_command_help(mock_out):
    cli._execute_command("\\help")
    function_output = mock_out.getvalue()
    assert "help" in function_output
    assert "execute" in function_output
    assert "quit" in function_output
    assert "clear" in function_output
    assert "<query>" in function_output


@mock_stdout
def test_execute_command_execute(mock_out):
    table_service.load_tables_from_directory("./tests/testdata/")
    cli._execute_command("\\execute ./tests/mosaic/testqueries/valid_query.mql")
    function_output = mock_out.getvalue()
    assert "MatrNr" in function_output
    assert "studenten" in function_output


def test_execute_query_file_from_command_exception():
    with pytest.raises(cli.CliErrorMessageException) as pytest_wrapped_e:
        cli._execute_query_file_from_command("\\execute")
    assert pytest_wrapped_e.type == cli.CliErrorMessageException


def test_execute_command_exception():
    with pytest.raises(cli.CliErrorMessageException) as pytest_wrapped_e:
        cli._execute_command("\\help;")
    assert pytest_wrapped_e.type == cli.CliErrorMessageException
    with pytest.raises(cli.CliErrorMessageException) as pytest_wrapped_e:
        cli._execute_command("\\Testcommand")
    assert pytest_wrapped_e.type == cli.CliErrorMessageException


@mock_stdout
def test_execute_command_quit(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._execute_command("\\q")
    function_output = mock_out.getvalue()
    assert "Bye" in function_output
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


@mock_stdout
def test_execute_command_optimize(mock_out):
    cli._execute_command("\\optimize")
    function_output = mock_out.getvalue()
    assert "Optimizer was enabled" in function_output
    cli._execute_command("\\optimize")
    function_output = mock_out.getvalue()
    assert "Optimizer was disabled" in function_output


@mock_stdout
def test_load_initial_data(mock_out):
    cli._load_initial_data("./tests/testdata/")
    function_output = mock_out.getvalue()
    assert table_service.retrieve_table("studenten") is not None
    assert "could not be loaded" in function_output
    assert "doNotLoad.notable" in function_output


@mock_stdout
def test_load_initial_data_exception(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._load_initial_data("./")
    assert "Error" in mock_out.getvalue()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1


@mock_stdout
def test_execute_initial_query_file_exception(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._execute_initial_query_file("./tests/mosaic/testqueries/wrong_query")
    assert "Error" in mock_out.getvalue()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


def test_main_no_query_file():
    runner = CliRunner()
    result = runner.invoke(cli.main, ["--data-directory", "./tests/testdata/"])
    assert "Data loaded from" in result.output
    assert "Welcome to Mosaic" in result.output


def test_main_query_file():
    runner = CliRunner()
    result = runner.invoke(cli.main, ["--data-directory", "./tests/testdata/", "--query-file", "./tests/mosaic/testqueries/valid_query.mql"])
    assert "MatrNr" in result.output
    assert "studenten" in result.output


def test_main_optimize():
    runner = CliRunner()
    result = runner.invoke(cli.main, ["--data-directory", "./tests/testdata/", "--optimize"])
    assert "Optimizer is enabled" in result.output


@mock_stdout
@mock_prompt_input(input_list=["", "\\help", "\\quit"])
def test_main_loop_command_execution(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._main_loop()
    assert "help" in mock_out.getvalue()
    assert "Bye" in mock_out.getvalue()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


@mock_stdout
@mock_prompt_input(input_list=["#tables;", "\\q"])
def test_main_loop_query_execution(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._main_loop()
    assert "#tables" in mock_out.getvalue()
    assert "studenten" in mock_out.getvalue()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


@mock_stdout
@mock_prompt_input(input_list=["pi\n", "MatrNr studenten;", "\\q"])
def test_main_loop_multi_line_query(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._main_loop()
    assert "studenten.MatrNr" in mock_out.getvalue()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


@mock_stdout
@mock_prompt_input(input_list=["\\helping", "\\q"])
def test_main_cli_error_message_exception(mock_out):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        cli._main_loop()
    assert "Error" in mock_out.getvalue()
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0
