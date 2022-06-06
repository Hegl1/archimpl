import os
import sys
import traceback

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.history import FileHistory

from mosaic import query_executor
from mosaic import table_service


class CliErrorMessageException(Exception):
    pass


_optimizer_enabled = False


def _quit_application():
    click.echo("Bye!")
    sys.exit(0)


def _print_command_help():
    click.echo("\\help \t\t\t\t shows this output.")
    click.echo("\\execute <query-file> \t\t executes the query loaded from query-file.")
    click.echo("\\optimize \t\t\t toggles whether the queries should be optimized.")
    click.echo("\\quit \t\t\t\t quits the application.")
    click.echo("\\clear \t\t\t\t clears the screen.")
    click.echo("")
    click.echo("<query> \t\t\t executes a query that needs to be terminated by \";\"")


def _print_results(results, printTime=True):
    for result, execution_time in results:
        click.echo(result)

        if printTime:
            click.echo(f"Executed query in {execution_time:0.3f} ms.\n")


def _execute_query_file_from_command(user_in):
    """
    Function that parses the \\execute <file_name> command and calls the regular execute function with the resulting
    file name
    """
    split_string = user_in.split(" ")
    if len(split_string) != 2:
        raise CliErrorMessageException("Wrong usage of \\execute. See \\help for further detail")
    else:
        results = query_executor.execute_query_file(split_string[1], _optimizer_enabled)
        _print_results(results)


def _execute_command(user_in):
    """
    Function that executes a command entered by the user.
    """
    if user_in.endswith(';'):
        raise CliErrorMessageException("Do not use \";\" at the end of commands!")
    elif user_in == "\\help" or user_in == "\\h":
        _print_command_help()
    elif user_in == "\\quit" or user_in == "\\q":
        _quit_application()
    elif user_in == "\\clear" or user_in == "\\c":
        click.clear()
    elif user_in == "\\optimize" or user_in == "\\o":
        global _optimizer_enabled
        _optimizer_enabled = not _optimizer_enabled

        click.echo("Optimizer was " + ("enabled" if _optimizer_enabled else "disabled"))
    elif user_in.startswith("\\execute ") or user_in.startswith("\\e "):
        _execute_query_file_from_command(user_in)
    else:
        raise CliErrorMessageException("Unknown command entered. See \\help for a list of available commands.")


def _get_prompt_session():
    """
    Function that returns a prompt session with the proper history
    """
    command_history = FileHistory(os.path.expanduser("~/archimpl_history"))
    return PromptSession(history=command_history, auto_suggest=AutoSuggestFromHistory())


def _get_prompt_input(prompt_symbol, prompt_session):
    """
    Helper function that returns the result of prompt_session.prompt.
    Useful for mocking in unit tests.
    """
    return prompt_session.prompt(prompt_symbol)


def _multi_line_loop(user_in, prompt_session):
    """
    Helper function to allow for multiline input.
    Concatenates every new input line with the previous input and returns it.
    """
    while not user_in.endswith(';'):
        user_in += " " + _get_prompt_input(">   ", prompt_session)
    return user_in


def _main_loop():
    """
    Function that represents the main interaction with the user.
    Distinguishes between queries and commands and also handles wrong input.
    """
    session = _get_prompt_session()
    while True:
        try:
            user_in = _get_prompt_input(">>> ", session)
            if user_in == '':
                continue
            elif user_in.startswith("\\"):
                _execute_command(user_in)
            else:
                if not user_in.endswith(";"):
                    user_in = _multi_line_loop(user_in, session)

                results = query_executor.execute_query(user_in, _optimizer_enabled)
                _print_results(results)
        except CliErrorMessageException as e:
            click.secho("Error: " + str(e), fg='red')
        except Exception:
            # handle not expected exceptions by printing the stacktrace and continue
            tb = traceback.format_exc()
            click.secho(tb, fg='red')


def _load_initial_data(data_directory):
    """
    Function that loads the initial data at cli startup based on the provided data directory.
    """
    try:
        not_loaded = table_service.load_tables_from_directory(data_directory)
        if len(not_loaded) > 0:
            click.secho("Error: Following files could not be loaded: ", fg="red")
            for file in not_loaded:
                click.secho(f"\t{file[0]} ({file[1]})", fg="red")
    except table_service.NoTableLoadedException:
        click.secho("Error: No table file could be loaded.", fg="red")
        sys.exit(1)


def _execute_initial_query_file(query_file_path):
    """
    Function that is used to load and execute a query file upon program startup.
    """
    try:
        results = query_executor.execute_query_file(query_file_path, _optimizer_enabled)
        _print_results(results)
    except CliErrorMessageException as e:
        click.secho("Error: " + str(e), fg='red')
    sys.exit(0)


@click.command()
@click.option("--data-directory", required=True, type=click.Path(exists=True),
              help="Directory which contains all tables to load at startup")
@click.option("--query-file", default=None, type=click.Path(exists=True),
              help="Path to an optional query file to execute")
@click.option("--optimize", is_flag = True, help="Enables the optimizer")
def main(data_directory, query_file, optimize):
    """
    Function that executes on program startup. Loads initial data and optionally executes a query file.
    """
    _load_initial_data(data_directory)

    global _optimizer_enabled
    _optimizer_enabled = optimize

    if query_file is not None:
        _execute_initial_query_file(query_file)
    click.echo(f"Data loaded from \"{data_directory}\"\n")

    if optimize:
        click.echo("Optimizer is enabled\n")

    click.secho("Welcome to Mosaic!\n", fg="green")
    _main_loop()
