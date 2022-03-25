import click
import sys
import traceback
from mosaic import table_service
from mosaic import query_executor


class CliErrorMessageException(Exception):
    pass


def _quit_application():
    click.echo("Bye!")
    sys.exit(0)


def _print_command_help():
    click.echo("\\help \t\t\t\t shows this output.")
    click.echo("\\execute <query-file> \t\t executes the query loaded from query-file.")
    click.echo("\\quit \t\t\t\t quits the application.")
    click.echo("\\clear \t\t\t\t clears the screen.")
    click.echo("")
    click.echo("<query> \t\t\t executes a query that needs to be terminated by \";\"")


def _execute_query_file_from_command(user_in):
    """
    Function that parses the \\execute <file_name> command and calls the regular execute function with the resulting
    file name
    """
    split_string = user_in.split(" ")
    if len(split_string) != 2:
        raise CliErrorMessageException("Wrong usage of \\execute. See \\help for further detail")
    else:
        query_executor.execute_query_file(split_string[1])


def _execute_command(user_in):
    """
    Function that executes a command entered by the user.
    """
    if user_in[-1] == ';':
        raise CliErrorMessageException("Do not use \";\" at the end of commands!")
    elif user_in == "\\help" or user_in == "\\h":
        _print_command_help()
    elif user_in == "\\quit" or user_in == "\\q":
        _quit_application()
    elif user_in == "\\clear" or user_in == "\\c":
        click.clear()
    elif user_in.startswith("\\execute ") or user_in.startswith("\\e "):
        _execute_query_file_from_command(user_in)
    else:
        raise CliErrorMessageException("Unknown command entered. See \\help for a list of available commands.")


def _multi_line_loop(user_in):
    """
    Helper function to allow for multiline input.
    Concatenates every new input line with the previous input and returns it.
    """
    while user_in[-1] != ';':
        click.echo(">   ", nl=False)
        user_in += " " + input()
    return user_in


def _main_loop():
    """
    Function that represents the main interaction with the user.
    Distinguishes between queries and commands and also handles wrong input.
    """
    while True:
        try:
            click.echo(">>> ", nl=False)
            user_in = input()
            if user_in == '':
                continue
            elif user_in[0] == "\\":
                _execute_command(user_in)
            else:
                if user_in[-1] != ';':
                    user_in = _multi_line_loop(user_in)
                query_executor.execute_query(user_in)
        except CliErrorMessageException as e:
            click.secho("Error: " + str(e), fg='red')
        except Exception:
            # handle not expected exceptions by printing the stacktrace and continue
            tb = traceback.format_exc()
            click.secho(tb, fg='red')


@click.command()
@click.option("--data-directory", required=True, type=click.Path(exists=True),
              help="Directory which contains all tables to load at startup")
@click.option("--query-file", default=None, type=click.Path(exists=True),
              help="Path to an optional query file to execute")
def main(data_directory, query_file):
    """
    Function that executes on program startup. Loads initial data and optionally executes a query file.
    """
    table_service.load_tables_from_directory(data_directory)
    if query_file is not None:
        try:
            query_executor.execute_query_file(query_file)
        except CliErrorMessageException as e:
            click.secho("Error: " + str(e), fg='red')
        sys.exit(0)
    _main_loop()
