import click
import os
import sys
import traceback
import parser


class CliErrorMessageException(Exception):
    pass


def _load_tables_from_directory(directory):
    for file in os.listdir(directory):
        # TODO load tables from files in the given directory
        click.echo(file)


def _execute_query_file(file_path):
    try:
        with open(file_path, 'r') as query_file:
            queries = query_file.read().strip()
            queries.replace("\n", "")
            if queries[-1] != ';':
                raise CliErrorMessageException("Missing semicolon at the end of query file")
            else:
                _execute_query(queries)
    except FileNotFoundError:
        raise CliErrorMessageException("Invalid Path, no query file found")
    except IsADirectoryError:
        raise CliErrorMessageException("Path is a directory, not a query file")


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
    split_string = user_in.split(" ")
    if len(split_string) != 2:
        raise CliErrorMessageException("Wrong usage of \\execute. See \\help for further detail")
    else:
        _execute_query_file(split_string[1])


def _execute_command(user_in):
    if user_in[-1] == ';':
        raise CliErrorMessageException("Do not use \";\" at the end of commands!")
    elif user_in == "\\help":
        _print_command_help()
    elif user_in == "\\quit":
        _quit_application()
    elif user_in == "\\clear":
        click.clear()
    elif user_in.startswith("\\execute"):
        _execute_query_file_from_command(user_in)
    else:
        raise CliErrorMessageException("Unknown command entered. See \\help for a list of available commands.")


def _print_table(table_name):
    # TODO implement
    # TODO handle table name not found
    click.echo(f"Printing table: {table_name}")


def _execute_query(user_in):
    for query in user_in.split(";"):
        # strip to allow chaining of multiple queries in a line
        query = query.strip()
        if len(query) == 0:
            pass
        elif query[0] == "#":
            # TODO handle table name not found
            _print_table(query[1:])
        else:
            result = parser.parse_query(query)
            if result.has_error():
                raise CliErrorMessageException(result.error)
            else:
                click.echo(result.ast)


def _multi_line_loop(user_in):
    while user_in[-1] != ';':
        click.echo(">   ", nl=False)
        user_in += input()
    return user_in


def _main_loop():
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
                _execute_query(user_in)
        except CliErrorMessageException as e:
            click.secho(str(e), fg='red')
        except Exception:
            # handle not expected exceptions by printing the stacktrace and continue
            tb = traceback.format_exc()
            click.secho(tb, fg='red')


@click.command()
@click.option("--data-directory", default="./", type=click.Path(exists=True),
              help="Directory which contains all tables to load at startup")
@click.option("--query-file", default=None, type=click.Path(exists=True),
              help="Path to an optional query file to execute")
def main(data_directory, query_file):
    _load_tables_from_directory(data_directory)
    if query_file is not None:
        try:
            _execute_query_file(query_file)
        except CliErrorMessageException as e:
            click.secho(str(e), fg='red')
        sys.exit(0)
    _main_loop()
