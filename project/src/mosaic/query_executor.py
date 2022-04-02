import click
from mosaic import parser
from mosaic import table_service
from mosaic import cli


def execute_query(user_in):
    """
    Function that executes queries. Multiple queries per line are also possible.
    """
    for query in user_in.split(";"):
        # strip to allow chaining of multiple queries in a line
        query = query.strip()
        if not query:
            pass
        elif " " not in query:
            # if there is a one word query
            try:
                click.echo(table_service.retrieve(query))
            except table_service.TableNotFoundException:
                raise cli.CliErrorMessageException("Table name not found")
        else:
            result = parser.parse_query(query)
            if result.has_error():
                raise cli.CliErrorMessageException(result.error)
            else:
                click.echo(result.ast)


def execute_query_file(file_path):
    """
    Function that executes queries found in a .mql file.
    """
    try:
        with open(file_path, 'r') as query_file:
            queries = query_file.read().strip()
            queries.replace("\n", " ")
            if not queries.endswith(';'):
                raise cli.CliErrorMessageException("Missing semicolon at the end of query file")
            else:
                execute_query(queries)
    except FileNotFoundError:
        raise cli.CliErrorMessageException("Invalid Path, no query file found")
    except IsADirectoryError:
        raise cli.CliErrorMessageException("Path is a directory, not a query file")