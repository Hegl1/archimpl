from mosaic import parser
from mosaic import cli
from mosaic import compiler
from mosaic.table_service import TableNotFoundException


def execute_query(user_in):
    """
    Function that executes queries. Multiple queries per line are also possible.
    Returns a list containing all results
    """
    results = []

    for query in user_in.split(";"):
        # strip to allow chaining of multiple queries in a line
        query = query.strip()

        if query:
            ast = parser.parse_query(query)

            if ast.has_error():
                raise cli.CliErrorMessageException(ast.error)

            try:
                result_operator = compiler.compile(ast.ast)

                results.append(result_operator.get_result())
            except TableNotFoundException as e:
                raise cli.CliErrorMessageException(f"Table with name \"{e.args[0]}\" does not exist")

    return results


def execute_query_file(file_path):
    """
    Function that executes queries found in a .mql file.
    Returns a list containing all results
    """
    try:
        with open(file_path, 'r') as query_file:
            queries = query_file.read().strip()
            queries.replace("\n", " ")
            if not queries.endswith(';'):
                raise cli.CliErrorMessageException("Missing semicolon at the end of query file")
            else:
                return execute_query(queries)
    except FileNotFoundError:
        raise cli.CliErrorMessageException("Invalid Path, no query file found")
    except PermissionError:
        raise cli.CliErrorMessageException("Permission error when accessing the file")
    except IsADirectoryError:
        raise cli.CliErrorMessageException("Path is a directory, not a query file")