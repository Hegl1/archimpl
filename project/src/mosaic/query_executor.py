from time import perf_counter_ns

from mosaic import cli
from mosaic import parser
from mosaic.compiler import compiler
from mosaic.table_service import TableNotFoundException
from mosaic.compiler import optimizer


def execute_query(user_in, optimize=False):
    """
    Function that executes queries. Multiple queries per line are also possible.
    Returns a list containing all results as tuples: (result, execution_time)
    The execution_time is passed as milliseconds
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
                result_expression = compiler.compile(ast.ast)

                if optimize:
                    result_expression = optimizer.optimize(result_expression)

                start_execution = perf_counter_ns()

                result = result_expression.get_result()

                end_execution = perf_counter_ns()
                execution_time = (end_execution - start_execution) / 1000000

                results.append((result, execution_time))
            except TableNotFoundException as e:
                raise cli.CliErrorMessageException(f"Table with name \"{e.args[0]}\" does not exist")
            except Exception as e:
                message = str(e)

                if len(message) == 0:
                    message = f"A '{type(e).__name__}' occurred"

                raise cli.CliErrorMessageException(message)

    return results


def execute_query_file(file_path, optimize=False):
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
                return execute_query(queries, optimize)
    except FileNotFoundError:
        raise cli.CliErrorMessageException("Invalid Path, no query file found")
    except PermissionError:
        raise cli.CliErrorMessageException("Permission error when accessing the file")
    except IsADirectoryError:
        raise cli.CliErrorMessageException("Path is a directory, not a query file")
