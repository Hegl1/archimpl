"""This file contains the implementation of our relational algebra parser.

Author: Maximilian Mayerl                                                    #
"""

from parsimonious.exceptions import ParseError
from parsimonious.nodes import Node

from .grammar import grammar as _grammar


class ParsingResult:
    """The result produced by the query parser.

    This is either a valid abstract syntax tree (AST), or an error message.
    """

    def __init__(self, ast: Node, error: str):
        """Creates a ParsingResult.

        Args:
            ast (Node): the abstract syntax tree generated in the parse.
            error (str): the error message associated with the parse, if an
                error occurred.
        """
        self.ast = ast
        self.error = error

    def has_error(self):
        """Determine whether the parse resulted in an error."""
        return self.error is not None


def parse_query(query: str):
    """Parses the given query and returns the parsing result.

    Args:
        query (str): the query that should be parsed.
    """
    # We try to parse the given query.
    # If that fails, we get the error message.
    ast = None
    error = None

    try:
        ast = _grammar.parse(query)
    except ParseError as err:
        error = f'Error During Query Parsing: {err}'

    # Pack the result into a `CompilationResult`.
    return ParsingResult(ast, error)
