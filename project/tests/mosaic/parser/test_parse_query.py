"""Tests the parse_query function."""
from mosaic.parser.grammar import grammar
from mosaic.parser import parse_query


def test_valid_query():
    """Tests if the method works for a valid query."""
    query = '#tables'
    expected_ast = grammar.parse(query)
    expected_error = None

    result = parse_query(query)

    assert result.ast == expected_ast
    assert result.error == expected_error


def test_invalid_query():
    """Tests if the method works for an invalid query."""
    query = '#tables;'
    expected_ast = None
    expected_error = '\';\''

    result = parse_query(query)

    assert result.ast == expected_ast
    assert expected_error in result.error
