"""Tests the ParsingReult class."""
from mosaic.parser import ParsingResult


def test_has_error_method():
    """Tests if the has error method correctly checks for errors."""
    result = ParsingResult(ast=None, error=None)
    assert result.has_error() is False

    result = ParsingResult(ast=None, error='')
    assert result.has_error() is True

    result = ParsingResult(ast=None, error='Some error')
    assert result.has_error() is True


def test_ast_member():
    """Tests if the ast member works as expected."""
    ast = object()

    result = ParsingResult(ast=ast, error=None)

    assert result.ast == ast


def test_error_member():
    """Test if the error member works as expected."""
    error = 'Some error'

    result = ParsingResult(ast=None, error=error)

    assert result.error == error
