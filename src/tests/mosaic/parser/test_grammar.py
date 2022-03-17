"""Tests for the grammar used by the parser."""
from parsimonious.exceptions import ParseError
import pytest

from mosaic.parser.grammar import grammar


@pytest.mark.parametrize(
    'query',
    [
        '#tables',  # sys table
        'relation',  # normal table
        'pi car rel',
        'pi #car rel',
        'pi rel.car rel',
        'pi #rel.car rel',
        'select car rel',
        'select #car rel',
        'select rel.car rel',
        'select #rel.car rel',
        'sigma car > 1 rel',
        'sigma car > 1.1 rel',
        'sigma car > "5" rel',
        'gamma Semester aggregate Anzahl as count(MatrNr) studenten',
        'pi Name, FullName as "Prof. " + Name professoren',
    ],
)
def test_valid_query(query):
    """Tests if valid queries produce an output."""
    # if parsing fails an exception is raised and the test fails
    grammar.parse(query)


@pytest.mark.parametrize(
    'query',
    [
        '',
        'table 1',
        'pi rel.car.wheel rel',
        'pi #rel.car.wheel rel',
        'select rel.car.wheel rel',
        'select #rel.car.wheel rel',
        'gamma Semester aggregate "Anzahl" as count(MatrNr) studenten',
        'pi Name, "FullName" as "Prof. " + Name professoren',
    ],
)
def test_invalid_query(query):
    """Tests if invalid queries produce an error."""
    with pytest.raises(ParseError):
        grammar.parse(query)
