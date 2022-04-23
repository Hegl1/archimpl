import pytest

from mosaic.query_executor import execute_query


@pytest.mark.parametrize(
    'query,correctly_sorted_fields,column_index',
    [
        ("tau Rang professoren;", ["C3", "C3", "C3", "C4", "C4", "C4", "C4"], 2),
        ("tau assistenten.Boss assistenten;", [2125, 2125, 2126, 2127, 2127, 2134], 3),
        ("tau Name pi PersNr, Name assistenten;", ["Aristoteles", "Newton", "Platon", "Rhetikus", "Spinoza", "Wittgenstein"], 1)
    ],
)
def test_ordering_single_key(query, correctly_sorted_fields, column_index):
    result, _ = execute_query(query)[0]
    assert len(result.records) == len(correctly_sorted_fields)
    for i, correctly_sorted_field in enumerate(correctly_sorted_fields):
        assert result.records[i][column_index] == correctly_sorted_field


def test_ordering_compound_key():
    result, _ = execute_query("tau Boss, Name assistenten;")[0]
    correctly_sorted_fields = ["Aristoteles", "Platon", "Wittgenstein", "Newton", "Rhetikus", "Spinoza"]
    for i, correctly_sorted_field in enumerate(correctly_sorted_fields):
        assert result.records[i][1] == correctly_sorted_field


def test_ordering_no_key():
    with pytest.raises(Exception):
        result, _ = execute_query("tau assistenten;")[0]


def test_ordering_malformed_key():
    with pytest.raises(Exception):
        result, _ = execute_query("tau Ran professoren;")[0]
    with pytest.raises(Exception):
        result, _ = execute_query("tau profesoren.Rang professoren;")[0]
