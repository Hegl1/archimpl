import pytest

from mosaic.query_executor import execute_query


def test_ordering_single_key():
    result, _ = execute_query("tau Rang professoren;")[0]
    assert len(result.records) == 7
    correctly_sorted_fields = ["C3", "C3", "C3", "C4", "C4", "C4", "C4"]
    for i, correctly_sorted_field in enumerate(correctly_sorted_fields):
        assert result.records[i][2] == correctly_sorted_field

    result, _ = execute_query("tau assistenten.Boss assistenten;")[0]
    correctly_sorted_fields = [2125, 2125, 2126, 2127, 2127, 2134]
    for i, correctly_sorted_field in enumerate(correctly_sorted_fields):
        assert result.records[i][3] == correctly_sorted_field

    result, _ = execute_query("tau Name pi PersNr, Name assistenten;")[0]
    correctly_sorted_fields = ["Aristoteles", "Newton", "Platon", "Rhetikus", "Spinoza", "Wittgenstein"]
    for i, correctly_sorted_field in enumerate(correctly_sorted_fields):
        assert result.records[i][1] == correctly_sorted_field


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
