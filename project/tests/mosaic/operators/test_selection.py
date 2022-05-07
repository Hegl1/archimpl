from mosaic.query_executor import execute_query
import pytest


def test_selection_get_result():
    result, _ = execute_query("sigma Rang > \"C3\" professoren;")[0]
    for record in result.records:
        assert record[2] > "C3"
