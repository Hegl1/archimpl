from mosaic import table_service
from mosaic.query_executor import execute_query


def test_selection_get_result():
    result, _ = execute_query("sigma Rang > \"C3\" professoren;")[0]
    for record in result.records:
        assert record[2] > "C3"


def test_selection_with_literal():
    table = table_service.retrieve_table("professoren")

    result, _ = execute_query("sigma 0 professoren;")[0]
    assert len(result) == 0
    result, _ = execute_query("sigma 1 professoren;")[0]
    assert len(result) == len(table)


def test_selection_with_column_name():
    table = table_service.retrieve_table("#columns")

    result, _ = execute_query("sigma ordinal_position #columns;")[0]
    assert len(result) < len(table)

    for i in range(len(result)):
        assert result[i, "ordinal_position"]
