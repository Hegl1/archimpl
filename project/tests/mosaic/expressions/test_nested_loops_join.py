from mosaic import table_service
from mosaic.query_executor import execute_query


def test_cross_join():
    result, _ = execute_query("pi Rang, Name professoren cross join pi Name assistenten;")[0]
    table1 = table_service.retrieve("professoren", makeCopy=False)
    table2 = table_service.retrieve("assistenten", makeCopy=False)

    assert len(result) == len(table1) * len(table2)
    assert len(result.schema_names) == 3
    assert ["C4", "Sokrates", "Platon"] in result.records
    assert ["C4", "Sokrates", "Spinoza"] in result.records
    assert ["C4", "Kant", "Spinoza"] in result.records
