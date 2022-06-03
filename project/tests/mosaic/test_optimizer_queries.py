import pytest
from mosaic import query_executor, table_service
from mosaic.compiler.operators.explain import Explain


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.initialize()
    table_service.load_tables_from_directory("./data/kemper")


def _execute_query(query, optimization = True):
    results = query_executor.execute_query(query, optimization)
    assert len(results) == 1
    return results[0][0]


def _check_query_result_same_optimization(query):
    result_normal = _execute_query(query, False)
    result_optimized = _execute_query(query, True)

    assert result_normal.schema.table_name == result_optimized.schema.table_name
    assert result_normal.schema.column_names == result_optimized.schema.column_names
    assert result_normal.schema.column_types == result_optimized.schema.column_types
    assert result_normal.records == result_optimized.records


def test_optimizer_selection_push_down_projection():
    query = "sigma m > 0 (pi m as MatrNr hoeren);"
    result = _execute_query(f"explain {query}")
    assert result[0][0] == "-->Projection(columns=[m=hoeren.MatrNr])"
    assert result[1][0] == "---->Selection(condition=(hoeren.MatrNr > 0))"
    assert result[2][0] == "------>TableScan(hoeren)"

    _check_query_result_same_optimization(query)


def test_optimizer_selection_push_down_cross_join():
    query = "sigma Name = \"Sokrates\" (hoeren cross join studenten);"
    result = _execute_query(f"explain {query}")
    assert result[0][0] == "-->NestedLoopsJoin(cross, natural=True, condition=None)"
    assert result[1][0] == "---->TableScan(hoeren)"
    assert result[2][0] == "---->Selection(condition=(studenten.Name = \"Sokrates\"))"
    assert result[3][0] == "------>TableScan(studenten)"

    _check_query_result_same_optimization(query)


def test_optimizer_selection_push_down_union():
    query = "sigma MatrNr > 0 (pi MatrNr hoeren union pi MatrNr studenten);"
    result = _execute_query(f"explain {query}")
    assert result[0][0] == "-->Union"
    assert result[1][0] == "---->Projection(columns=[hoeren.MatrNr=hoeren.MatrNr])"
    assert result[2][0] == "------>Selection(condition=(hoeren.MatrNr > 0))"
    assert result[3][0] == "-------->TableScan(hoeren)"
    assert result[4][0] == "---->Projection(columns=[studenten.MatrNr=studenten.MatrNr])"
    assert result[5][0] == "------>Selection(condition=(studenten.MatrNr > 0))"
    assert result[6][0] == "-------->TableScan(studenten)"

    _check_query_result_same_optimization(query)


def test_optimizer_selection_do_not_push_down_projection():
    query = "sigma m > 0 (pi m as 123 hoeren);"

    _check_query_result_same_optimization(f"explain {query}")
    _check_query_result_same_optimization(query)


def test_optimizer_selection_push_down_complex():
    query = "sigma MatrNr > 26120 and n = \"Sokrates\" and Raum != \"10\" and test = \"test\"" \
                "(tau MatrNr (pi MatrNr, n as Name, vnr as VorlNr, Raum, test as \"test\" (hoeren cross join professoren)));"
    result = _execute_query(f"explain {query}")
    assert result[0][0] == "-->OrderBy(key=[hoeren.MatrNr])"
    assert result[1][0] == "---->Selection(condition=(test = \"test\"))"
    assert result[2][0] == "------>Projection(columns=[hoeren.MatrNr=hoeren.MatrNr, n=professoren.Name, vnr=hoeren.VorlNr, professoren.Raum=professoren.Raum, test=\"test\"])"
    assert result[3][0] == "-------->NestedLoopsJoin(cross, natural=True, condition=None)"
    assert result[4][0] == "---------->Selection(condition=(hoeren.MatrNr > 26120))"
    assert result[5][0] == "------------>TableScan(hoeren)"
    assert result[6][0] == "---------->Selection(condition=((professoren.Raum != \"10\") AND (professoren.Name = \"Sokrates\")))"
    assert result[7][0] == "------------>TableScan(professoren)"

    _check_query_result_same_optimization(query)


def test_optimizer_selection_push_through_projection_fqn():
    query = 'sigma Name > "K" (pi professoren.Name (pi professoren.Name, professoren.Rang (professoren cross join assistenten)));'
    result = _execute_query(f"explain {query}")
    assert result[0][0] == "-->Projection(columns=[professoren.Name=professoren.Name])"
    assert result[1][0] == "---->Projection(columns=[professoren.Name=professoren.Name, professoren.Rang=professoren.Rang])"
    assert result[2][0] == "------>NestedLoopsJoin(cross, natural=True, condition=None)"
    assert result[3][0] == '-------->Selection(condition=(professoren.Name > "K"))'
    assert result[4][0] == "---------->TableScan(professoren)"
    assert result[5][0] == "-------->TableScan(assistenten)"