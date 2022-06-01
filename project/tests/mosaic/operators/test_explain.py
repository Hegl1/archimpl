import pytest
from mosaic.query_executor import execute_query
from mosaic import table_service


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_explain_table_scan():
    result, _ = execute_query("explain #tables;")[0]
    assert len(result) == 1
    assert result.records[0][0] == "-->TableScan(#tables)"


def test_explain_table_scan_rename():
    result, _ = execute_query("explain #tables as tabs;")[0]
    assert len(result) == 1
    assert result.records[0][0] == "-->TableScan(table_name=#tables, alias=tabs)"


def test_explain_table_scan_rename_projection():  # tests get_schema in table_scan
    result, _ = execute_query("explain pi table_name #tables as tabs;")[0]
    assert len(result) == 2
    assert result.records[0][0] == "-->Projection(columns=[tabs.table_name=tabs.table_name])"
    assert result.records[1][0] == "---->TableScan(table_name=#tables, alias=tabs)"


def test_explain_projection():
    result, _ = execute_query("explain pi MatrNr, Name studenten;")[0]
    assert len(result) == 2
    assert result.records[0][0] == "-->Projection(columns=[studenten.MatrNr=studenten.MatrNr, studenten.Name=studenten.Name])"
    assert result.records[1][0] == "---->TableScan(studenten)"


def test_explain_projection_distinct():
    result, _ = execute_query("explain pi distinct MatrNr, Name studenten;")[0]
    assert len(result) == 3
    assert result.records[0][0] == "-->HashDistinct"


def test_explain_projection_distinct_projection():  # tests get_schema of distinct
    result, _ = execute_query("explain pi MatrNr (pi distinct MatrNr, Name studenten);")[0]
    assert len(result) == 4
    assert result.records[0][0] == "-->Projection(columns=[studenten.MatrNr=studenten.MatrNr])"
    assert result.records[1][0] == "---->HashDistinct"


def test_explain_selection():
    result, _ = execute_query("explain sigma Rang > \"C3\" professoren;")[0]
    assert len(result) == 2
    assert result.records[0][0] == "-->Selection(condition=(professoren.Rang > \"C3\"))"
    assert result.records[1][0] == "---->TableScan(professoren)"


def test_explain_union():
    result, _ = execute_query("explain pi VorlNr as Vorgaenger voraussetzen union pi VorlNr vorlesungen;")[0]
    assert len(result) == 5
    assert result.records[0][0] == "-->Union"
    assert result.records[1][0] == "---->Projection(columns=[VorlNr=voraussetzen.Vorgaenger])"
    assert result.records[2][0] == "------>TableScan(voraussetzen)"
    assert result.records[3][0] == "---->Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[4][0] == "------>TableScan(vorlesungen)"


def test_explain_union_projection():  # tests get_schema of union
    result, _ = execute_query("explain pi VorlNr (pi VorlNr as Vorgaenger voraussetzen union pi VorlNr vorlesungen);")[0]
    assert len(result) == 6
    assert result.records[0][0] == "-->Projection(columns=[VorlNr=VorlNr])"
    assert result.records[1][0] == "---->Union"
    assert result.records[2][0] == "------>Projection(columns=[VorlNr=voraussetzen.Vorgaenger])"
    assert result.records[3][0] == "-------->TableScan(voraussetzen)"
    assert result.records[4][0] == "------>Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[5][0] == "-------->TableScan(vorlesungen)"


def test_explain_intersect():
    result, _ = execute_query("explain pi VorlNr vorlesungen intersect pi VorlNr as Vorgaenger voraussetzen;")[0]
    assert len(result) == 5
    assert result.records[0][0] == "-->Intersect"
    assert result.records[1][0] == "---->Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[2][0] == "------>TableScan(vorlesungen)"
    assert result.records[3][0] == "---->Projection(columns=[VorlNr=voraussetzen.Vorgaenger])"
    assert result.records[4][0] == "------>TableScan(voraussetzen)"


def test_explain_intersect_projection():  # tests get_schema of intersect
    result, _ = execute_query("explain pi VorlNr (pi VorlNr vorlesungen intersect pi VorlNr as Vorgaenger voraussetzen);")[0]
    assert len(result) == 6
    assert result.records[0][0] == "-->Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[1][0] == "---->Intersect"
    assert result.records[2][0] == "------>Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[3][0] == "-------->TableScan(vorlesungen)"
    assert result.records[4][0] == "------>Projection(columns=[VorlNr=voraussetzen.Vorgaenger])"
    assert result.records[5][0] == "-------->TableScan(voraussetzen)"


def test_explain_difference():
    result, _ = execute_query("explain pi VorlNr as Vorgaenger voraussetzen except pi VorlNr vorlesungen;")[0]
    assert len(result) == 5
    assert result.records[0][0] == "-->Except"
    assert result.records[1][0] == "---->Projection(columns=[VorlNr=voraussetzen.Vorgaenger])"
    assert result.records[2][0] == "------>TableScan(voraussetzen)"
    assert result.records[3][0] == "---->Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[4][0] == "------>TableScan(vorlesungen)"


def test_explain_difference_projection():  # tests get_schema of difference
    result, _ = execute_query("explain pi VorlNr (pi VorlNr as Vorgaenger voraussetzen except pi VorlNr vorlesungen);")[0]
    assert len(result) == 6
    assert result.records[0][0] == "-->Projection(columns=[VorlNr=VorlNr])"
    assert result.records[1][0] == "---->Except"
    assert result.records[2][0] == "------>Projection(columns=[VorlNr=voraussetzen.Vorgaenger])"
    assert result.records[3][0] == "-------->TableScan(voraussetzen)"
    assert result.records[4][0] == "------>Projection(columns=[vorlesungen.VorlNr=vorlesungen.VorlNr])"
    assert result.records[5][0] == "-------->TableScan(vorlesungen)"

def test_explain_cross_join():
    result, _ = execute_query("explain pi PersNr, Name professoren cross join pi PersNr, Name, Boss assistenten;")[0]
    assert len(result) == 5
    assert result.records[0][0] == "-->NestedLoopsJoin(cross, natural=True, condition=None)"
    assert result.records[1][0] == "---->Projection(columns=[professoren.PersNr=professoren.PersNr, professoren.Name=professoren.Name])"
    assert result.records[2][0] == "------>TableScan(professoren)"
    assert result.records[3][0] == "---->Projection(columns=[assistenten.PersNr=assistenten.PersNr, assistenten.Name=assistenten.Name, assistenten.Boss=assistenten.Boss])"
    assert result.records[4][0] == "------>TableScan(assistenten)"


def test_explain_ordering():
    result, _ = execute_query("explain tau PersNr professoren;")[0]
    assert len(result) == 2
    assert result[0][0] == "-->OrderBy(key=[professoren.PersNr])"
    assert result[1][0] == "---->TableScan(professoren)"


def test_explain_arithmetic():
    result, _ = execute_query("explain pi col as (null + 4) professoren;")[0]
    assert len(result) == 2
    assert result[0][0] == "-->Projection(columns=[col={NULL + 4}])"
    assert result[1][0] == "---->TableScan(professoren)"


def test_explain_conjunctive():
    result, _ = execute_query('explain sigma Rang > "C3" and PersNr > 10 professoren;')[0]
    assert len(result) == 2
    assert result[0][0] == '-->Selection(condition=((professoren.Rang > "C3") and (professoren.PersNr > 10)))'
    assert result[1][0] == "---->TableScan(professoren)"


def test_explain_disjunctive():
    result, _ = execute_query('explain sigma Rang > "C3" or PersNr > 10 professoren;')[0]
    assert len(result) == 2
    assert result[0][0] == '-->Selection(condition=((professoren.Rang > "C3") or (professoren.PersNr > 10)))'
    assert result[1][0] == "---->TableScan(professoren)"

def test_explain_aggregate():
    result,_ = execute_query('explain gamma Rang aggregate Anzahl as count(PersNr) professoren;')[0]
    assert len(result) == 2
    assert result[0][0] == "-->Aggregation(groups=[professoren.Rang=Rang],aggregates=[COUNT(professoren.PersNr) -> Anzahl])"
    assert result[1][0] == "---->TableScan(professoren)"