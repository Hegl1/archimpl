import pytest
from mosaic import query_executor
from mosaic import table_service


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.initialize()
    table_service.load_tables_from_directory("./data/kemper02")


def test_indices_table_schema():
    result, _ = query_executor.execute_query('#indices;', False)[0]
    assert result.schema.column_names == ["#indices.name", "#indices.table", "#indices.column"]


@pytest.mark.parametrize(
    'query,column_names,result_records',
    [
        ('sigma Name = "C3" pi Name as Rang professoren;', ['Name'], [["C3"], ["C3"], ["C3"]]),
        ('sigma Name = "Fichte" (pi Name professoren union pi Name studenten);', ['professoren.Name'], [["Fichte"]])
    ],
)
def test_milestone_4_query_output_same_if_optimized(query, column_names, result_records):
    _test_query(query, column_names, result_records)


def _test_query(query, column_names, result_records):
    for optimize in [True, False]:
        results = query_executor.execute_query(query, optimize)
        assert len(results) == 1
        result, _ = results[0]

        assert result.schema.column_names == column_names
        assert result.records == result_records


@pytest.mark.parametrize(
    'query,result_not_optimized,result_optimized',
    [
        ('explain sigma PersNr = 2126 (professoren join professoren.PersNr = vorlesungen.gelesenVon vorlesungen);',
         [['-->Selection(condition=(professoren.PersNr = 2126))'],
          ['---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))'],
          ['------>TableScan(professoren)'],
          ['------>TableScan(vorlesungen)']],
         [['-->HashJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))'],
          ['---->IndexSeek(professoren_PersNr, condition=(professoren.PersNr = 2126))'],
          ['---->TableScan(vorlesungen)']]),
        ('explain sigma Name = "Fichte" (pi Name professoren union pi Name studenten);',
         [['-->Selection(condition=(professoren.Name = "Fichte"))'],
          ['---->Union'],
          ['------>Projection(columns=[professoren.Name=professoren.Name])'],
          ['-------->TableScan(professoren)'],
          ['------>Projection(columns=[studenten.Name=studenten.Name])'],
          ['-------->TableScan(studenten)']],
         [['-->Union'],
          ['---->Projection(columns=[professoren.Name=professoren.Name])'],
          ['------>Selection(condition=(professoren.Name = "Fichte"))'],
          ['-------->TableScan(professoren)'],
          ['---->Projection(columns=[studenten.Name=studenten.Name])'],
          ['------>IndexSeek(studenten_Name, condition=(studenten.Name = "Fichte"))']])
    ],
)
def test_milestone_4_optimize_query(query, result_not_optimized, result_optimized):
    _test_explain_optimize_query_explain_output(query, result_not_optimized, result_optimized)


def _test_explain_optimize_query_explain_output(query, result_not_optimized, result_optimized):
    for optimize in [True, False]:
        results = query_executor.execute_query(query, optimize)

        assert len(results) == 1
        result, _ = results[0]
        assert len(result.schema.column_names) == 1

        if optimize:
            assert result.records == result_optimized
        else:
            assert result.records == result_not_optimized
