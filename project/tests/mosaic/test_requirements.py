import pytest
from mosaic import query_executor
from mosaic import table_service
from mosaic.cli import CliErrorMessageException


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.initialize()
    table_service.load_tables_from_directory("./data/kemper")


def _test_query(query, column_names, result_rows):
    for optimize in [True, False]:
        results = query_executor.execute_query(query, optimize)

        assert len(results) == 1

        result, _ = results[0]

        assert result.schema.column_names == column_names
        assert len(result) == result_rows


@pytest.mark.parametrize(
    'query,column_names,result_rows',
    [
        ('sigma test != null and test <= 58237.5 pi test as (MatrNr * 2) - 5.0 / 2, test2 as null hoeren;', ['test', 'test2'], 10),
        ('pi col as (null + 2) hoeren;', ['col'], 10),
    ],
)
def test_queries(query, column_names, result_rows):
    _test_query(query, column_names, result_rows)


def test_ambiguous_column_name():
    with pytest.raises(CliErrorMessageException):
        _test_query('pi Name (professoren cross join assistenten);', [], 0)


# Milestone 2 queries


@pytest.mark.parametrize(
    'query,column_names,result_rows',
    [
        ('pi column_name, #columns.data_type, ordinal_position #columns;', ['#columns.column_name', '#columns.data_type', '#columns.ordinal_position'], 28),
        ('pi PersNr, Name, FullName as "Prof. " + professoren.Name, NewName as Name professoren;', ['professoren.PersNr', 'professoren.Name', 'FullName', 'NewName'], 7),
        ('sigma Rang > "C3" professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 4),
        ('professoren as employee;', ['employee.PersNr', 'employee.Name', 'employee.Rang', 'employee.Raum'], 7),
        ('pi PersNr, Name, Stelle as "Professor" professoren union pi PersNr, Name, Stelle as "Assistent" assistenten;', ['professoren.PersNr', 'professoren.Name', 'Stelle'], 13),
        ('sigma PersNr < 2135 professoren intersect sigma PersNr > 2125 professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 4),
        ('professoren except sigma PersNr >= 2127 professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 2),
        ('pi Rang, Name professoren cross join pi Name assistenten;', ['professoren.Rang', 'professoren.Name', 'assistenten.Name'], 42),
        ('tau Name pi PersNr, Name assistenten;', ['assistenten.PersNr', 'assistenten.Name'], 6),
        ('explain pi PersNr, Name professoren;', ['Operator'], 2),
        ('explain pi PersNr, Name professoren cross join pi PersNr, Name, Boss assistenten;', ['Operator'], 5),
        ('#columns;', ['#columns.table_name', '#columns.column_name', '#columns.ordinal_position', '#columns.data_type'], 28),
        ('#tables;', ['#tables.table_name'], 9),
        ('professoren as profs;', ['profs.PersNr', 'profs.Name', 'profs.Rang', 'profs.Raum'], 7),
        ('pi profs.Name professoren as profs;', ['profs.Name'], 7),
        ('pi distinct table_name #columns;', ['#columns.table_name'], 9),
        ('sigma table_name = "pruefen" #columns;', ['#columns.table_name', '#columns.column_name', '#columns.ordinal_position', '#columns.data_type'], 4),
        ('sigma Rang = "C4" professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 4),
        ('sigma Rang = "C4" and Raum >= "3" professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 2),
        ('sigma Rang = "C4" or Name = "Kopernikus" professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 5),
        ('pi Name professoren union pi Name studenten;', ['professoren.Name'], 15),
        ('studenten cross join hoeren;', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.MatrNr', 'hoeren.VorlNr'], 80),
        ('tau Rang professoren;', ['professoren.PersNr', 'professoren.Name', 'professoren.Rang', 'professoren.Raum'], 7),
        ('explain (pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren));', ['Operator'], 5),
        ('sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren);', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.MatrNr', 'hoeren.VorlNr'], 10),
        (
            '(pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren)) except ' +
            '(pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren));',
            ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.VorlNr'], 0
        ),
        ('((pi VorlNr sigma MatrNr = 29120 hoeren) intersect (pi VorlNr sigma gelesenVon = 2125 vorlesungen));', ['hoeren.VorlNr'], 2),
    ],
)
def test_milestone_2_query(query, column_names, result_rows):
    _test_query(query, column_names, result_rows)


# Milestone 3 queries


@pytest.mark.parametrize(
    'query,column_names,result_rows',
    [
        ('pi PersNr, Name professoren join professoren.PersNr = assistenten.Boss pi PersNr, Name, Boss assistenten;', ['professoren.PersNr', 'professoren.Name', 'assistenten.PersNr', 'assistenten.Name', 'assistenten.Boss'], 6),
        ('pi PersNr, Name professoren left join professoren.PersNr = assistenten.Boss pi PersNr, Name, Boss assistenten;', ['professoren.PersNr', 'professoren.Name', 'assistenten.PersNr', 'assistenten.Name', 'assistenten.Boss'], 9),
        ('pi VorlNr, Titel vorlesungen natural join pi VorlNr, MatrNr, Note pruefen;', ['vorlesungen.VorlNr', 'vorlesungen.Titel', 'pruefen.MatrNr', 'pruefen.Note'], 3),
        ('pi VorlNr, Titel vorlesungen natural left join pi VorlNr, MatrNr, Note pruefen;', ['vorlesungen.VorlNr', 'vorlesungen.Titel', 'pruefen.MatrNr', 'pruefen.Note'], 10),
        ('gamma Rang aggregate Anzahl as count(PersNr) professoren;', ['professoren.Rang', 'Anzahl'], 2),
        ('studenten natural join hoeren;', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.VorlNr'], 10),
        ('studenten natural left join hoeren;', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.VorlNr'], 14),
        ('studenten left join studenten.MatrNr = hoeren.MatrNr hoeren;', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.MatrNr', 'hoeren.VorlNr'], 14),
        ('gamma Semester aggregate Anzahl as count(MatrNr) studenten;', ['studenten.Semester', 'Anzahl'], 7),
        ('gamma aggregate AvgSemester as avg(Semester), MinSemester as min(Semester), MaxSemester as max(Semester) studenten;', ['AvgSemester', 'MinSemester', 'MaxSemester'], 1),
        ('sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren);', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.MatrNr', 'hoeren.VorlNr'], 10),
        ('(pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren)) except (studenten natural join hoeren);', ['studenten.MatrNr', 'studenten.Name', 'studenten.Semester', 'hoeren.VorlNr'], 0),
        ('((pi VorlNr sigma MatrNr = 29120 hoeren) intersect (pi VorlNr sigma gelesenVon = 2125 vorlesungen)) natural join vorlesungen;', ['hoeren.VorlNr', 'vorlesungen.Titel', 'vorlesungen.SWS', 'vorlesungen.gelesenVon'], 2)
    ],
)
def test_milestone_3_query(query, column_names, result_rows):
    _test_query(query, column_names, result_rows)


@pytest.mark.parametrize(
    'query,result_not_optimized,result_optimized',
    [
        ('explain sigma Rang = "C3" and Raum > "200" and SWS = 3 (professoren join PersNr = gelesenVon vorlesungen);',
         [['-->Selection(condition=((professoren.Rang = "C3") AND (professoren.Raum > "200") AND (vorlesungen.SWS = 3)))'],
          ['---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))'],
          ['------>TableScan(professoren)'],
          ['------>TableScan(vorlesungen)']],
         [['-->HashJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))'],
          ['---->Selection(condition=((professoren.Rang = "C3") AND (professoren.Raum > "200")))'],
          ['------>TableScan(professoren)'],
          ['---->Selection(condition=(vorlesungen.SWS = 3))'],
          ['------>TableScan(vorlesungen)']]),
        ('explain sigma Name > "K" (pi Name professoren union pi Name studenten);',
         [['-->Selection(condition=(professoren.Name > "K"))'],
          ['---->Union'],
          ['------>Projection(columns=[professoren.Name=professoren.Name])'],
          ['-------->TableScan(professoren)'],
          ['------>Projection(columns=[studenten.Name=studenten.Name])'],
          ['-------->TableScan(studenten)']],
         [['-->Union'],
          ['---->Projection(columns=[professoren.Name=professoren.Name])'],
          ['------>Selection(condition=(professoren.Name > "K"))'],
          ['-------->TableScan(professoren)'],
          ['---->Projection(columns=[studenten.Name=studenten.Name])'],
          ['------>Selection(condition=(studenten.Name > "K"))'],
          ['-------->TableScan(studenten)']])
    ],
)
def test_milestone_3_optimize_query(query, result_not_optimized, result_optimized):
    _test_explain_optimize_query(query, result_not_optimized, result_optimized)


def _test_explain_optimize_query(query, result_not_optimized, result_optimized):
    for optimize in [True, False]:
        results = query_executor.execute_query(query, optimize)

        assert len(results) == 1
        result, _ = results[0]
        assert len(result.schema.column_names) == 1

        if optimize:
            assert result.records == result_optimized
        else:
            assert result.records == result_not_optimized


# Milestone 3 speedup tests - disable if needed to save time when running tests

@pytest.mark.parametrize(
    'query',
    [
        ('sigma studenten.Name = "Theophrastos" and vorlesungen.SWS = 4 and professoren.Name = "Sokrates" and assistenten.Name = "Platon" (studenten cross join vorlesungen cross join professoren cross join assistenten cross join pruefen cross join voraussetzen cross join hoeren);'),
        ('sigma Name > "K" ((pi studenten.Name (sigma studenten.Name = "Theophrastos" and vorlesungen.SWS = 4 and professoren.Name = "Sokrates" and assistenten.Name = "Platon" (studenten cross join vorlesungen cross join professoren cross join assistenten cross join pruefen cross join voraussetzen cross join hoeren))) union (pi Name professoren));')
    ],
)
def test_milestone_3_optimize_query_execution_time(query):
    _test_optimize_query_execution_time(query)


def _test_optimize_query_execution_time(query):
    times = [0, 0]
    for i, optimize in enumerate([True, False]):
        _, execution_time = query_executor.execute_query(query, optimize)[0]
        times[i] = execution_time
    assert times[1] / times[0] > 1000
