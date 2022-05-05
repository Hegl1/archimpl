import pytest
from mosaic import query_executor
from mosaic import table_service
from mosaic.cli import CliErrorMessageException


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.initialize()
    table_service.load_tables_from_directory("./data/kemper")


def _test_query(query, column_names, result_rows):
    results = query_executor.execute_query(query)

    assert len(results) == 1

    result, _ = results[0]

    print(result.records)
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
