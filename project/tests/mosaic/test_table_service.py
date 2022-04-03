import pytest
from mosaic import table_service


@pytest.fixture(autouse=True)
def clean_loaded_tables():
    table_service._tables = dict()


def test_load_from_file():
    assert len(table_service._tables) == 0
    table_service.load_from_file("./tests/testdata/studenten.table")
    assert len(table_service._tables) == 1
    table = table_service._tables["studenten"]
    assert table is not None
    assert table.table_name == "studenten"
    assert table.schema_names == ["studenten.MatrNr", "studenten.Name", "studenten.Semester"]
    assert table.schema_types == [table_service.SchemaType.INT, table_service.SchemaType.VARCHAR,
                                  table_service.SchemaType.INT]
    assert table.records[0]["studenten.MatrNr"] == 24002


def test_load_tables_from_directory():
    assert len(table_service._tables) == 0
    not_loaded = table_service.load_tables_from_directory("./tests/testdata/")
    assert len(table_service._tables) == 3  # incl. #tables and #columns
    assert len(not_loaded) == 1
    with pytest.raises(table_service.NoTableLoadedException):
        table_service.load_tables_from_directory("./")


def test_retrieve():
    table_service.load_tables_from_directory("./tests/testdata/")
    assert table_service.retrieve("studenten") is not None
    assert len(table_service.retrieve("studenten").records) == 8
    assert table_service.retrieve("studenten").schema_names == ["studenten.MatrNr", "studenten.Name",
                                                                "studenten.Semester"]
    with pytest.raises(table_service.TableNotFoundException):
        table_service.retrieve("notFoundTable")
    assert table_service.retrieve("#tables") is not None
    assert len(table_service.retrieve("#tables").records) == 3
    assert table_service.retrieve("#columns") is not None
    assert len(table_service.retrieve("#columns").records) == 8


def test_table_exists():
    assert table_service.table_exists("#tables")
    assert not table_service.table_exists("notExists")

