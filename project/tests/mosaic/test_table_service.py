import pytest
from mosaic import table_service


@pytest.fixture(autouse=True)
def clean_loaded_tables():
    table_service.initialize()


def test_load_from_file():
    assert len(table_service._tables) == 2
    table_service.load_from_file("./tests/testdata/studenten.table")
    assert len(table_service._tables) == 3
    table = table_service._tables["studenten"]
    assert table is not None
    assert table.table_name == "studenten"
    assert table.schema_names == ["studenten.MatrNr", "studenten.Name", "studenten.Semester"]
    assert table.schema_types == [table_service.SchemaType.INT, table_service.SchemaType.VARCHAR,
                                  table_service.SchemaType.INT]
    assert table[0, "studenten.MatrNr"] == 24002


def test_load_tables_from_directory():
    assert len(table_service._tables) == 2
    not_loaded = table_service.load_tables_from_directory("./tests/testdata/")
    assert len(table_service._tables) == 5  # incl. #tables and #columns
    assert len(not_loaded) == 1
    with pytest.raises(table_service.NoTableLoadedException):
        table_service.load_tables_from_directory("./")


def test_retrieve():
    table_service.load_tables_from_directory("./tests/testdata/")
    assert table_service.retrieve("studenten") is not None
    assert len(table_service.retrieve("studenten").records) == 8
    assert table_service.retrieve("studenten").schema_names == ["studenten.MatrNr", "studenten.Name", "studenten.Semester"]
    with pytest.raises(table_service.TableNotFoundException):
        table_service.retrieve("notFoundTable")
    assert table_service.retrieve("#tables") is not None
    assert len(table_service.retrieve("#tables").records) == 5
    assert table_service.retrieve("#columns") is not None
    assert len(table_service.retrieve("#columns").records) == 14


def test_table_exists():
    assert table_service.table_exists("#tables")
    assert not table_service.table_exists("notExists")


def test_table_get_column_index():
    table = table_service.retrieve("#columns")
    assert table is not None

    assert table.get_column_index("#columns.column_name") == 1 # FQN
    assert table.get_column_index("column_name") == 1 # simple name

    with pytest.raises(table_service.TableIndexException):
        table.get_column_index("notFoundIndex")

def test_table_get_item():
    table = table_service.retrieve("#columns")
    assert table is not None

    assert len(table[0]) == 4
    assert table[0, "ordinal_position"] == 0
    assert table[0, "#columns.table_name"] == "#tables"
    assert len(table[0:2]) == 2
    assert table[1:5, "column_name"] == ["#columns.table_name", "#columns.column_name", "#columns.ordinal_position", "#columns.data_type"]