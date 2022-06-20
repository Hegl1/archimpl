import pytest
from mosaic import table_service


@pytest.fixture(autouse=True)
def clean_loaded_tables():
    table_service.initialize()


def test_load_from_file():
    assert len(table_service._tables) == 3
    table_service.load_from_file("./tests/testdata/studenten.table")
    assert len(table_service._tables) == 4
    table = table_service._tables["studenten"]
    assert table is not None
    assert table.table_name == "studenten"
    assert table.schema.column_names == [
        "studenten.MatrNr", "studenten.Name", "studenten.Semester"]
    assert table.schema.column_types == [table_service.SchemaType.INT, table_service.SchemaType.VARCHAR,
                                         table_service.SchemaType.INT]
    assert table[0, "studenten.MatrNr"] == 24002


def test_load_tables_from_directory():
    assert len(table_service._tables) == 3
    not_loaded = table_service.load_tables_from_directory("./tests/testdata/")
    # incl. #tables and #columns and #indices
    assert len(table_service._tables) == 12
    assert len(not_loaded) == 17
    with pytest.raises(table_service.NoTableLoadedException):
        table_service.load_tables_from_directory("./")


def test_retrieve():
    table_service.load_tables_from_directory("./tests/testdata/")
    assert table_service.retrieve_table("studenten") is not None
    assert len(table_service.retrieve_table("studenten").records) == 8
    assert table_service.retrieve_table("studenten").schema.column_names == ["studenten.MatrNr", "studenten.Name",
                                                                             "studenten.Semester"]
    with pytest.raises(table_service.TableNotFoundException):
        table_service.retrieve_table("notFoundTable")
    assert table_service.retrieve_table("#tables") is not None
    assert len(table_service.retrieve_table("#tables").records) == 12
    assert table_service.retrieve_table("#columns") is not None
    assert len(table_service.retrieve_table("#columns").records) == 33


def test_table_exists():
    assert table_service.table_exists("#tables")
    assert not table_service.table_exists("notExists")


def test_table_get_column_index():
    table = table_service.retrieve_table("#columns")
    assert table is not None

    assert table.get_column_index("#columns.column_name") == 1  # FQN
    assert table.get_column_index("column_name") == 1  # simple name

    with pytest.raises(table_service.TableIndexException):
        table.get_column_index("notFoundIndex")


def test_table_get_item():
    table = table_service.retrieve_table("#columns")
    assert table is not None

    assert len(table[3]) == 4
    assert table[3, "ordinal_position"] == 0
    assert table[3, "#columns.table_name"] == "#tables"
    assert len(table[0:2]) == 2
    assert table[4:8, "column_name"] == ["#columns.table_name", "#columns.column_name", "#columns.ordinal_position",
                                         "#columns.data_type"]


def test_table_get_item_not_found():
    table = table_service.retrieve_table("#columns")
    assert table is not None

    tables = len(table)

    with pytest.raises(table_service.TableIndexException):
        table[tables]


def test_index_created_on_table_file_loading():
    table_service.load_from_file("./tests/testdata/correctIndex.table")
    assert len(table_service._indices) == 1
    assert table_service.index_exists("correctIndex", "MatrNr")
    assert len(table_service._indices["correctIndex"]) == 1
    assert len(table_service.retrieve_index("correctIndex", "MatrNr")) == 4


def test_retrieve_non_existing_index():
    table_service.load_from_file("./tests/testdata/correctIndex.table")
    assert len(table_service.retrieve_index("correctIndex", "MatrNr")) == 4
    with pytest.raises(table_service.IndexNotFoundException):
        table_service.retrieve_index("correctIndex", "WrongIndex")
        table_service.retrieve_index("studenten", "MatrNr")


def test_index_exists():
    table_service.load_from_file("./tests/testdata/correctIndex.table")
    assert table_service.index_exists("correctIndex", "MatrNr")
    assert not table_service.index_exists("correctIndex", "VorlNr")
    assert not table_service.index_exists("correctIndex", "WrongColumn")
    assert not table_service.index_exists("studenten", "MatrNr")


def test_index_section_empty():
    table_service.load_from_file("./tests/testdata/emptyIndex.table")
    assert len(table_service._indices) == 0


def test_no_newline_schema_index_section():
    with pytest.raises(table_service.TableParsingException):
        table_service.load_from_file(
            "./tests/testdata/noNewLineSchemaIndex.table")


def test_no_newline_schema_data_section():
    with pytest.raises(table_service.TableParsingException):
        table_service.load_from_file(
            "./tests/testdata/noNewLineIndexData.table")


def test_duplicated_column_name():
    with pytest.raises(table_service.TableParsingException):
        table_service.load_from_file("./tests/testdata/duplicatedColumn.table")


def test_bad_column_start():
    with pytest.raises(table_service.TableParsingException):
        table_service.load_from_file("./tests/testdata/badColumnStart.table")


def test_bad_column_name():
    with pytest.raises(table_service.TableParsingException):
        table_service.load_from_file("./tests/testdata/badColumnName.table")


def test_null_as_column_name():
    with pytest.raises(table_service.TableParsingException):
        table_service.load_from_file("./tests/testdata/nullColumnName.table")


def test_wrong_schema_type():
    with pytest.raises(table_service.WrongSchemaTypeException):
        table_service.get_schema_type(table_service.get_schema_type)
