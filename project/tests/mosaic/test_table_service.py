import pytest
from mosaic import table_service


def test_load_tables_from_directory():
    table_service.load_tables_from_directory("./data/kemper/")
    assert len(table_service._tables) == 7


def test_retrieve():
    assert len(table_service.retrieve("studenten").records) == 8
