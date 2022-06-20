from mosaic.cli import CliErrorMessageException
from mosaic.query_executor import execute_query
from mosaic import table_service
import pytest


def test_sum_aggregation():
    result, _ = execute_query(
        "gamma Boss aggregate SumPersNr as sum(PersNr) assistenten;")[0]
    assert result.schema.column_names == [
        "assistenten.Boss", "SumPersNr"]
    assert result.schema.column_types == [
        table_service.SchemaType.INT, table_service.SchemaType.INT]
    assert result.records == [[2125, 6005], [
        2126, 3004], [2127, 6011], [2134, 3007]]


def test_no_group_aggregation():
    result, _ = execute_query(
        "gamma aggregate AvgSemester as avg(Semester), MinSemester as min(Semester), MaxSemester as max(Semester) studenten;")[
        0]
    assert result.schema.column_names == [
        "AvgSemester", "MinSemester", "MaxSemester"]
    assert result.schema.column_types == [
        table_service.SchemaType.FLOAT, table_service.SchemaType.INT, table_service.SchemaType.INT]
    assert result.records == [[7.625, 2, 18]]


def test_invalid_varchar_aggregation():
    with pytest.raises(CliErrorMessageException, match="Varchar can only be aggregated with count, min and max"):
        execute_query(
            "gamma Boss aggregate SumName as sum(Name) assistenten;")[0]
    with pytest.raises(CliErrorMessageException, match="Varchar can only be aggregated with count, min and max"):
        execute_query(
            "gamma Boss aggregate AvgName as avg(Name) assistenten;")[0]


def test_valid_varchar_aggregation():
    result, _ = execute_query(
        "gamma Boss aggregate MaxName as max(Name), MinName as min(Name), CountName as count(Name) assistenten;")[0]
    assert result.schema.column_names == [
        "assistenten.Boss", "MaxName", "MinName", "CountName"]
    assert result.schema.column_types == [
        table_service.SchemaType.INT, table_service.SchemaType.VARCHAR, table_service.SchemaType.VARCHAR,
        table_service.SchemaType.INT]


def test_literal_addition_aggregation():
    result, _ = execute_query(
        'gamma z as Boss + "hallo" aggregate Anzahl as count(PersNr) assistenten;')[0]
    assert result.schema.column_names == [
        "z", "Anzahl"]
    assert result.schema.column_types == [
        table_service.SchemaType.VARCHAR, table_service.SchemaType.INT]
    assert result.records == [["2125hallo", 2], [
        "2126hallo", 1], ["2127hallo", 2], ["2134hallo", 1]]


def test_literal_aggregation():
    result, _ = execute_query(
        'gamma test as "hallo" aggregate Anzahl as count(PersNr) assistenten;')[0]
    assert result.schema.column_names == [
        "test", "Anzahl"]
    assert result.schema.column_types == [
        table_service.SchemaType.VARCHAR, table_service.SchemaType.INT]
    assert result.records == [["hallo", 6]]


def test_int_subtraction_aggregation():
    result, _ = execute_query(
        'gamma sub as VorlNr - SWS aggregate TitleCount as count(Titel) vorlesungen;')[0]
    assert result.schema.column_names == [
        "sub", "TitleCount"]
    assert result.schema.column_types == [
        table_service.SchemaType.INT, table_service.SchemaType.INT]
    assert result.records == [[4997, 2], [5037, 1], [5040, 1], [5047, 1], [4048, 1], [5049, 1], [5214, 1], [5257, 1],
                              [5020, 1], [4626, 1]]
