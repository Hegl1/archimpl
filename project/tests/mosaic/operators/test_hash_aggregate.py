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
    assert result.records == [[2125,6005],[2126,3004],[2127,6011],[2134,3007]]

def test_no_group_aggregation():
    result, _ = execute_query(
        "gamma aggregate AvgSemester as avg(Semester), MinSemester as min(Semester), MaxSemester as max(Semester) studenten;")[0]
    assert result.schema.column_names == [
        "AvgSemester", "MinSemester", "MaxSemester"]
    assert result.schema.column_types == [
        table_service.SchemaType.FLOAT, table_service.SchemaType.INT, table_service.SchemaType.INT]
    assert result.records == [[7.625,2,18]]


def test_invalid_varchar_aggregations():
    with pytest.raises(CliErrorMessageException, match="Varchar can only be aggregated with count, min and max"):
        execute_query(
            "gamma Boss aggregate SumName as sum(Name) assistenten;")[0]
    with pytest.raises(CliErrorMessageException, match="Varchar can only be aggregated with count, min and max"):
        execute_query(
            "gamma Boss aggregate AvgName as avg(Name) assistenten;")[0]


def test_valid_varchar_aggregations():
    result, _ = execute_query(
        "gamma Boss aggregate MaxName as max(Name), MinName as min(Name), CountName as count(Name) assistenten;")[0]
    assert result.schema.column_names == [
        "assistenten.Boss", "MaxName", "MinName", "CountName"]
    assert result.schema.column_types == [
        table_service.SchemaType.INT, table_service.SchemaType.VARCHAR, table_service.SchemaType.VARCHAR, table_service.SchemaType.INT]
