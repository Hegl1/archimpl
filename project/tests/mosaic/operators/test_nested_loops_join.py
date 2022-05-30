import pytest

from mosaic import table_service
from mosaic.compiler.operators.nested_loops_join_operators import SelfJoinWithoutRenamingException, NestedLoopsJoin, \
    JoinType
from mosaic.compiler.operators.table_scan_operator import TableScan
from mosaic.query_executor import execute_query


def test_cross_join():
    result, _ = execute_query("pi Rang, Name professoren cross join pi Name assistenten;")[0]
    table1 = table_service.retrieve("professoren", makeCopy=False)
    table2 = table_service.retrieve("assistenten", makeCopy=False)

    assert len(result) == len(table1) * len(table2)
    assert len(result.schema.column_names) == 3
    assert ["C4", "Sokrates", "Platon"] in result.records
    assert ["C4", "Sokrates", "Spinoza"] in result.records
    assert ["C4", "Kant", "Spinoza"] in result.records


def test_table_self_join_without_renaming():
    table1 = TableScan("professoren")
    with pytest.raises(SelfJoinWithoutRenamingException):
        NestedLoopsJoin(table1, table1, JoinType.CROSS, condition=None, is_natural=False)

    table2 = TableScan("assistenten", "professoren")
    with pytest.raises(SelfJoinWithoutRenamingException):
        NestedLoopsJoin(table1, table2, JoinType.CROSS, condition=None, is_natural=False)


def test_table_self_join_with_renaming():
    table1 = TableScan("professoren")
    table2 = TableScan("professoren", "p")
    join = NestedLoopsJoin(table1, table2, JoinType.CROSS, condition=None, is_natural=False)
    try:
        join.get_result()
    except SelfJoinWithoutRenamingException as e:
        assert False


def test_inner_join():
    result, _ = execute_query("studenten join studenten.MatrNr = hoeren.MatrNr hoeren;")[0]
    table1 = table_service.retrieve("studenten", makeCopy=False)
    table2 = table_service.retrieve("hoeren", makeCopy=False)

    assert len(result) == 10
    assert len(result.schema.column_names) == len(table1.schema.column_names) + len(table2.schema.column_names)
    assert [26120, "Fichte", 10, 26120, 5001] in result.records
    assert "hoeren.MatrNr" in result.schema.column_names
    for record in result.records:
        assert record[0] == record[3]


def test_left_outer_join():
    result, _ = execute_query("studenten left join studenten.MatrNr = hoeren.MatrNr hoeren;")[0]
    table1 = table_service.retrieve("studenten", makeCopy=False)
    table2 = table_service.retrieve("hoeren", makeCopy=False)

    assert len(result) == 14
    assert len(result.schema.column_names) == len(table1.schema.column_names) + len(table2.schema.column_names)
    assert [25403, "Jonas", 12, None, None] in result.records
    assert "hoeren.MatrNr" in result.schema.column_names
    for record in result.records:
        assert record[0] == record[3] or record[3] == None


def test_natural_inner_join():
    result, _ = execute_query("studenten natural join hoeren;")[0]
    table1 = table_service.retrieve("studenten", makeCopy=False)
    table2 = table_service.retrieve("hoeren", makeCopy=False)

    assert len(result) == 10
    assert len(result.schema.column_names) == len(table1.schema.column_names) + len(table2.schema.column_names) - 1
    assert [26120, "Fichte", 10, 5001] in result.records
    assert "hoeren.MatrNr" not in result.schema.column_names


def test_natural_left_outer_join():
    result, _ = execute_query("studenten natural left join hoeren;")[0]
    table1 = table_service.retrieve("studenten", makeCopy=False)
    table2 = table_service.retrieve("hoeren", makeCopy=False)

    assert len(result) == 14
    assert len(result.schema.column_names) == len(table1.schema.column_names) + len(table2.schema.column_names) - 1
    assert [25403, "Jonas", 12, None] in result.records
    assert "hoeren.MatrNr" not in result.schema.column_names


def test_join_condition_greater_equal():
    result, _ = execute_query("studenten join studenten.MatrNr >= hoeren.MatrNr hoeren;")[0]

    assert len(result) == 32
    assert len(result.schema.column_names) == 5
    assert [27550, "Schopenhauer", 6, 26120, 5001] in result.records
    for record in result.records:
        assert record[0] >= record[3]


def test_join_condition_smaller():
    result, _ = execute_query("studenten join studenten.MatrNr < hoeren.MatrNr hoeren;")[0]

    assert len(result) == 48
    assert len(result.schema.column_names) == 5
    assert [26120, "Fichte", 10, 27550, 5001] in result.records
    for record in result.records:
        assert record[0] < record[3]

def test_join_condition_conjunctive():
    result, _ = execute_query("studenten join studenten.MatrNr < hoeren.MatrNr and studenten.MatrNr > hoeren.MatrNr "
                              "hoeren;")[0]
    assert len(result) == 0

def test_join_condition_disjunctive():
    result, _ = execute_query("studenten join studenten.MatrNr < hoeren.MatrNr or studenten.MatrNr > hoeren.MatrNr "
                              "hoeren;")[0]
    assert len(result) == 70
    for record in result.records:
        assert record[0] != record[3]

