import pytest

from mosaic import table_service
from mosaic.compiler.operators.nested_loops_join_operators import SelfJoinWithoutRenamingException, NestedLoopsJoin, JoinType
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
    join = NestedLoopsJoin(table1, table1, JoinType.CROSS, condition=None, is_natural=False)
    with pytest.raises(SelfJoinWithoutRenamingException):
        join.get_result()
    table2 = TableScan("assistenten", "professoren")
    join = NestedLoopsJoin(table1, table2, JoinType.CROSS, condition=None, is_natural=False)
    with pytest.raises(SelfJoinWithoutRenamingException):
        join.get_result()


def test_table_self_join_with_renaming():
    table1 = TableScan("professoren")
    table2 = TableScan("professoren", "p")
    join = NestedLoopsJoin(table1, table2, JoinType.CROSS, condition=None, is_natural=False)
    try:
        join.get_result()
    except SelfJoinWithoutRenamingException as e:
        assert False
