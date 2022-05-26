from mosaic.compiler.operators.explain import Explain
import pytest
from mosaic import table_service
from mosaic.table_service import Schema
from mosaic.compiler.operators.selection_operator import Selection
from mosaic.compiler.operators.projection_operator import Projection
from mosaic.compiler.operators.table_scan_operator import TableScan
from mosaic.compiler.operators.abstract_join_operator import JoinType
from mosaic.compiler.operators.hash_join_operator import HashJoin
from mosaic.compiler.operators.set_operators import Union
from mosaic.compiler.operators.hash_distinct_operator import HashDistinct
from mosaic.compiler.operators.ordering_operator import OrderingOperator
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.comparative_operation_expression import ComparativeOperationExpression, ComparativeOperator
from mosaic.compiler.expressions.arithmetic_operation_expression import ArithmeticOperationExpression, \
    ArithmeticOperator
from mosaic.compiler import optimizer


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_optimizer_split_selections():
    selection = Selection(TableScan("professoren"), ConjunctiveExpression([
        ColumnExpression("Name"),
        ComparativeOperationExpression(ColumnExpression("Name"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    ]))

    selection = optimizer._selection_access_helper(selection, optimizer._split_selections)

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ColumnExpression)
    assert selection.condition.get_result() == "Name"
    assert isinstance(selection.table_reference, Selection)

    sub_selection = selection.table_reference
    assert isinstance(sub_selection.condition, ComparativeOperationExpression)
    assert isinstance(sub_selection.condition.left, ColumnExpression)
    assert isinstance(sub_selection.condition.right, LiteralExpression)
    assert isinstance(sub_selection.table_reference, TableScan)


def test_optimizer_join_selections():
    selection = Selection(
        Selection(
            TableScan("professoren"),
            ComparativeOperationExpression(ColumnExpression("Name"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
        ),
        ColumnExpression("Name")
    )

    selection = optimizer._selection_access_helper(selection, optimizer._join_selections)

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ConjunctiveExpression)
    assert len(selection.condition.conditions) == 2
    assert isinstance(selection.condition.conditions[0], ColumnExpression)
    assert isinstance(selection.condition.conditions[1], ComparativeOperationExpression)


def test_optimizer_join_selections_nested_conjunctives():
    selection = Selection(
        Selection(
            TableScan("professoren"),
            ConjunctiveExpression([ColumnExpression("PersNr"), ColumnExpression("Rang")])
        ),
        ConjunctiveExpression([ColumnExpression("Name"), ColumnExpression("Raum")])
    )

    selection = optimizer._selection_access_helper(selection, optimizer._join_selections)

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ConjunctiveExpression)
    assert len(selection.condition.conditions) == 4


def test_optimizer_columns_fully_covered_in_first():
    columns = ["test", "schema1.MatrNr", "Name"]
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["tust", "schema2.MatrNr", "schema2.no"], [])

    assert optimizer._are_columns_fully_covered_in_first_schema(columns, schema1, schema2)


def test_optimizer_columns_not_fully_covered_in_first():
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["tust", "schema2.MatrNr", "schema2.no"], [])

    assert not optimizer._are_columns_fully_covered_in_first_schema(["test", "MatrNr", "Name"], schema1, schema2)
    assert not optimizer._are_columns_fully_covered_in_first_schema(["not_existent", "schema1.MatrNr", "Name"], schema1, schema2)


def test_optimizer_columns_fully_covered_in_both():
    columns = ["test", "Name"]
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["test", "schema2.MatrNr", "schema2.Name"], [])

    assert optimizer._are_columns_fully_covered_in_both_schemas(columns, schema1, schema2)


def test_optimizer_columns_not_fully_covered_in_both():
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["test", "schema2.MatrNr", "schema2.Name"], [])

    assert not optimizer._are_columns_fully_covered_in_both_schemas(["test", "Name", "schema1.MatrNr"], schema1, schema2)
    assert not optimizer._are_columns_fully_covered_in_both_schemas(["test", "Name", "schema2.MatrNr"], schema1, schema2)


def test_optimizer_get_condition_columns():
    conjunctive = ConjunctiveExpression([
        ComparativeOperationExpression(
            ColumnExpression("MatrNr"),
            ComparativeOperator.EQUAL,
            ArithmeticOperationExpression(
                LiteralExpression("Mr. "),
                ArithmeticOperator.ADD,
                ColumnExpression("Name")
            )
        ),
        ColumnExpression("Raum")
    ])

    columns = optimizer._get_condition_columns(conjunctive)

    assert len(columns) == 3
    assert "MatrNr" in columns
    assert "Name" in columns
    assert "Raum" in columns


def test_optimizer_replace_condition_columns():
    conjunctive = ConjunctiveExpression([
        ComparativeOperationExpression(
            ColumnExpression("MatrNr"),
            ComparativeOperator.EQUAL,
            ArithmeticOperationExpression(
                LiteralExpression("Mr. "),
                ArithmeticOperator.ADD,
                ColumnExpression("Name")
            )
        ),
        ColumnExpression("Raum")
    ])

    column_replacement = {
        "MatrNr": "hoeren.MatrNr",
        "Name": "nm"
    }

    optimizer._replace_condition_columns(conjunctive, column_replacement)

    conjunctive.conditions[0].left.value == "hoeren.MatrNr"
    conjunctive.conditions[0].right.right == "nm"
    conjunctive.conditions[1].value == "Raum"


def test_optimizer_selection_push_through_projection_alias():
    projection = Projection([("nm", ColumnExpression("Name"))], TableScan("professoren"))
    selection = Selection(
        projection,
        ComparativeOperationExpression(ColumnExpression("nm"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Projection)
    assert isinstance(projection.table_reference, Selection)
    assert isinstance(selection.table_reference, TableScan)
    assert selection.condition.left.value == "Name"


def test_optimizer_selection_push_through_projection_no_alias():
    projection = Projection([(None, ColumnExpression("Name"))], TableScan("professoren"))
    selection = Selection(
        projection,
        ComparativeOperationExpression(ColumnExpression("Name"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Projection)
    assert isinstance(projection.table_reference, Selection)
    assert isinstance(selection.table_reference, TableScan)
    assert selection.condition.left.value == "Name"


def test_optimizer_selection_do_not_push_through_projection_literal_expression():
    projection = Projection([("nm", LiteralExpression("test"))], TableScan("professoren"))
    selection = Selection(
        projection,
        ComparativeOperationExpression(ColumnExpression("nm"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Selection)
    assert isinstance(projection.table_reference, TableScan)
    assert isinstance(selection.table_reference, Projection)


def test_optimizer_selection_do_not_push_through_projection_col_not_found():
    projection = Projection([("nm", LiteralExpression("test"))], TableScan("professoren"))
    selection = Selection(
        projection,
        ComparativeOperationExpression(ColumnExpression("not_found_column"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Selection)
    assert isinstance(projection.table_reference, TableScan)
    assert isinstance(selection.table_reference, Projection)


def test_optimizer_selection_push_through_natural_join():
    join = HashJoin(TableScan("hoeren"), TableScan("studenten"), JoinType.INNER, None, True)
    selection = Selection(
        join,
        ComparativeOperationExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(26120))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, HashJoin)
    assert isinstance(join.table1_reference, Selection)
    assert isinstance(join.table2_reference, Selection)
    assert isinstance(join.table1_reference.table_reference, TableScan)
    assert isinstance(join.table2_reference.table_reference, TableScan)

    assert isinstance(join.table1_reference.condition, ComparativeOperationExpression)
    assert join.table1_reference.condition.left.value == "MatrNr"
    assert isinstance(join.table2_reference.condition, ComparativeOperationExpression)
    assert join.table2_reference.condition.left.value == "MatrNr"


def test_optimizer_selection_do_not_push_through_natural_join_not_fully_covered():
    join = HashJoin(TableScan("hoeren"), TableScan("studenten"), JoinType.INNER, None, True)
    selection = Selection(
        join,
        ConjunctiveExpression([
            ComparativeOperationExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(26120)),
            ColumnExpression("Semester")
        ])
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, Selection)
    assert isinstance(join.table1_reference, TableScan)
    assert isinstance(join.table2_reference, TableScan)


def test_optimizer_selection_push_through_inner_join_left():
    join = HashJoin(
        TableScan("hoeren"),
        TableScan("studenten"),
        JoinType.INNER,
        ComparativeOperationExpression(ColumnExpression("hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
        False
    )
    selection = Selection(
        join,
        ComparativeOperationExpression(ColumnExpression("VorlNr"), ComparativeOperator.EQUAL, LiteralExpression(5001))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, HashJoin)
    assert isinstance(join.table1_reference, Selection)
    assert isinstance(join.table2_reference, TableScan)


def test_optimizer_selection_push_through_inner_join_right():
    join = HashJoin(
        TableScan("hoeren"),
        TableScan("studenten"),
        JoinType.INNER,
        ComparativeOperationExpression(ColumnExpression("hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
        False
    )
    selection = Selection(
        join,
        ComparativeOperationExpression(ColumnExpression("Semester"), ComparativeOperator.EQUAL, LiteralExpression(12))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, HashJoin)
    assert isinstance(join.table1_reference, TableScan)
    assert isinstance(join.table2_reference, Selection)


def test_optimizer_selection_do_not_push_through_inner_join_not_fully_covered():
    join = HashJoin(
        TableScan("hoeren"),
        TableScan("studenten"),
        JoinType.INNER,
        ComparativeOperationExpression(ColumnExpression("hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
        False
    )
    selection = Selection(
        join,
        ComparativeOperationExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(26120))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, Selection)
    assert isinstance(join.table1_reference, TableScan)
    assert isinstance(join.table2_reference, TableScan)


def test_optimizer_selection_push_through_set_operator():
    union = Union(
        Projection([(None, ColumnExpression("MatrNr"))], TableScan("hoeren")),
        Projection([(None, ColumnExpression("MatrNr"))], TableScan("studenten"))
    )
    selection = Selection(
        union,
        ComparativeOperationExpression(ColumnExpression("hoeren.MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(28106))
    )

    node = optimizer._selection_push_through_set_operator(selection, union)

    assert isinstance(node, Union)
    assert isinstance(union.table1_reference, Projection)
    assert isinstance(union.table2_reference, Projection)
    assert isinstance(union.table1_reference.table_reference, Selection)
    assert isinstance(union.table2_reference.table_reference, Selection)

    assert isinstance(union.table1_reference.table_reference.condition.left, ColumnExpression)
    assert union.table1_reference.table_reference.condition.left.value == "hoeren.MatrNr"
    assert isinstance(union.table2_reference.table_reference.condition.left, ColumnExpression)
    assert union.table2_reference.table_reference.condition.left.value == "studenten.MatrNr"


def test_optimizer_selection_push_down_projection_distinct_join():
    selection = Selection(
        Selection(
            Projection(
                [(None, ColumnExpression("nm")), ("test", LiteralExpression("test_text")), (None, ColumnExpression("Semester"))],
                Selection(
                    HashDistinct(
                        Projection(
                            [("nm", ColumnExpression("Name")), (None, ColumnExpression("Semester"))],
                            HashJoin(
                                TableScan("hoeren"),
                                TableScan("studenten"),
                                JoinType.INNER,
                                ComparativeOperationExpression(ColumnExpression("hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
                                False
                            )
                        )
                    ),
                    ComparativeOperationExpression(ColumnExpression("nm"), ComparativeOperator.EQUAL, LiteralExpression("Jonas"))
                )
            ),
            ComparativeOperationExpression(ColumnExpression("Semester"), ComparativeOperator.EQUAL, LiteralExpression(10))
        ),
        ComparativeOperationExpression(ColumnExpression("test"), ComparativeOperator.EQUAL, LiteralExpression("test_text"))
    )

    node = optimizer._selection_access_helper(selection, optimizer._selection_push_down)

    assert isinstance(node, Selection)
    assert isinstance(node.table_reference, Projection)
    assert isinstance(node.table_reference.table_reference, HashDistinct)
    assert isinstance(node.table_reference.table_reference.table_reference, Projection)
    assert isinstance(node.table_reference.table_reference.table_reference.table_reference, HashJoin)
    assert isinstance(node.table_reference.table_reference.table_reference.table_reference.table1_reference, TableScan)
    assert isinstance(node.table_reference.table_reference.table_reference.table_reference.table2_reference, Selection)
    assert isinstance(node.table_reference.table_reference.table_reference.table_reference.table2_reference.table_reference, Selection)

    selection1 = node.table_reference.table_reference.table_reference.table_reference.table2_reference
    selection2 = node.table_reference.table_reference.table_reference.table_reference.table2_reference.table_reference

    assert isinstance(selection1.condition.left, ColumnExpression)
    assert selection1.condition.left.value == "Name"
    assert isinstance(selection2.condition.left, ColumnExpression)
    assert selection2.condition.left.value == "Semester"


def test_optimizer_selection_push_down_set_operator_ordering():
    selection = Selection(
        OrderingOperator(
            [ColumnExpression("MatrNr")],
            Union(
                Projection(
                    [(None, ColumnExpression("MatrNr"))],
                    TableScan("hoeren")
                ),
                Projection(
                    [(None, ColumnExpression("MatrNr"))],
                    TableScan("studenten")
                )
            )
        ),
        ComparativeOperationExpression(ColumnExpression("MatrNr"), ComparativeOperator.GREATER, LiteralExpression(0))
    )

    node = optimizer._selection_access_helper(selection, optimizer._selection_push_down)

    assert isinstance(node, OrderingOperator)
    assert isinstance(node.table_reference, Union)
    assert isinstance(node.table_reference.table1_reference, Projection)
    assert isinstance(node.table_reference.table2_reference, Projection)
    assert isinstance(node.table_reference.table1_reference.table_reference, Selection)
    assert isinstance(node.table_reference.table2_reference.table_reference, Selection)

    selection1 = node.table_reference.table1_reference.table_reference
    selection2 = node.table_reference.table2_reference.table_reference

    assert isinstance(selection1.condition.left, ColumnExpression)
    assert selection1.condition.left.value == "MatrNr"
    assert isinstance(selection2.condition.left, ColumnExpression)
    assert selection2.condition.left.value == "studenten.MatrNr"
