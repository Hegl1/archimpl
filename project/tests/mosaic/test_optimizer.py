from mosaic.compiler.operators.explain import Explain
import pytest
from mosaic import table_service
from mosaic.compiler.operators.index_seek import IndexSeek
from mosaic.table_service import Schema
from mosaic.compiler.operators.selection import Selection
from mosaic.compiler.operators.projection import Projection
from mosaic.compiler.operators.table_scan import TableScan
from mosaic.compiler.operators.abstract_join import JoinType
from mosaic.compiler.operators.hash_join import HashJoin
from mosaic.compiler.operators.set_operators import Union
from mosaic.compiler.operators.hash_distinct import HashDistinct
from mosaic.compiler.operators.ordering import Ordering
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, ComparativeOperator
from mosaic.compiler.expressions.arithmetic_expression import ArithmeticExpression, \
    ArithmeticOperator
from mosaic.compiler import optimizer


@pytest.fixture(autouse=True)
def refresh_loaded_tables():
    table_service.load_tables_from_directory("./tests/testdata/")


def test_optimizer_split_selections():
    selection = Selection(TableScan("professoren"), ConjunctiveExpression([
        ColumnExpression("Name"),
        ComparativeExpression(ColumnExpression(
            "Name"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    ]))

    selection = optimizer._node_access_helper(
        selection, optimizer._split_selections, Selection)

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ColumnExpression)
    assert selection.condition.get_result() == "Name"
    assert isinstance(selection.node, Selection)

    sub_selection = selection.node
    assert isinstance(sub_selection.condition, ComparativeExpression)
    assert isinstance(sub_selection.condition.left, ColumnExpression)
    assert isinstance(sub_selection.condition.right, LiteralExpression)
    assert isinstance(sub_selection.node, TableScan)


def test_optimizer_join_selections():
    selection = Selection(
        Selection(
            TableScan("professoren"),
            ComparativeExpression(ColumnExpression(
                "Name"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
        ),
        ColumnExpression("Name")
    )

    selection = optimizer._node_access_helper(
        selection, optimizer._join_selections, Selection)

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ConjunctiveExpression)
    assert len(selection.condition.conditions) == 2
    assert isinstance(selection.condition.conditions[0], ColumnExpression)
    assert isinstance(
        selection.condition.conditions[1], ComparativeExpression)


def test_optimizer_join_selections_nested_conjunctives():
    selection = Selection(
        Selection(
            TableScan("professoren"),
            ConjunctiveExpression(
                [ColumnExpression("PersNr"), ColumnExpression("Rang")])
        ),
        ConjunctiveExpression(
            [ColumnExpression("Name"), ColumnExpression("Raum")])
    )

    selection = optimizer._node_access_helper(
        selection, optimizer._join_selections, Selection)

    assert isinstance(selection, Selection)
    assert isinstance(selection.condition, ConjunctiveExpression)
    assert len(selection.condition.conditions) == 4


def test_optimizer_columns_fully_covered_in_first():
    columns = ["test", "schema1.MatrNr", "Name"]
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["tust", "schema2.MatrNr", "schema2.no"], [])

    assert optimizer._are_columns_fully_covered_in_first_schema(
        columns, schema1, schema2)


def test_optimizer_columns_not_fully_covered_in_first():
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["tust", "schema2.MatrNr", "schema2.no"], [])

    assert not optimizer._are_columns_fully_covered_in_first_schema(
        ["test", "MatrNr", "Name"], schema1, schema2)
    assert not optimizer._are_columns_fully_covered_in_first_schema(
        ["not_existent", "schema1.MatrNr", "Name"], schema1, schema2)


def test_optimizer_columns_fully_covered_in_both():
    columns = ["test", "Name", "schema1.Name"]
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["test", "schema2.MatrNr", "schema2.Name"], [])

    assert optimizer._are_columns_fully_covered_in_both_schemas(
        columns, schema1, schema2)


def test_optimizer_columns_not_fully_covered_in_both():
    schema1 = Schema("schema1", ["test", "schema1.MatrNr", "schema1.Name"], [])
    schema2 = Schema("schema2", ["test", "schema2.MatrNr", "schema2.Test"], [])

    assert not optimizer._are_columns_fully_covered_in_both_schemas(
        ["test", "Name", "MatrNr"], schema1, schema2)


def test_optimizer_get_condition_columns():
    conjunctive = ConjunctiveExpression([
        ComparativeExpression(
            ColumnExpression("MatrNr"),
            ComparativeOperator.EQUAL,
            ArithmeticExpression(
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
        ComparativeExpression(
            ColumnExpression("MatrNr"),
            ComparativeOperator.EQUAL,
            ArithmeticExpression(
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
    projection = Projection(
        TableScan("professoren"), [("nm", ColumnExpression("Name"))])
    selection = Selection(
        projection,
        ComparativeExpression(ColumnExpression(
            "nm"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Projection)
    assert isinstance(projection.node, Selection)
    assert isinstance(selection.node, TableScan)
    assert selection.condition.left.value == "Name"


def test_optimizer_selection_push_through_projection_no_alias():
    projection = Projection(
        TableScan("professoren"), [(None, ColumnExpression("Name"))])
    selection = Selection(
        projection,
        ComparativeExpression(ColumnExpression(
            "Name"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Projection)
    assert isinstance(projection.node, Selection)
    assert isinstance(selection.node, TableScan)
    assert selection.condition.left.value == "Name"


def test_optimizer_selection_do_not_push_through_projection_literal_expression():
    projection = Projection(
        TableScan("professoren"), [("nm", LiteralExpression("test"))])
    selection = Selection(
        projection,
        ComparativeExpression(ColumnExpression(
            "nm"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Selection)
    assert isinstance(projection.node, TableScan)
    assert isinstance(selection.node, Projection)


def test_optimizer_selection_do_not_push_through_projection_col_not_found():
    projection = Projection(
        TableScan("professoren"), [("nm", LiteralExpression("test"))])
    selection = Selection(
        projection,
        ComparativeExpression(ColumnExpression(
            "not_found_column"), ComparativeOperator.EQUAL, LiteralExpression("Sokrates"))
    )

    node = optimizer._selection_push_through_projection(selection, projection)

    assert isinstance(node, Selection)
    assert isinstance(projection.node, TableScan)
    assert isinstance(selection.node, Projection)


def test_optimizer_selection_push_through_natural_join():
    join = HashJoin(TableScan("hoeren"), TableScan(
        "studenten"), JoinType.INNER, None, True)
    selection = Selection(
        join,
        ComparativeExpression(ColumnExpression(
            "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(26120))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, HashJoin)
    assert isinstance(join.left_node, Selection)
    assert isinstance(join.right_node, Selection)
    assert isinstance(join.left_node.node, TableScan)
    assert isinstance(join.right_node.node, TableScan)

    assert isinstance(join.left_node.condition,
                      ComparativeExpression)
    assert join.left_node.condition.left.value == "MatrNr"
    assert isinstance(join.right_node.condition,
                      ComparativeExpression)
    assert join.right_node.condition.left.value == "studenten.MatrNr"


def test_optimizer_selection_do_not_push_through_natural_join_not_fully_covered():
    join = HashJoin(TableScan("hoeren"), TableScan(
        "studenten"), JoinType.INNER, None, True)
    selection = Selection(
        join,
        ConjunctiveExpression([
            ComparativeExpression(ColumnExpression(
                "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(26120)),
            ColumnExpression("Semester")
        ])
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, Selection)
    assert isinstance(join.left_node, TableScan)
    assert isinstance(join.right_node, TableScan)


def test_optimizer_selection_push_through_inner_join_left():
    join = HashJoin(
        TableScan("hoeren"),
        TableScan("studenten"),
        JoinType.INNER,
        ComparativeExpression(ColumnExpression(
            "hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
        False
    )
    selection = Selection(
        join,
        ComparativeExpression(ColumnExpression(
            "VorlNr"), ComparativeOperator.EQUAL, LiteralExpression(5001))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, HashJoin)
    assert isinstance(join.left_node, Selection)
    assert isinstance(join.right_node, TableScan)


def test_optimizer_selection_push_through_inner_join_right():
    join = HashJoin(
        TableScan("hoeren"),
        TableScan("studenten"),
        JoinType.INNER,
        ComparativeExpression(ColumnExpression(
            "hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
        False
    )
    selection = Selection(
        join,
        ComparativeExpression(ColumnExpression(
            "Semester"), ComparativeOperator.EQUAL, LiteralExpression(12))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, HashJoin)
    assert isinstance(join.left_node, TableScan)
    assert isinstance(join.right_node, Selection)


def test_optimizer_selection_do_not_push_through_inner_join_not_fully_covered():
    join = HashJoin(
        TableScan("hoeren"),
        TableScan("studenten"),
        JoinType.INNER,
        ComparativeExpression(ColumnExpression(
            "hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
        False
    )
    selection = Selection(
        join,
        ComparativeExpression(ColumnExpression(
            "MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(26120))
    )

    node = optimizer._selection_push_through_join_operator(selection, join)

    assert isinstance(node, Selection)
    assert isinstance(join.left_node, TableScan)
    assert isinstance(join.right_node, TableScan)


def test_optimizer_selection_push_through_set_operator():
    union = Union(
        Projection(TableScan("hoeren"), [(None, ColumnExpression("MatrNr"))]),
        Projection(TableScan("studenten"),
                   [(None, ColumnExpression("MatrNr"))])
    )
    selection = Selection(
        union,
        ComparativeExpression(ColumnExpression(
            "hoeren.MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(28106))
    )

    node = optimizer._selection_push_through_set_operator(selection, union)

    assert isinstance(node, Union)
    assert isinstance(union.left_node, Projection)
    assert isinstance(union.right_node, Projection)
    assert isinstance(union.left_node.node, Selection)
    assert isinstance(union.right_node.node, Selection)

    assert isinstance(
        union.left_node.node.condition.left, ColumnExpression)
    assert union.left_node.node.condition.left.value == "hoeren.MatrNr"
    assert isinstance(
        union.right_node.node.condition.left, ColumnExpression)
    assert union.right_node.node.condition.left.value == "studenten.MatrNr"


def test_optimizer_selection_push_down_projection_distinct_join():
    selection = Selection(
        Selection(
            Projection(
                Selection(
                    HashDistinct(
                        Projection(
                            HashJoin(
                                TableScan("hoeren"),
                                TableScan("studenten"),
                                JoinType.INNER,
                                ComparativeExpression(ColumnExpression(
                                    "hoeren.MatrNr"), ComparativeOperator.EQUAL, ColumnExpression("studenten.MatrNr")),
                                False
                            ),
                            [("nm", ColumnExpression("Name")),
                             (None, ColumnExpression("Semester"))]
                        )
                    ),
                    ComparativeExpression(ColumnExpression(
                        "nm"), ComparativeOperator.EQUAL, LiteralExpression("Jonas"))
                ),
                [(None, ColumnExpression("nm")), ("test", LiteralExpression(
                    "test_text")), (None, ColumnExpression("Semester"))]
            ),
            ComparativeExpression(ColumnExpression(
                "Semester"), ComparativeOperator.EQUAL, LiteralExpression(10))
        ),
        ComparativeExpression(ColumnExpression(
            "test"), ComparativeOperator.EQUAL, LiteralExpression("test_text"))
    )

    node = optimizer._node_access_helper(
        selection, optimizer._selection_push_down(), Selection)

    assert isinstance(node, Selection)
    assert isinstance(node.node, Projection)
    assert isinstance(node.node.node, HashDistinct)
    assert isinstance(
        node.node.node.node, Projection)
    assert isinstance(
        node.node.node.node.node, HashJoin)
    assert isinstance(
        node.node.node.node.node.left_node, TableScan)
    assert isinstance(
        node.node.node.node.node.right_node, Selection)
    assert isinstance(
        node.node.node.node.node.right_node.node, Selection)

    selection1 = node.node.node.node.node.right_node
    selection2 = node.node.node.node.node.right_node.node

    assert isinstance(selection1.condition.left, ColumnExpression)
    assert selection1.condition.left.value == "Name"
    assert isinstance(selection2.condition.left, ColumnExpression)
    assert selection2.condition.left.value == "Semester"


def test_optimizer_selection_push_down_set_operator_ordering():
    selection = Selection(
        Ordering(
            Union(
                Projection(
                    TableScan("hoeren"),
                    [(None, ColumnExpression("MatrNr"))]
                ),
                Projection(
                    TableScan("studenten"),
                    [(None, ColumnExpression("MatrNr"))]
                )
            ),
            [ColumnExpression("MatrNr")]
        ),
        ComparativeExpression(ColumnExpression(
            "MatrNr"), ComparativeOperator.GREATER, LiteralExpression(0))
    )

    node = optimizer._node_access_helper(
        selection, optimizer._selection_push_down(), Selection)

    assert isinstance(node, Ordering)
    assert isinstance(node.node, Union)
    assert isinstance(node.node.left_node, Projection)
    assert isinstance(node.node.right_node, Projection)
    assert isinstance(
        node.node.left_node.node, Selection)
    assert isinstance(
        node.node.right_node.node, Selection)

    selection1 = node.node.left_node.node
    selection2 = node.node.right_node.node

    assert isinstance(selection1.condition.left, ColumnExpression)
    assert selection1.condition.left.value == "MatrNr"
    assert isinstance(selection2.condition.left, ColumnExpression)
    assert selection2.condition.left.value == "studenten.MatrNr"


def test_optimizer_apply_index_seek_simple_selection_over_table_scan():
    node = Selection(
        TableScan("correctIndex"),
        ComparativeExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(28106))
    )

    node = optimizer._node_access_helper(
        node, optimizer._apply_index_seek, Selection)

    assert isinstance(node, IndexSeek)
    assert node.table_name == "correctIndex"
    assert node.index_column == "MatrNr"
    assert node.alias is None
    assert isinstance(node.condition, ComparativeExpression)
    assert node.condition.operator == ComparativeOperator.EQUAL
    assert isinstance(node.condition.left, ColumnExpression)
    assert isinstance(node.condition.right, LiteralExpression)


def test_optimizer_apply_index_seek_simple_selection_over_table_scan_alias():
    node = Selection(
        TableScan("correctIndex", alias="c"),
        ComparativeExpression(ColumnExpression("c.MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(28106))
    )

    node = optimizer._node_access_helper(
        node, optimizer._apply_index_seek, Selection)

    assert isinstance(node, IndexSeek)
    assert node.table_name == "correctIndex"
    assert node.index_column == "MatrNr"
    assert node.alias == "c"
    assert isinstance(node.condition, ComparativeExpression)


def test_optimizer_apply_index_seek_multiple_selections_over_table_scan_choose_root_selection():
    node = Selection(
        Selection(
            TableScan("correctIndex"),
            ComparativeExpression(LiteralExpression(28107), ComparativeOperator.GREATER, ColumnExpression("MatrNr"))
        ),
        ComparativeExpression(ColumnExpression("MatrNr"), ComparativeOperator.EQUAL, LiteralExpression(28106))
    )

    node = optimizer._node_access_helper(
        node, optimizer._apply_index_seek, Selection)

    assert isinstance(node, Selection)
    assert isinstance(node.condition, ComparativeExpression)
    assert node.condition.operator == ComparativeOperator.GREATER
    assert isinstance(node.condition.right, ColumnExpression)
    assert isinstance(node.condition.left, LiteralExpression)

    assert isinstance(node.node, IndexSeek)
    assert node.node.table_name == "correctIndex"
    assert node.node.index_column == "MatrNr"
    assert node.node.alias is None
    assert isinstance(node.node.condition, ComparativeExpression)
    assert node.node.condition.operator == ComparativeOperator.EQUAL
    assert isinstance(node.node.condition.left, ColumnExpression)
    assert isinstance(node.node.condition.right, LiteralExpression)
