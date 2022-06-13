from mosaic.compiler.operators.table_scan import TableScan
from mosaic.compiler.expressions.column_expression import ColumnExpression
from mosaic.compiler.operators.projection import Projection
from mosaic import table_service
from mosaic.compiler.alias_schema_builder import InvalidAliasException
from mosaic.table_service import Table
from mosaic.table_service import TableIndexException
from mosaic.compiler.expressions.arithmetic_expression import ArithmeticExpression, ArithmeticOperator, \
    IncompatibleOperationException
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.table_service import SchemaType
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression, ComparativeOperator
import pytest


@pytest.mark.parametrize(
    'column_name',
    [
        'table_name',
        'ordinal_position',
        '#columns.table_name',
        '#columns.ordinal_position'
    ],
)
def test_select_one_column(column_name):
    table_scan = TableScan("#columns")
    column_expression = ColumnExpression(column_name)
    projection = Projection(table_scan, [(None, column_expression)])

    table = table_service.retrieve_table("#columns")

    result = projection.get_result()

    assert isinstance(result, Table)
    assert len(result.schema.column_names) == 1
    assert result.get_column_index(column_name) == 0
    assert result.schema.column_types[0] == table.schema.column_types[table.get_column_index(column_name)]


@pytest.mark.parametrize(
    'column_names',
    [
        ['table_name', 'ordinal_position'],
        ['ordinal_position', 'data_type'],
        ['#columns.table_name', 'ordinal_position'],
        ['ordinal_position', '#columns.data_type'],
        ['#columns.ordinal_position', '#columns.data_type'],
        ['ordinal_position', 'data_type', 'column_name', 'table_name'],
    ],
)
def test_select_multiple_columns(column_names):
    table_scan = TableScan("#columns")

    column_expressions = []

    for column_name in column_names:
        column_expressions.append((None, ColumnExpression(column_name)))

    projection = Projection(table_scan, column_expressions)

    table = table_service.retrieve_table("#columns")

    result = projection.get_result()

    assert isinstance(result, Table)
    assert len(result.schema.column_names) == len(column_names)

    for index, column_name in enumerate(column_names):
        assert result.get_column_index(column_name) == index
        assert result.schema.column_types[index] == table.schema.column_types[table.get_column_index(column_name)]


def test_select_non_existent_column():
    table_scan = TableScan("#columns")
    column_expression = ColumnExpression("non_existent")

    projection = Projection(table_scan, [(None, column_expression)])

    with pytest.raises(TableIndexException):
        projection.get_result()


def test_select_string_concat():
    table_scan = TableScan("#columns")
    arithmetic_operation = ArithmeticExpression(LiteralExpression("Position: "), ArithmeticOperator.ADD,
                                                         ColumnExpression("ordinal_position"))

    projection = Projection(table_scan, [("FullOrdinal", arithmetic_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "FullOrdinal"
    assert result.schema.column_types[0] == SchemaType.VARCHAR
    assert result[0, "FullOrdinal"] == "Position: 0"


def test_select_int_subtract():
    table_scan = TableScan("#columns")
    arithmetic_operation = ArithmeticExpression(ColumnExpression("ordinal_position"),
                                                         ArithmeticOperator.SUBTRACT, LiteralExpression(1))

    table = table_service.retrieve_table("#columns")

    projection = Projection(table_scan, [("t", arithmetic_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "t"
    assert result.schema.column_types[0] == SchemaType.INT
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (table[index, "ordinal_position"] - 1)


def test_select_float_multiply():
    table_scan = TableScan("#columns")
    arithmetic_operation = ArithmeticExpression(ColumnExpression("ordinal_position"),
                                                         ArithmeticOperator.TIMES, LiteralExpression(1.5))

    table = table_service.retrieve_table("#columns")

    projection = Projection(table_scan, [("t", arithmetic_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "t"
    assert result.schema.column_types[0] == SchemaType.FLOAT
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (table[index, "ordinal_position"] * 1.5)


def test_select_float_divide():
    table_scan = TableScan("#columns")
    arithmetic_operation = ArithmeticExpression(ColumnExpression("ordinal_position"),
                                                         ArithmeticOperator.DIVIDE, LiteralExpression(2))

    table = table_service.retrieve_table("#columns")

    projection = Projection(table_scan, [("t", arithmetic_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "t"
    assert result.schema.column_types[0] == SchemaType.FLOAT
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (table[index, "ordinal_position"] / 2)


def test_select_literal_only():
    table_scan = TableScan("#columns")
    literal = LiteralExpression(0)

    table = table_service.retrieve_table("#columns")

    projection = Projection(table_scan, [("zero", literal)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "zero"
    assert result.schema.column_types[0] == SchemaType.INT
    assert len(result) == len(table)

    for record in result.records:
        assert record[0] == 0


def test_select_concat_columns():
    table_scan = TableScan("#columns")
    arithmetic_operation = ArithmeticExpression(ColumnExpression("ordinal_position"), ArithmeticOperator.ADD,
                                                         ColumnExpression("data_type"))

    table = table_service.retrieve_table("#columns")

    projection = Projection(table_scan, [("t", arithmetic_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "t"
    assert result.schema.column_types[0] == SchemaType.VARCHAR
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (str(table[index, "ordinal_position"]) + table[index, "data_type"])


def test_select_nested_arithmetic():
    table_scan = TableScan("#columns")
    inner_arithmetic_operation = ArithmeticExpression(LiteralExpression(10), ArithmeticOperator.ADD,
                                                               LiteralExpression(5))
    arithmetic_operation = ArithmeticExpression(LiteralExpression(10), ArithmeticOperator.ADD,
                                                         inner_arithmetic_operation)

    table = table_service.retrieve_table("#columns")

    projection = Projection(table_scan, [("t", arithmetic_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "t"
    assert result.schema.column_types[0] == SchemaType.INT
    assert len(result) == len(table)

    for record in result.records:
        assert record[0] == 25


def test_select_bad_string_operation():
    table_scan = TableScan("#columns")
    arithmetic_operation = ArithmeticExpression(ColumnExpression("ordinal_position"),
                                                         ArithmeticOperator.SUBTRACT, LiteralExpression("test"))

    projection = Projection(table_scan, [("t", arithmetic_operation)])

    with pytest.raises(IncompatibleOperationException):
        projection.get_result()


def test_select_null():
    table_scan = TableScan("#tables")

    projection = Projection(table_scan, [("t", LiteralExpression(None))])

    result = projection.get_result()

    for index in range(len(result)):
        assert result[index, "t"] == None


def test_select_bad_alias():
    table_scan = TableScan("#tables")

    projection = Projection(table_scan, [("test.test", LiteralExpression("test"))])

    with pytest.raises(InvalidAliasException):
        projection.get_result()


def test_select_comparative():
    table_scan = TableScan("#columns")
    comparative_operation = ComparativeExpression(LiteralExpression(2), ComparativeOperator.EQUAL, LiteralExpression(2))

    projection = Projection(table_scan, [("test", comparative_operation)])

    result = projection.get_result()

    assert result.schema.column_names[0] == "test"
    assert result.schema.column_types[0] == SchemaType.INT
    assert result[0, "test"] == 1