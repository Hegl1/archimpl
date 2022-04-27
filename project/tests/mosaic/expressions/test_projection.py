from mosaic.expressions.table_scan import TableScan
from mosaic.expressions.column_expression import ColumnExpression
from mosaic.expressions.projection import Projection, InvalidAliasException
from mosaic import table_service
from mosaic.table_service import Table
from mosaic.table_service import TableIndexException
from mosaic.expressions.arithmetic_operation_expression import ArithmeticOperationExpression, ArithmeticOperator, IncompatibleOperationException
from mosaic.expressions.literal_expression import LiteralExpression
from mosaic.table_service import SchemaType
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
    projection = Projection([(None, column_expression)], table_scan)

    table = table_service.retrieve("#columns")

    result = projection.get_result()

    assert isinstance(result, Table)
    assert len(result.schema_names) == 1
    assert result.get_column_index(column_name) == 0
    assert result.schema_types[0] == table.schema_types[table.get_column_index(column_name)]

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

    projection = Projection(column_expressions, table_scan)

    table = table_service.retrieve("#columns")

    result = projection.get_result()

    assert isinstance(result, Table)
    assert len(result.schema_names) == len(column_names)

    for index, column_name in enumerate(column_names):
        assert result.get_column_index(column_name) == index
        assert result.schema_types[index] == table.schema_types[table.get_column_index(column_name)]

def test_select_non_existent_column():
    table_scan = TableScan("#columns")
    column_expression = ColumnExpression("non_existent")

    projection = Projection([(None, column_expression)], table_scan)

    with pytest.raises(TableIndexException):
        projection.get_result()

def test_select_string_concat():
    table_scan = TableScan("#columns")
    arithemetic_operation = ArithmeticOperationExpression(LiteralExpression("Position: "), ArithmeticOperator.ADD, ColumnExpression("ordinal_position"))

    projection = Projection([("FullOrdinal", arithemetic_operation)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "FullOrdinal"
    assert result.schema_types[0] == SchemaType.VARCHAR
    assert result[0, "FullOrdinal"] == "Position: 0"

def test_select_int_subtract():
    table_scan = TableScan("#columns")
    arithemetic_operation = ArithmeticOperationExpression(ColumnExpression("ordinal_position"), ArithmeticOperator.SUBTRACT, LiteralExpression(1))

    table = table_service.retrieve("#columns")

    projection = Projection([("t", arithemetic_operation)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "t"
    assert result.schema_types[0] == SchemaType.INT
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (table[index, "ordinal_position"] - 1)

def test_select_float_multiply():
    table_scan = TableScan("#columns")
    arithemetic_operation = ArithmeticOperationExpression(ColumnExpression("ordinal_position"), ArithmeticOperator.TIMES, LiteralExpression(1.5))

    table = table_service.retrieve("#columns")

    projection = Projection([("t", arithemetic_operation)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "t"
    assert result.schema_types[0] == SchemaType.FLOAT
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (table[index, "ordinal_position"] * 1.5)

def test_select_float_divide():
    table_scan = TableScan("#columns")
    arithemetic_operation = ArithmeticOperationExpression(ColumnExpression("ordinal_position"), ArithmeticOperator.DIVIDE, LiteralExpression(2))

    table = table_service.retrieve("#columns")

    projection = Projection([("t", arithemetic_operation)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "t"
    assert result.schema_types[0] == SchemaType.FLOAT
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (table[index, "ordinal_position"] / 2)

def test_select_literal_only():
    table_scan = TableScan("#columns")
    literal = LiteralExpression(0)

    table = table_service.retrieve("#columns")

    projection = Projection([("zero", literal)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "zero"
    assert result.schema_types[0] == SchemaType.INT
    assert len(result) == len(table)

    for record in result.records:
        assert record[0] == 0

def test_select_concat_columns():
    table_scan = TableScan("#columns")
    arithemetic_operation = ArithmeticOperationExpression(ColumnExpression("ordinal_position"), ArithmeticOperator.ADD, ColumnExpression("data_type"))

    table = table_service.retrieve("#columns")

    projection = Projection([("t", arithemetic_operation)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "t"
    assert result.schema_types[0] == SchemaType.VARCHAR
    assert len(result) == len(table)

    for index, record in enumerate(result.records):
        assert record[0] == (str(table[index, "ordinal_position"]) + table[index, "data_type"])

def test_select_nested_arithmetic():
    table_scan = TableScan("#columns")
    inner_arithemetic_operation = ArithmeticOperationExpression(LiteralExpression(10), ArithmeticOperator.ADD, LiteralExpression(5))
    arithemetic_operation = ArithmeticOperationExpression(LiteralExpression(10), ArithmeticOperator.ADD, inner_arithemetic_operation)

    table = table_service.retrieve("#columns")

    projection = Projection([("t", arithemetic_operation)], table_scan)

    result = projection.get_result()

    assert result.schema_names[0] == "t"
    assert result.schema_types[0] == SchemaType.INT
    assert len(result) == len(table)

    for record in result.records:
        assert record[0] == 25

def test_select_bad_string_operation():
    table_scan = TableScan("#columns")
    arithemetic_operation = ArithmeticOperationExpression(ColumnExpression("ordinal_position"), ArithmeticOperator.SUBTRACT, LiteralExpression("test"))

    projection = Projection([("t", arithemetic_operation)], table_scan)

    with pytest.raises(IncompatibleOperationException):
        projection.get_result()

def test_select_null():
    table_scan = TableScan("#tables")

    projection = Projection([("t", LiteralExpression(None))], table_scan)

    result = projection.get_result()

    for index in range(len(result)):
        assert result[index, "t"] == None

def test_select_bad_alias():
    table_scan = TableScan("#tables")

    projection = Projection([("test.test", LiteralExpression("test"))], table_scan)

    with pytest.raises(InvalidAliasException):
        projection.get_result()