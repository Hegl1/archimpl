from mosaic.expressions.table_scan import TableScan
from mosaic.expressions.column_expression import ColumnExpression
from mosaic.expressions.projection import Projection
from mosaic import table_service
from mosaic.table_service import Table
from mosaic.table_service import TableIndexException
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
