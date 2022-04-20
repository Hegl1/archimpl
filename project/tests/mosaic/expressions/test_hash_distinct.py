from mosaic.expressions.table_scan import TableScan
from mosaic.expressions.column_expression import ColumnExpression
from mosaic.expressions.projection import Projection
from mosaic.expressions.hash_distinct import HashDistinct

def test_distinct_one_column():
    table_scan = TableScan("#columns")
    column_expression = ColumnExpression("table_name")
    projection = Projection([(None, column_expression)], table_scan)

    hash_distinct = HashDistinct(projection)
    result = hash_distinct.get_result()

    result_column = result[:,"table_name"]

    for index in reversed(range(len(result_column))):
        assert result_column.index(result_column[index]) == index