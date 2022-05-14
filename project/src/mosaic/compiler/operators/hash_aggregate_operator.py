from abc import ABC
from enum import Enum
from mosaic.compiler.operators.abstract_operator import AbstractOperator
from mosaic.table_service import Schema, Table

class AggregateFunction(Enum):
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT = "COUNT"

class HashAggregate(AbstractOperator, ABC):
    
    def __init__(self,table_referece,groups,aggregations):
        super().__init__()
        self.table_reference = table_referece
        self.groups = groups
        self.aggregation = aggregations

    def get_schema(self):
        return self.table_reference.get_schema()

    def get_result(self):
        table = self.table_reference.get_result()
        result = []
        #compare group length to aggregate length to see if possible

        #calculate aggregations

        #change column name to given one and new table name
        schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)

        return Table(schema, result)


    def __str__(self):
        schema = self.get_schema()
        return f"Aggregation(...)"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)