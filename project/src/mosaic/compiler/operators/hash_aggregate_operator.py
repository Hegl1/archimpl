from abc import ABC
from copy import deepcopy
from enum import Enum
from unittest import result
from mosaic.compiler.operators.abstract_operator import AbstractOperator
from mosaic.table_service import Schema, SchemaType, Table


class VarcharAggregateException(Exception):
    pass


class AggregateFunction(Enum):
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    COUNT = "COUNT"


def aggregate_schema_type(aggregation_function, current_schema):
    if current_schema == SchemaType.VARCHAR and aggregation_function != AggregateFunction.COUNT:
        raise VarcharAggregateException(
            "Varchar can only be aggregated with count")

    if aggregation_function == AggregateFunction.AVG:
        SchemaType.FLOAT
    elif aggregation_function == AggregateFunction.COUNT:
        SchemaType.INT
    elif aggregation_function == AggregateFunction.MAX:
        return current_schema
    elif aggregation_function == AggregateFunction.MIN:
        return current_schema
    elif aggregation_function == AggregateFunction.SUM:
        return current_schema


def aggregate(aggregation_function, to_aggregate):
    if aggregation_function == AggregateFunction.AVG:
        result = sum(to_aggregate) / len(to_aggregate)
    elif aggregation_function == AggregateFunction.COUNT:
        result = len(to_aggregate)
    elif aggregation_function == AggregateFunction.MAX:
        result = max(to_aggregate)
    elif aggregation_function == AggregateFunction.MIN:
        result = min(to_aggregate)
    elif aggregation_function == AggregateFunction.SUM:
        result = sum(to_aggregate)

    return result


def extract(aggregations):
    clean_aggregations = []
    aggregation = aggregations
    while(isinstance(aggregation, list)):
        if (isinstance(aggregation[0], list)):
            aggregation = aggregation[0]
            continue
        clean_aggregations.append(aggregation[0])
        if len(aggregation) == 3:
            aggregation = aggregation[2]
        else:
            aggregation = None

    return clean_aggregations


class HashAggregate(AbstractOperator, ABC):

    def __init__(self, table_referece, group_names, aggregations):
        super().__init__()
        self.table_reference = table_referece
        self.group_names = group_names
        self.aggregations = extract(aggregations)

    def get_schema(self):
        return self.build_schema()

    def group_columns(self):
        table = self.table_reference.get_result()

        group_table = {}

        # create new table wit first in group:
        column_indexes = [table.get_column_index(
            group_name[1].value) for group_name in self.group_names]
        for row in table.records:
            key = tuple(row[key_index] for key_index in column_indexes)
            if key not in group_table:
                group_table[key] = [row]
            else:
                group_table[key] += [row]

        return group_table

    def calculate_aggregations(self, groups=None):
        table = self.table_reference.get_result()
        rows = []
        if groups:
            for grouped_keys, group in groups.items():
                row = list(grouped_keys)
                for aggregation in self.aggregations:
                    aggregation_function = aggregation[1]
                    aggregated_column_index = table.get_column_index(
                        aggregation[2].value)

                    to_aggregate = [group_row[aggregated_column_index]
                                    for group_row in group]

                    result = aggregate(aggregation_function, to_aggregate)

                    row.append(result)

                rows.append(row)

        else:
            rows.append([])
            for aggregation in self.aggregations:
                aggregation_function = aggregation[1]

                to_aggregate = table[:, aggregation[2].value]

                result = aggregate(aggregation_function, to_aggregate)

                rows[0].append(result)

        return rows

    def build_schema(self):
        old_table = self.table_reference.get_result()
        old_schema = self.table_reference.get_schema()

        column_names = []
        column_types = []
        # get selected columns
        if self.group_names:
            column_names += [old_schema.get_fully_qualified_column_name(
                group_name[1].value) for group_name in self.group_names]
            column_types += [old_table.get_column_index(name)
                             for name in column_names]

        column_names += [aggregation[0]
                         for aggregation in self.aggregations]
        column_types += [aggregate_schema_type(aggregation[1], old_schema.column_types[old_table.get_column_index(aggregation[2].value)])
                         for aggregation in self.aggregations]

        return Schema(old_schema.table_name, column_names, column_types)

    def get_result(self):

        schema = self.build_schema()

        # calculate schema
        if self.group_names:
            # group
            groups = self.group_columns()

            # calculate aggregations
            records = self.calculate_aggregations(groups)
        else:
            records = self.calculate_aggregations()

        return Table(schema, records)

    def __str__(self):
        schema = self.table_reference.get_schema()
        groups = []
        aggregates = []
        for aggregate in self.aggregations:
            aggregates.append(
                f"{aggregate[1].value}({schema.get_fully_qualified_column_name(aggregate[2].value)}) -> {aggregate[0]}")
        for group in self.group_names:
            groups.append(
                schema.get_fully_qualified_column_name(group[1].value))

        return f"Aggregation(groups=[{', '.join(groups)}],aggregates=[{', '.join(aggregates)}])"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
