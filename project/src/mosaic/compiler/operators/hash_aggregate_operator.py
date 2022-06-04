from abc import ABC
from copy import deepcopy
from enum import Enum
from mosaic.compiler.expressions.abstract_computation_expression import AbstractComputationExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.compiler.operators.abstract_operator import AbstractOperator
from mosaic.compiler.alias_schema_builder import build_schema
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
    """
    Receives aggregation function and schema type of current column.
    Depending on the aggregation function it returns a different schema type.
    Return schema of column.
    """
    if current_schema == SchemaType.VARCHAR and (aggregation_function == AggregateFunction.AVG or aggregation_function == AggregateFunction.SUM):
        raise VarcharAggregateException(
            "Varchar can only be aggregated with count, min and max")

    if aggregation_function == AggregateFunction.AVG:
        return SchemaType.FLOAT
    elif aggregation_function == AggregateFunction.COUNT:
        return SchemaType.INT
    elif aggregation_function == AggregateFunction.MAX:
        return current_schema
    elif aggregation_function == AggregateFunction.MIN:
        return current_schema
    elif aggregation_function == AggregateFunction.SUM:
        return current_schema


def aggregate(aggregation_function, to_aggregate):
    """
    This function receives a list and an aggregation function.
    Depending on the aggregation function it applies different
    operations to the list.
    Returns aggregated list.
    """
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
    """
    Aggregation functions are not returned as list,
    but as nested lists from the compiler. example: [aggregation, [],[aggregation2, [], aggregation3]].
    This function extracts aggregations and returns them as list: [aggregation2,aggrgation2,aggregation3]
    """
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

    def __init__(self, table_reference, group_names, aggregations):
        super().__init__()
        self.table_reference = table_reference
        self.group_names = group_names
        self.aggregations = extract(aggregations)

    def get_schema(self):
        return self._build_schema()

    def _get_key(self, grouping_columns, row, table, index):
        key_list = []
        for group_column in grouping_columns:
            if isinstance(group_column, AbstractComputationExpression):
                key_list.append(group_column.get_result(table, index))
            elif isinstance(group_column, LiteralExpression):
                key_list.append(group_column.get_result())
            else:
                key_list.append(row[group_column])

        return tuple(key_list)

    def _group_columns(self):
        """
        First the function fetches all the indexes of the grouping columns of the aggregation.
        If there are no grouping columns it returns a dictionary with an empty key and the records of the table as value.
        If there are grouping columns it loops through all the columns and adds matching rows to the group column tuple keys.
        Returns Dictionary with group column tuples as key and matching rows as value.
        """
        table = self.table_reference.get_result()

        group_table = {}

        # if no group names return hashmap
        if not self.group_names:
            return {"": table.records}

        grouping_columns = []

        for (_, group_name) in self.group_names:
            if isinstance(group_name, AbstractComputationExpression) or isinstance(group_name, LiteralExpression):
                grouping_columns.append(group_name)
            else:
                grouping_columns.append(
                    table.get_column_index(group_name.value))

        # generate dictionary
        for (index, row) in enumerate(table.records):
            # get indexes of all group columns
            key = self._get_key(grouping_columns, row, table, index)

            if key not in group_table:
                group_table[key] = [row]
            else:
                group_table[key] += [row]

        return group_table

    def _calculate_aggregations(self, groups):
        """
        Receives a dictionary where the keys are a tuple of the group columns and the values the matching rows.
        For each group first the grouped tuple gets converted to a list to add the group column values.
        If the group values are empty grouped_keys is empty and a empty list gets created.
        Afterwards all aggregations get applied to the current group by calling the aggregate function.
        The results for each aggregation get added and at the end the list with group columns and then
        aggregation results gets appended to the table records.
        Returns computed records.
        """
        table = self.table_reference.get_result()
        records = []
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

            records.append(row)

        return records

    def _build_schema(self):
        """
        Builds the schema by appending the current schema of the group columns and
        then it uses the aggregate_schema_type function to get the schema type
        for the aggregated columns.
        Returns the built schema.
        """
        old_table = self.table_reference.get_result()
        old_schema = self.table_reference.get_schema()

        column_names, column_types, _ = build_schema(
            self.group_names, old_schema)

        column_names += [aggregation[0]
                         for aggregation in self.aggregations]
        column_types += [aggregate_schema_type(aggregation[1], old_schema.column_types[old_table.get_column_index(aggregation[2].value)])
                         for aggregation in self.aggregations]

        return Schema(old_schema.table_name, column_names, column_types)

    def get_result(self):
        """
        Calculates the records and the schema for the aggregations using private class functions.
        Returns table with calculated records and schema.
        """

        schema = self._build_schema()
        groups = self._group_columns()
        records = self._calculate_aggregations(groups)

        return Table(schema, records)

    def __str__(self):
        table_schema = self.table_reference.get_schema()
        aggregate_schema = self.get_schema()
        groups = []
        aggregates = []
        for aggregate in self.aggregations:
            aggregates.append(
                f"{aggregate[1].value}({table_schema.get_fully_qualified_column_name(aggregate[2].value)}) -> {aggregate[0]}")
        for i, (_, column_ref) in enumerate(self.group_names):
            groups.append(
                (f"{aggregate_schema.column_names[i]}={str(column_ref)}"))

        return f"Aggregation(groups=[{', '.join(groups)}],aggregates=[{', '.join(aggregates)}])"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
