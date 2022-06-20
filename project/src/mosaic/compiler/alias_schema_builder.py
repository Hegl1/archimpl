from mosaic.compiler.expressions.abstract_computation_expression import AbstractComputationExpression
from mosaic.compiler.expressions.arithmetic_expression import ArithmeticExpression
from mosaic.compiler.expressions.comparative_expression import ComparativeExpression
from mosaic.compiler.expressions.conjunctive_expression import ConjunctiveExpression
from mosaic.compiler.expressions.disjunctive_expression import DisjunctiveExpression
from mosaic.compiler.expressions.literal_expression import LiteralExpression
from mosaic.table_service import get_schema_type, SchemaType


class InvalidAliasException(Exception):
    pass


def build_schema(column_references, old_schema):
    """
    Builds the schema for the projection-result, including the columns.
    columns is a list containing:
    - integers (index of the value in the reference table)
    - ArithmeticOperationExpressions (get_result used for calculation of row value)
    - LiteralExpression (get_result used for value)

    Returns the following tuple: (schema_names, schema_types, columns)
    """
    schema_names = []
    schema_types = []

    columns = []

    for (alias, column_reference) in column_references:
        if alias is not None and "." in alias:
            raise InvalidAliasException("Aliases cannot contain \".\"")

        if isinstance(column_reference, AbstractComputationExpression):
            column_value = column_reference

            schema_names.append(alias)

            if isinstance(column_reference, ArithmeticExpression):
                schema_types.append(column_reference.get_schema_type(old_schema))
            elif isinstance(column_reference, ComparativeExpression) or \
                    isinstance(column_reference, ConjunctiveExpression) or \
                    isinstance(column_reference, DisjunctiveExpression):
                schema_types.append(SchemaType.INT)
            else:
                schema_types.append(SchemaType.NULL)
        elif isinstance(column_reference, LiteralExpression):
            column_value = column_reference

            schema_names.append(alias)
            schema_types.append(get_schema_type(column_reference.get_result()))
        else:
            column = column_reference.get_result()
            column_name = old_schema.get_fully_qualified_column_name(column)
            column_value = old_schema.get_column_index(column)

            schema_names.append(column_name if alias is None else alias)
            schema_types.append(old_schema.column_types[column_value])

        columns.append(column_value)

    return schema_names, schema_types, columns
