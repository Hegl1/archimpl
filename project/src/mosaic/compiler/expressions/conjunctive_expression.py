from mosaic.compiler.get_string_representation import get_string_representation
from mosaic.table_service import Schema
from .abstract_computation_expression import AbstractComputationExpression
from .literal_expression import LiteralExpression


class ConjunctiveExpression(AbstractComputationExpression):
    """
    Class that represents a conjunction.
    """

    def __init__(self, conditions):
        super().__init__()

        self.conditions = conditions

    def get_result(self, table, row_index):
        for comparative in self.conditions:
            if isinstance(comparative, AbstractComputationExpression):
                result = comparative.get_result(table, row_index)
            else:
                result = comparative.get_result()

            if not result:
                return 0

        return 1

    def simplify(self):
        self.conditions = [condition.simplify() for condition in self.conditions]

        new_conditions = []

        for condition in self.conditions:
            if isinstance(condition, LiteralExpression):
                result = condition.get_result()

                if not result:
                    return LiteralExpression(0)
            elif isinstance(condition, ConjunctiveExpression):
                new_conditions += condition.conditions
            else:
                new_conditions.append(condition)

        if len(new_conditions) == 0:
            return LiteralExpression(1)
        if len(new_conditions) == 1:
            return new_conditions[0]

        self.conditions = new_conditions

        return self

    def get_string_representation(self, schema: Schema = None):
        return "(" + " AND ".join([get_string_representation(condition, schema) for condition in self.conditions]) + ")"
