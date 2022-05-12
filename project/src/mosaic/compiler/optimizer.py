from .operators.abstract_operator import AbstractOperator


def optimize(execution_plan: AbstractOperator):
    execution_plan = execution_plan.simplify()
    
    return execution_plan
