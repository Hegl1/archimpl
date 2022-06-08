from .abstract_compile_node import AbstractCompileNode
from .expressions.abstract_expression import AbstractExpression
from ..table_service import Schema


def get_string_representation(node: AbstractCompileNode, schema: Schema):
    """
    Generates the string-representation of this node.
    If it is not none and an expression the get_string_representation method is called
    with the given schema
    """
    if node is None or not isinstance(node, AbstractExpression):
        return str(node)
    else:
        return node.get_string_representation(schema)