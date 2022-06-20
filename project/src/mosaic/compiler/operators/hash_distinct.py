from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class HashDistinct(AbstractOperator):
    """
    Class that represents a "distinct" operation.
    It is used for duplicate elimination and here is implemented in a hash-based way.
    """

    def __init__(self, node):
        super().__init__()

        self.node = node

    def get_result(self):
        table = self.node.get_result()

        hashes = set()
        records = []

        for record in table.records:
            record_hash = hash(str(record))

            if record_hash not in hashes:
                hashes.add(record_hash)
                records.append(record)

        schema = Schema(table.table_name, table.schema.column_names, table.schema.column_types)
        return Table(schema, records)

    def get_schema(self):
        return self.node.get_schema()

    def simplify(self):
        self.node = self.node.simplify()

        return self

    def __str__(self):
        return f"HashDistinct"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.node.explain(rows, indent + 2)
