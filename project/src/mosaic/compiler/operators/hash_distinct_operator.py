from mosaic.table_service import Table, Schema
from .abstract_operator import AbstractOperator


class HashDistinct(AbstractOperator):
    """
    Class that represents a "distinct" operation.
    It is used for duplicate elimination and here is implemented in a hash-based way.
    """

    def __init__(self, table_reference):
        super().__init__()

        self.table_reference = table_reference

    def get_result(self):
        table = self.table_reference.get_result()

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
        return self.table_reference.get_schema()

    def simplify(self):
        self.table_reference = self.table_reference.simplify()

        return self

    def __str__(self):
        return f"HashDistinct"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table_reference.explain(rows, indent + 2)
