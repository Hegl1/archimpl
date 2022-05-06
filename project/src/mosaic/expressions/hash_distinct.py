from .abstract_expression import AbstractExpression
from mosaic.table_service import Table



class HashDistinct(AbstractExpression):
    def __init__(self, table):
        super().__init__()

        self.table = table

    def get_result(self):
        table = self.table.get_result()

        hashes = set()
        records = []

        for record in table.records:
            record_hash = hash(str(record))

            if record_hash not in hashes:
                hashes.add(record_hash)
                records.append(record)

        return Table(table_name=table.table_name, schema_names=table.schema_names, schema_types=table.schema_types, data=records)

    def get_schema(self):
        return self.table.get_schema()

    def __str__(self):
        return f"HashDistinct"

    def explain(self, rows, indent):
        super().explain(rows, indent)
        self.table.explain(rows, indent + 2)
