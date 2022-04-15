from .abstract_expression import AbstractExpression

class HashDistinct(AbstractExpression):
    def __init__(self, table):
        super().__init__()

        self.table = table

    def get_result(self):
        table = self.table.get_result()

        hashes = set()
        duplicate_indices = []

        for index, record in enumerate(table.records):
            record_hash = hash(str(record))
            if record_hash in hashes:
                duplicate_indices.append(index)
            else:
                hashes.add(record_hash)

        duplicate_indices.reverse()

        for index in duplicate_indices:
            table.records.pop(index)

        return table

    def __str__(self):
        return f"HashDistinct(table_name={self.table.table_name})"