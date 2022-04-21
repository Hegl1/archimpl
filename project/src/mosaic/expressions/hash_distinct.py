from .abstract_expression import AbstractExpression

class HashDistinct(AbstractExpression):
    def __init__(self, table):
        super().__init__()

        self.table = table

    def get_result(self):
        table = self.table.get_result()

        hashes = set()

        for index in reversed(range(len(table.records))):
            record = table[index]
            record_hash = hash(str(record))

            if record_hash in hashes:
                table.records.pop(index)
            else:
                hashes.add(record_hash)

        return table

    def __str__(self):
        return f"HashDistinct"