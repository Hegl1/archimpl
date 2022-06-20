from mosaic import table_service


def evaluate(comparative_operation):
    table = retrieve_table("studenten")
    sum = 0
    entries = len(table)
    for i in range(entries):
        sum += comparative_operation.get_result(table=table, row_index=i)

    return (sum, entries)


def retrieve_table(name):
    table_service.load_tables_from_directory("./tests/testdata/")
    return table_service.retrieve_table(name)
