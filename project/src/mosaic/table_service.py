import os


class Table:

    def __init__(self, table_name, schema_names, schema_types, data):
        self.table_name = table_name
        self.schema_names = schema_names
        self.schema_types = schema_types
        self.records = data


class TableNotFoundException(Exception):
    pass


_tables = dict()


def load_from_file(path):
    """
    Loads a table from a file.
    This function extracts a table from a specific file format and saves a specific tabel into the tables dict.
    """

    table_name = path.split('/')[-1].split('.')[0]
    data_list = list()
    schema_names = []
    schema_types = []

    with open(path, "r") as f:
        lines = f.readlines()
        i = 0

        # get first and last line of schema
        while lines[i] != "[Schema]\n":
            i += 1

        schema_end = i

        while lines[schema_end] != "\n":
            schema_end += 1

        # retrieve schema_names and schema_types in lists
        for j in range(i + 1, schema_end):
            schema = lines[j].rstrip('\n').replace(" ", "").split(':')
            schema_names.append(table_name + "." + schema[0])
            schema_types.append(schema[1])

        i = schema_end

        # get first line of data
        while lines[i] != "[Data]\n":
            i += 1

        # retrieve data and convert into tuple list
        for j in range(i + 1, len(lines)):
            column_data = lines[j].rstrip('\n').split(';')
            data = dict()
            for k in range(len(column_data)):
                data[schema_names[k]] = column_data[k]
            data_list.append(data)

    _tables[table_name] = Table(table_name, schema_names, schema_types, data_list)


def load_tables_from_directory(path):
    """
    This function calls the load_from_file function for every file (which represent a tables) in path
    """

    for file in os.listdir(path):
        if file.endswith(".table"):
            load_from_file(os.path.join(path, file))


def retrieve(table_name):
    """
    This function returns a table specified by the table_name
    """

    if table_name == "#tables":
        table_names = list(_tables.keys()) + ["#tables"] + ["#column"]
        return Table(table_name, ["tables.table_names"], ["varchar"], table_names)
    elif table_name == "#columns":
        return _retrieve_column_table()
    else:
        try:
            return _tables[table_name]
        except KeyError:
            raise TableNotFoundException


def _retrieve_column_table():
    """
    This private function returns the #column table if needed
    """

    column_data = list()
    for table_name in _tables:
        column_names = _tables[table_name].schema_names
        for column_name in column_names:
            ordinal_position = column_names.index(column_name)
            column_data.append({"#colums.table_name": table_name,
                                "#columns.column_name": column_name,
                                "#columns.ordinal_position": ordinal_position,
                                "#columns.data_type": _tables[table_name].schema_types[ordinal_position]
                                })

    column_data.append({"#colums.table_name": "#table",
                        "#columns.column_name": "#tables.table_name",
                        "#columns.ordinal_position": 0,
                        "#columns.data_type": "varchar"
                        })

    column_data.append({"#colums.table_name": "#columns",
                        "#columns.column_name": "#columns.table_name",
                        "#columns.ordinal_position": 0,
                        "#columns.data_type": "varchar"
                        })

    column_data.append({"#colums.table_name": "#columns",
                        "#columns.column_name": "#columns.column_name",
                        "#columns.ordinal_position": 1,
                        "#columns.data_type": "varchar"
                        })

    column_data.append({"#colums.table_name": "#columns",
                        "#columns.column_name": "#columns.ordinal_position",
                        "#columns.ordinal_position": 2,
                        "#columns.data_type": "int"
                        })

    column_data.append({"#colums.table_name": "#columns",
                        "#columns.column_name": "#columns.data_type",
                        "#columns.ordinal_position": 3,
                        "#columns.data_type": "varchar"
                        })

    return Table("#columns",
                 ["#columns.table_name", "#columns.column_name", "#columns.ordinal_position", "#columns.data_type"],
                 ["varchar", "varchar", "int", "varchar"], column_data)
