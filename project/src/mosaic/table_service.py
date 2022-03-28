import os
import tabulate
from enum import Enum


class SchemaType(Enum):
    """
    Enum class that represents the available Datatypes in our Database.
    """

    INT = "int"
    FLOAT = "float"
    VARCHAR = "varchar"


class Table:
    """
    Class that represents one table in our Database.
    This class has the following properties:
    table_name: str - the name of the table
    schema_names: [str] - the names of the columns. Position in the list matters
    schema_types: [SchemaType] - the type of the corresponding column
    data: [{str: int|float|str}] - list that represents a row of the table.
        each list entry is a dict with column name as key and corresponding entry as value.
    """

    def __init__(self, table_name, schema_names, schema_types, data):
        self.table_name = table_name
        self.schema_names = schema_names
        self.schema_types = schema_types
        self.records = data

    def __str__(self):
        return tabulate.tabulate([d.values() for d in self.records], self.schema_names, tablefmt="psql",
                                 stralign="left")


class TableNotFoundException(Exception):
    pass


class NoTableLoadedException(Exception):
    pass


class WrongSchemaTypeException(Exception):
    pass


_tables = dict()


def _convert_schema_type_string(type_string):
    if type_string == 'int':
        return SchemaType.INT
    elif type_string == 'float':
        return SchemaType.FLOAT
    elif type_string == 'varchar':
        return SchemaType.VARCHAR
    else:
        raise WrongSchemaTypeException


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
            schema_types.append(_convert_schema_type_string(schema[1]))

        i = schema_end

        # get first line of data
        while lines[i] != "[Data]\n":
            i += 1

        # retrieve data and convert into tuple list
        for j in range(i + 1, len(lines)):
            column_data = lines[j].rstrip('\n').split(';')
            data = dict()
            for k in range(len(column_data)):

                if schema_types[k] == SchemaType.INT:
                    data[schema_names[k]] = int(column_data[k])
                elif schema_types[k] == SchemaType.FLOAT:
                    data[schema_names[k]] = float(column_data[k])
                else:
                    data[schema_names[k]] = column_data[k]
            data_list.append(data)

    _tables[table_name] = Table(table_name, schema_names, schema_types, data_list)


def load_tables_from_directory(path):
    """
    This function calls the load_from_file function for every file (which represent a tables) in path
    Returns a list of files that could not be loaded
    """
    not_loaded_files = []
    loaded_files = []
    for file in os.listdir(path):
        if file.endswith(".table"):
            try:
                load_from_file(os.path.join(path, file).replace("\\", "/"))
                loaded_files.append(file)
            except Exception:
                not_loaded_files.append(file)
        else:
            not_loaded_files.append(file)

    if len(loaded_files) == 0:
        # Throw exepction
        raise NoTableLoadedException

    return not_loaded_files



def retrieve(table_name):
    """
    This function returns a table specified by the table_name
    """

    if table_name == "#tables":
        table_names = [{"#tables.table_name": item} for item in list(_tables.keys())] + [
            {"#tables.table_name": "#tables"}] + [
                          {"#tables.table_name": "#columns"}]
        return Table(table_name, ["tables.table_name"], ["varchar"], table_names)
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
                                "#columns.data_type": _tables[table_name].schema_types[ordinal_position].value
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


def table_exists(name):
    return name in _tables or name == '#tables' or name == '#columns'
