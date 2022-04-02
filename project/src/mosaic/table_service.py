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


class TableParsingException(Exception):
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
    This function extracts a table from a specific file format and saves a specific table into the tables dict.
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
            if len(lines) == i + 1:
                raise TableParsingException("No schema section found")

            i += 1

        schema_end = i

        while lines[schema_end] != "\n":
            schema_end += 1

        # retrieve schema_names and schema_types in lists
        for j in range(i + 1, schema_end):
            line = lines[j].rstrip('\n')
            schema = [s.strip() for s in line.split(':')]

            if len(schema) != 2:
                raise TableParsingException(f"Too many parts in line {j+1}")
            if " " in schema[0]:
                raise TableParsingException(f"Column name can not contain spaces in line {j+1}")

            schema_names.append(table_name + "." + schema[0])

            try:
                schema_types.append(_convert_schema_type_string(schema[1]))
            except WrongSchemaTypeException:
                raise TableParsingException(f"Unknown type in line {j+1}: \"{schema[1]}\"")

        i = schema_end

        # get first line of data
        while lines[i] != "[Data]\n":
            if len(lines) == i + 1:
                raise TableParsingException("No data section found")
            i += 1

        # retrieve data and convert into tuple list
        for j in range(i + 1, len(lines)):
            column_data = lines[j].rstrip('\n').split(';')

            if len(column_data) != len(schema_types):
                raise TableParsingException(f"Wrong amount of columns in line {j+1}")

            data = dict()

            try:
                for k in range(len(column_data)):
                    if schema_types[k] == SchemaType.INT:
                        data[schema_names[k]] = int(column_data[k])
                    elif schema_types[k] == SchemaType.FLOAT:
                        data[schema_names[k]] = float(column_data[k])
                    else:
                        data[schema_names[k]] = column_data[k]
            except Exception:
                raise TableParsingException(f"Parsing error in line {j+1} near \"{column_data[k]}\"")

            data_list.append(data)

    _tables[table_name] = Table(table_name, schema_names, schema_types, data_list)


def load_tables_from_directory(path):
    """
    This function calls the load_from_file function for every file (which represent a table) in path
    Returns a list of tuples for files that could not be loaded (file_name, error_information)
    """
    not_loaded_files = []
    loaded_files = []
    for file in os.listdir(path):
        if file.endswith(".table"):
            try:
                load_from_file(os.path.join(path, file).replace("\\", "/"))
                loaded_files.append(file)
            except Exception as ex:
                not_loaded_files.append((file, str(ex)))
        else:
            not_loaded_files.append((file, "Not a .table file"))

    if len(loaded_files) == 0:
        # Throw exception
        raise NoTableLoadedException
    else:
        _create_tables_table()
        _create_columns_table()
        # do not change function call order here. this yields a result exactly as required in MS1
    return not_loaded_files


def _create_tables_table():
    table_name = "#tables"
    table_names = [{"#tables.table_name": item} for item in list(_tables.keys())] + [
        {"#tables.table_name": "#tables"}] + [{"#tables.table_name": "#columns"}]
    _tables[table_name] = Table(table_name, [table_name + ".table_name"], [_convert_schema_type_string("varchar")],
                                table_names)


def _create_columns_table():
    columns_table_name = "#columns"
    columns_schema_names = [columns_table_name + ".table_name",
                            columns_table_name + ".column_name",
                            columns_table_name + ".ordinal_position",
                            columns_table_name + ".data_type"]
    columns_schema_types = [_convert_schema_type_string("varchar"),
                            _convert_schema_type_string("varchar"),
                            _convert_schema_type_string("int"),
                            _convert_schema_type_string("varchar")]
    columns_data = list()

    # add entries for all tables currently in database
    for table_name in _tables:
        column_names = _tables[table_name].schema_names
        for i, column_name in enumerate(column_names):
            columns_data.append({columns_schema_names[0]: table_name,
                                 columns_schema_names[1]: column_name,
                                 columns_schema_names[2]: i,
                                 columns_schema_names[3]: _tables[table_name].schema_types[i].value})

    # add entries for the #columns table columns
    for i, columns_schema_name in enumerate(columns_schema_names):
        columns_data.append({columns_schema_names[0]: columns_table_name,
                             columns_schema_names[1]: columns_schema_name,
                             columns_schema_names[2]: i,
                             columns_schema_names[3]: columns_schema_types[i].value})

    _tables[columns_table_name] = Table(columns_table_name, columns_schema_names, columns_schema_types, columns_data)


def retrieve(table_name):
    """
    This function returns a table specified by the table_name
    """
    try:
        return _tables[table_name]
    except KeyError:
        raise TableNotFoundException


def table_exists(name):
    return name in _tables or name == '#tables' or name == '#columns'
