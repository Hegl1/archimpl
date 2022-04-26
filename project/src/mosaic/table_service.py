from copy import copy
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
    NULL = "null"


class Table:
    """
    Class that represents one table in our Database.
    This class has the following properties:
    table_name: str - the name of the table
    schema_names: [str] - the FQN of the columns. Position in the list matters
    schema_types: [SchemaType] - the type of the corresponding column
    data: [[float | int | str]]] - list that represents tables data.
        Each entry of the list represents a row in the table.
    """

    def __init__(self, table_name, schema_names, schema_types, data):
        self.table_name = table_name
        self.schema_names = schema_names
        self.schema_types = schema_types
        self.records = data

    def get_simple_column_name(self, column_name):
        """
        Transforms the FQN column name into a simple column name
        """
        if column_name.startswith(self.table_name + "."):
            column_name = column_name[len(self.table_name) + 1:] # remove FQN

        return column_name

    def get_fully_qualified_column_name(self, column_name):
        """
        Transforms a column name to a FQN column name
        """
        if column_name in self.schema_names:
            return column_name

        if "." in column_name:
            return column_name

        return f"{self.table_name}.{column_name}"

    def get_column_index(self, column_name):
        """
        Returns the index of the column in the schema. It handles also FQNs and simple references.
        If the column is not found, a TableIndexException is raised
        """
        fqn_column_name = self.get_fully_qualified_column_name(column_name)

        try:
            return self.schema_names.index(fqn_column_name)
        except ValueError:
            raise TableIndexException(
                f'No column with name "{self.get_simple_column_name(column_name)}" in table "{self.table_name}"')

    def rename(self, new_name):
        """
        Renames the table
        """
        self.schema_names = [name.replace(f"{self.table_name}.", f"{new_name}.") for name in self.schema_names]
        self.table_name = new_name

    def __str__(self):
        records = [[column if column is not None else "NULL" for column in row] for row in self.records]
        return tabulate.tabulate(records, self.schema_names, tablefmt="psql", stralign="left")

    def __getitem__(self, item):
        try:
            if isinstance(item, int) or isinstance(item, slice):
                return self.records[item]

            row_index, column_name = item
            column_index = self.get_column_index(column_name)
            if not isinstance(row_index, slice):
                return self.records[row_index][column_index]
            else:
                return [row[column_index] for row in self.records[row_index]]
        except IndexError:
            raise TableIndexException(f'No row with given index in table "{self.table_name}"')


class TableIndexException(Exception):
    pass


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


def get_schema_type(obj):
    """
    Returns the schema type for the given object,
    or raises a WrongSchemaTypeException if the type does not match
    """
    if obj is None:
        return SchemaType.NULL
    elif isinstance(obj, int):
        return SchemaType.INT
    elif isinstance(obj, float):
        return SchemaType.FLOAT
    elif isinstance(obj, str):
        return SchemaType.VARCHAR

    raise WrongSchemaTypeException


def _read_schema_section(table_name, schema_start, schema_lines):
    schema_names = []
    schema_types = []
    for i, line in enumerate(schema_lines):
        line = line.rstrip('\n')
        schema = [s.strip() for s in line.split(':')]

        if len(schema) != 2:
            raise TableParsingException(f"Too many parts in line {i + 2 + schema_start}")
        if " " in schema[0]:
            raise TableParsingException(f"Column name can not contain spaces in line {i + 2 + schema_start}")

        schema_names.append(f"{table_name}.{schema[0]}")

        try:
            schema_types.append(_convert_schema_type_string(schema[1]))
        except WrongSchemaTypeException:
            raise TableParsingException(f"Unknown type in line {i + 2 + schema_start}: \"{schema[1]}\"")

    return schema_names, schema_types


def _read_data_section(schema_types, data_start, data_lines):
    data_list = []
    for i, line in enumerate(data_lines):
        fields = line.rstrip('\n').split(';')
        if len(fields) != len(schema_types):
            raise TableParsingException(f"Wrong number of columns in line {i + 2 + data_start}")

        data = list(range(len(fields)))
        try:
            for k, field in enumerate(fields):
                if schema_types[k] == SchemaType.INT:
                    data[k] = int(field)
                elif schema_types[k] == SchemaType.FLOAT:
                    data[k] = float(field)
                else:
                    data[k] = field
        except Exception:
            raise TableParsingException(f"Parsing error in line {i + 2 + data_start} near \"{field}\"")

        data_list.append(data)

    return data_list


def load_from_file(path):
    """
    Loads a table from a file.
    This function extracts a table from a specific file format and saves a specific table into the tables dict.
    """
    table_name = path.split('/')[-1].split('.')[0]

    with open(path, "r") as f:
        lines = f.readlines()

        # find schema and data sections
        try:
            schema_start = lines.index('[Schema]\n')
        except ValueError:
            raise TableParsingException("No schema section found")
        schema_end = schema_start
        while lines[schema_end] != "\n":
            schema_end += 1

        try:
            data_start = lines.index('[Data]\n')
        except ValueError:
            raise TableParsingException("No data section found")

        # read content
        schema_names, schema_types = _read_schema_section(table_name, schema_start, lines[schema_start + 1:schema_end])
        data_list = _read_data_section(schema_types, data_start, lines[data_start + 1:])

    _tables[table_name] = Table(table_name, schema_names, schema_types, data_list)


def load_tables_from_directory(path):
    """
    This function calls the load_from_file function for every file (which represent a table) in path
    Returns a list of tuples for files that could not be loaded (file_name, error_information)
    """
    global _tables
    _tables = dict()

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
    table_names = [[item] for item in list(_tables.keys())] + [["#tables"]] + [["#columns"]]
    _tables[table_name] = Table(table_name, [f"{table_name}.table_name"], [_convert_schema_type_string("varchar")],
                                table_names)


def _create_columns_table():
    columns_table_name = "#columns"
    columns_schema_names = [f"{columns_table_name}.table_name",
                            f"{columns_table_name}.column_name",
                            f"{columns_table_name}.ordinal_position",
                            f"{columns_table_name}.data_type"]
    columns_schema_types = [_convert_schema_type_string("varchar"),
                            _convert_schema_type_string("varchar"),
                            _convert_schema_type_string("int"),
                            _convert_schema_type_string("varchar")]
    columns_data = list()

    # add entries for all tables currently in database
    for table_name in _tables:
        column_names = _tables[table_name].schema_names
        for i, column_name in enumerate(column_names):
            columns_data.append([table_name, column_name, i, _tables[table_name].schema_types[i].value])

    # add entries for the #columns table columns
    for i, columns_schema_name in enumerate(columns_schema_names):
        columns_data.append([columns_table_name, columns_schema_name,
                             i, columns_schema_types[i].value])

    _tables[columns_table_name] = Table(columns_table_name, columns_schema_names, columns_schema_types, columns_data)


def retrieve(table_name, makeCopy=False):
    """
    This function returns a table specified by the table_name
    """
    try:
        if makeCopy:
            return copy(_tables[table_name])

        return _tables[table_name]
    except KeyError:
        raise TableNotFoundException(table_name)


def table_exists(name):
    return name in _tables


def initialize():
    """
    Clears the stored tables and creates #tables and #columns table at start
    """
    global _tables
    _tables = dict()
    _create_tables_table()
    _create_columns_table()
