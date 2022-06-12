import os
from copy import deepcopy
from enum import Enum

import tabulate

from mosaic.compiler.compiler_exception import CompilerException


class SchemaType(Enum):
    """
    Enum class that represents the available Datatypes in our Database.
    """

    INT = "int"
    FLOAT = "float"
    VARCHAR = "varchar"
    NULL = "null"


class Schema:
    """
        Class that represents the schema of a table in our Database.
        This class has the following properties:
        table_name: str - the name of the table
        column_names: [str] - the FQN of the columns. Position in the list matters
        column_types: [SchemaType] - the type of the corresponding column
        """

    def __init__(self, table_name, column_names, column_types):
        self.table_name = table_name
        self.column_names = column_names
        self.column_types = column_types

    def get_simple_column_name_list(self):
        return [self.get_simple_column_name(name) for name in self.column_names]

    def get_simple_column_name(self, column_name):
        """
        Transforms the FQN column name into a simple column name
        """
        if "." in column_name:
            column_name = column_name.split(".")[1]  # remove FQN

        return column_name

    def get_fully_qualified_column_name(self, column_name):
        """
        Transforms a column name to a FQN column name
        """

        found_columns = list(filter(lambda column: column.endswith(f".{column_name}") or
                                    column == column_name, self.column_names))

        if len(found_columns) > 1:
            raise AmbiguousColumnException(
                f"Column \"{column_name}\" is ambiguous in table \"{self.table_name}\"")
        elif len(found_columns) == 1:
            return found_columns[0]

        if len(found_columns) == 0 and "." in column_name:
            return column_name

        return f"{self.table_name}.{column_name}"

    def get_column_index(self, column_name):
        """
        Returns the index of the column in the schema. It handles also FQNs and simple references.
        If the column is not found, a TableIndexException is raised
        """
        fqn_column_name = self.get_fully_qualified_column_name(column_name)

        try:
            return self.column_names.index(fqn_column_name)
        except ValueError:
            raise TableIndexException(
                f'No column with name "{self.get_simple_column_name(column_name)}" in table "{self.table_name}"')

    def rename(self, new_name):
        """
        Renames the table
        """
        self.column_names = [name.replace(
            f"{self.table_name}.", f"{new_name}.") for name in self.column_names]
        self.table_name = new_name


class Table:
    """
    Class that represents one table in our Database.
    This class has the following properties:
    schema: Schema - the schema of the table, including the table name, columns names and column types
    records: [[float | int | str]]] - list that represents tables data.
        Each entry of the list represents a row in the table.
    """

    def __init__(self, schema, records):
        self.schema = schema
        self.records = records

    @property
    def table_name(self):
        return self.schema.table_name

    def get_column_index(self, column_name):
        return self.schema.get_column_index(column_name)

    def rename(self, new_name):
        self.schema.rename(new_name)

    def __str__(self):
        records = [[column if column is not None else "NULL" for column in row]
                   for row in self.records]
        return tabulate.tabulate(records, self.schema.column_names, tablefmt="psql", stralign="left")

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
            raise TableIndexException(
                f'No row with given index in table "{self.table_name}"')

    def __len__(self):
        return len(self.records)


class IndexNotFoundException(CompilerException):
    pass


class TableIndexException(Exception):
    pass


class TableNotFoundException(CompilerException):
    pass


class NoTableLoadedException(Exception):
    pass


class WrongSchemaTypeException(Exception):
    pass


class TableParsingException(Exception):
    pass


class AmbiguousColumnException(Exception):
    pass


_tables = dict()
_indices = dict()


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
    column_names = []
    column_types = []
    for i, line in enumerate(schema_lines):
        line = line.rstrip('\n')
        schema = [s.strip() for s in line.split(':')]

        if len(schema) != 2:
            raise TableParsingException(
                f"Too many parts in line {i + 2 + schema_start}")
        if len(schema[0]) < 1:
            raise TableParsingException(
                f"Column name must be at least one character in line {i + 2 + schema_start}")
        if " " in schema[0]:
            raise TableParsingException(
                f"Column name can not contain spaces in line {i + 2 + schema_start}")
        if schema[0].lower() == "null":
            raise TableParsingException(
                f"Colmn name can not be named null (reserved keyword) in line {i + 2 + schema_start}")
        if not schema[0][0].isalpha():
            raise TableParsingException(
                f"Colum name can not start with a non letter in line {i + 2 + schema_start}")
        if any([not char.isalnum() for char in schema[0]]):
            raise TableParsingException(
                f"Colum name can only contain alphanumeric characters in line {i + 2 + schema_start}")

        column_name = f"{table_name}.{schema[0]}"

        if column_name in column_names:
            raise TableParsingException(
                f"Column names can not be duplicated in line {i + 2 + schema_start}")

        column_names.append(column_name)

        try:
            column_types.append(_convert_schema_type_string(schema[1]))
        except WrongSchemaTypeException:
            raise TableParsingException(
                f"Unknown type in line {i + 2 + schema_start}: \"{schema[1]}\"")

    return column_names, column_types


def _create_indices(table_name, schema: Schema, index_start, index_lines):
    _indices[table_name] = dict()
    for i, line in enumerate(index_lines):
        line = line.rstrip('\n')

        try:
            schema.get_column_index(line)
        except TableIndexException:
            raise TableParsingException(
                f'Column "{line}" in line {i + 2+ index_start} does not exist in schema')

        _indices[table_name][line] = dict()


def _read_data_section(column_types, schema, data_start, data_lines):
    data_list = []
    for i, line in enumerate(data_lines):
        if line == "\n":
            break

        fields = line.rstrip('\n').split(';')
        if len(fields) != len(column_types):
            raise TableParsingException(
                f"Wrong number of columns in line {i + 2 + data_start}")

        data = list(range(len(fields)))
        try:
            for k, field in enumerate(fields):
                if column_types[k] == SchemaType.INT:
                    data[k] = int(field)
                elif column_types[k] == SchemaType.FLOAT:
                    data[k] = float(field)
                else:
                    data[k] = field
        except Exception:
            raise TableParsingException(
                f"Parsing error in line {i + 2 + data_start} near \"{field}\"")

        data_list.append(data)

        if schema.table_name in _indices:
            for index_column in _indices[schema.table_name]:
                index_column_index = schema.get_column_index(index_column)
                key = data[index_column_index]
                if key not in _indices[schema.table_name][index_column]:
                    _indices[schema.table_name][index_column][key] = []
                _indices[schema.table_name][index_column][key].append(data)

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

        try:
            data_start = lines.index('[Data]\n')
        except ValueError:
            try:
                data_start = lines.index('[Data]')
            except ValueError:
                raise TableParsingException("No data section found")

        index_start = None
        try:
            index_start = lines.index('[Indices]\n')
        except ValueError:
            pass

        schema_end = schema_start
        while schema_end < len(lines) and lines[schema_end] != "\n":

            if index_start is not None and schema_end == index_start:
                raise TableParsingException(
                    "Newline between schema and indices section required")

            if schema_end == data_start:
                raise TableParsingException(
                    "Newline between schema and data section required")
            schema_end += 1

        if index_start is not None:
            index_end = index_start
            while index_end < len(lines) and lines[index_end] != "\n":
                if index_end == data_start:
                    raise TableParsingException(
                        "Newline between indices and data section required")
                index_end += 1

        # read content
        column_names, column_types = _read_schema_section(
            table_name, schema_start, lines[schema_start + 1:schema_end])
        schema = Schema(table_name, column_names, column_types)
        if index_start is not None and index_start != index_end - 1:
            _create_indices(table_name, schema, index_start,
                            lines[index_start + 1:index_end])

        data_list = _read_data_section(
            column_types, schema, data_start, lines[data_start + 1:])

    _tables[table_name] = Table(schema, data_list)


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
        _create_indices_table()
        _create_tables_table()
        _create_columns_table()
        # do not change function call order here. this yields a result exactly as required in MS1
    return not_loaded_files


def _create_tables_table():
    table_name = "#tables"
    table_names = [[item] for item in list(
        _tables.keys())] + [["#tables"]] + [["#columns"]]
    schema = Schema(table_name, [f"{table_name}.table_name"], [
                    _convert_schema_type_string("varchar")])
    _tables[table_name] = Table(schema, table_names)


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
        column_names = _tables[table_name].schema.column_names
        for i, column_name in enumerate(column_names):
            columns_data.append(
                [table_name, column_name, i, _tables[table_name].schema.column_types[i].value])

    # add entries for the #columns table columns
    for i, columns_schema_name in enumerate(columns_schema_names):
        columns_data.append([columns_table_name, columns_schema_name,
                             i, columns_schema_types[i].value])

    schema = Schema(columns_table_name, columns_schema_names,
                    columns_schema_types)
    _tables[columns_table_name] = Table(schema, columns_data)


def _create_indices_table():
    table_name = "#indices"
    column_names = [f"{table_name}.name",
                    f"{table_name}.table",
                    f"{table_name}.column"]
    column_types = [SchemaType.VARCHAR, SchemaType.VARCHAR, SchemaType.VARCHAR]
    records = []
    for table in _indices:
        for column in _indices[table]:
            records.append([_get_index_name(table, column), table, column])
    schema = Schema(table_name, column_names, column_types)
    _tables[table_name] = Table(schema, records)


def retrieve_table(table_name, makeCopy=False):
    """
    This function returns a table specified by the table_name
    """
    try:
        if makeCopy:
            return deepcopy(_tables[table_name])

        return _tables[table_name]
    except KeyError:
        raise TableNotFoundException(table_name)


def table_exists(name):
    return name in _tables


def _get_index_name(table_name, index_column):
    return f"{table_name}_{index_column}"


def retrieve_index(table_name, index_column):
    try:
        return _indices[table_name][index_column]
    except KeyError:
        raise IndexNotFoundException(
            f'Index with name "{_get_index_name(table_name, index_column)}" does not exist')


def index_exists(table_name, index_column):
    return table_name in _indices and index_column in _indices[table_name]


def initialize():
    """
    Clears the stored tables and creates #tables and #columns table at start
    """
    global _tables
    global _indices
    _tables = dict()
    _indices = dict()
    _create_indices_table()
    _create_tables_table()
    _create_columns_table()
