# Milestone 1

- **Submission Due**: 2022-03-28

## Description

For your project, you will have to implement a small, simplified database system that supports queries in a query language very closely inspired by relational algebra, as well as some additional commands.

For the first milestone, you have to implement the following things:

- loading tables and table data from files in a custom format,
- a simplified version of the INFORMATION_SCHEMA found in real relational databases, and
- a CLI interface that supports some basic commands and is also able to print the contents of a given table.

In the following, we will provide a brief specification of what the tables in your database system should look like. Following that, we will give use cases that your system will have to support after the first milestone is complete.

## Tables

As you know, relations in a relational database are typically represented as tables. A table has a defined set of columns (also called its _schema_) and stores data as a collection of rows, each of which follows the schema of the table. In the relational model, single rows are sometimes also referred to as a _tuple_. This has its roots in the mathematical foundations of the relational model, but you may consider taking it literally - a good data structure for storing table data in Python is a list of tuples.

As already mentioned, every table has a fixed schema, which describes the columns of the table. Each column is defined by

- its name,
- its ordinal position (i.e., the index of the column), and
- its datatype.

For example, consider the following table, called `studenten`:

```txt
+------------------+----------------+--------------------+
| studenten.MatrNr | studenten.Name | studenten.Semester |
+------------------+----------------+--------------------+
|            24002 | Xenokrates     |                 18 |
|            25403 | Jonas          |                 12 |
|            26120 | Fichte         |                 10 |
|            26830 | Aristoxenos    |                  8 |
|            27550 | Schopenhauer   |                  6 |
|            28106 | Carnap         |                  3 |
|            29120 | Theophrastos   |                  2 |
|            29555 | Feuerbach      |                  2 |
+------------------+----------------+--------------------+
```

The schema of this table consists of three columns:

1. name: `studenten.MatrNr`, ordinal position: 0, datatype: `int`
1. name: `studenten.Name`, ordinal position: 1, datatype: `varchar`
1. name: `studenten.Semester`, ordinal position: 2, datatype: `int`

Note how the name of the columns consist of two parts: the table name and a unique name for the column itself, separated by a dot. This is what we call a _fully-qualified name_. Make sure to store your column names accordingly, but keep in mind that, later on, we want to be able to refer to columns by only their _simple name_ (i.e., what comes after the dot) if doing so is unambiguous.

As for datatypes, the database system you have to develop for this course has to support the following three:

- `int`: integer numbers. If you develop your system in a language where you have to deal with the physical implementation of types, use at least 32 bit integers for this. Further, also use `int` to represent booleans.
- `float`: floating point numbers. Again, if you need to be exact here in your language, use at least 32 bit single precision floating point numbers for this.
- `varchar`: unicode strings of arbitrary length. To keep our type system simple, we do not support limiting the length of the strings we can store in each column. If you are using a low-level language, this will make things more complicated for you - you will have to come up with a way of storing arbitrary length strings in your records. Ways how to do this will be discussed in the course.

## Use Cases

After the completion of milestone 1, your system needs to at least support the following use cases.

### Start Application

Your applications has to automatically load the provided data that you can find in the [data folder](../code/data) of the [reference implementation](../code). The data format should be self-explanatory, but if you have trouble understanding it, please don't hesitate to ask questions about it.

The data folder needs to be present on the machine used to executed the following command on a shell:

```bash
<your-application> --data-directory <path-to-data>
```

The `<your-application>` part is substituted based on how your system has to be run. Your have to provide a description for this in your repository's README.

The expected outcome is that your application has loaded all the tables found in `<path-to-data>`, updated the INFORMATION_SCHEMA and provides a command prompt waiting for commands. When executed with `<path-to-data>` substituted with `data/kemper`, the output has to look as follows:

```txt
Data loaded from "data/kemper"

Welcome to Mosaic!

>>>
```

If no table files can be loaded, print an error message stating that and terminate the application. If only some of the table files could not be loaded, print an error message stating so, but continue loading other files.

### CLI Execute Query File

Your application needs to provide an optional command line option named `--query-file`. If this option is set, read the respective file and execute the query in it. Both queries printing a relation and an abstract syntax tree (see below) need to be supported and lead to the same output as if the query were run on the command prompt.

After executing the query, terminate your application. In other words, if the `--query-file` option is set, your system should not show a prompt or any other output other then the result of the query.

A call on a shell looks like the following:

```bash
<your-application> --data-directory <path-to-data> --query-file <path-to-query-file>
```

If execution problems arise, print a meaningful error message before terminating the application.

### Printing the Prompt

In general there are two types of input: commands and queries. Commands start with a `\` and queries are terminated with a `;`.

Commands are always single line inputs where the prompt looks like this `>>>`. Queries can either be single line, if the first line is terminated with a `;` or multiline. Except for the first line, all successive input lines in the prompt have to print `>`.

It needs to be possible to input the following as a query.

```txt
>>> #tables
>   ;
```

### Print Table Content

Your application needs to be able to print the contents of any given table. For this, a user will entry the name of the table as a query. For example, if the user wants to print the contents of a table called `hoeren`, they will enter the query `hoeren;` and your system has to print e.g.

```txt
+---------------+---------------+
| hoeren.MatrNr | hoeren.VorlNr |
+---------------+---------------+
|         26120 |          5001 |
|         27550 |          5001 |
|         27550 |          4052 |
|         28106 |          5041 |
|         28106 |          5052 |
|         28106 |          5216 |
|         28106 |          5259 |
|         29120 |          5001 |
|         29120 |          5041 |
|         29120 |          5049 |
+---------------+---------------+
```

### INFORMATION_SCHEMA

In addition to all the tables loaded from `<path-to-data>`, there have to be two special tables called `#tables` and `#columns`. These represent the previously mentioned INFORMATION_SCHEMA. Their columns can be derived from the example output below and the content needs to be determined from the loaded tables.

Entering the query `#tables;` for the `kemper` dataset has to result in the following output:

```txt
+--------------------+
| #tables.table_name |
+--------------------+
| professoren        |
| vorlesungen        |
| voraussetzen       |
| assistenten        |
| studenten          |
| hoeren             |
| pruefen            |
| #tables            |
| #columns           |
+--------------------+
```

Entering the query `#columns;` for the same dataset has to result in the following output:

```txt
+---------------------+---------------------------+---------------------------+--------------------+
| #columns.table_name | #columns.column_name      | #columns.ordinal_position | #columns.data_type |
+---------------------+---------------------------+---------------------------+--------------------+
| professoren         | professoren.PersNr        |                         0 | int                |
| professoren         | professoren.Name          |                         1 | varchar            |
| professoren         | professoren.Rang          |                         2 | varchar            |
| professoren         | professoren.Raum          |                         3 | varchar            |
| vorlesungen         | vorlesungen.VorlNr        |                         0 | int                |
| vorlesungen         | vorlesungen.Titel         |                         1 | varchar            |
| vorlesungen         | vorlesungen.SWS           |                         2 | int                |
| vorlesungen         | vorlesungen.gelesenVon    |                         3 | int                |
| voraussetzen        | voraussetzen.Vorgaenger   |                         0 | int                |
| voraussetzen        | voraussetzen.Nachfolger   |                         1 | int                |
| assistenten         | assistenten.PersNr        |                         0 | int                |
| assistenten         | assistenten.Name          |                         1 | varchar            |
| assistenten         | assistenten.Fachgebiet    |                         2 | varchar            |
| assistenten         | assistenten.Boss          |                         3 | int                |
| studenten           | studenten.MatrNr          |                         0 | int                |
| studenten           | studenten.Name            |                         1 | varchar            |
| studenten           | studenten.Semester        |                         2 | int                |
| hoeren              | hoeren.MatrNr             |                         0 | int                |
| hoeren              | hoeren.VorlNr             |                         1 | int                |
| pruefen             | pruefen.MatrNr            |                         0 | int                |
| pruefen             | pruefen.VorlNr            |                         1 | int                |
| pruefen             | pruefen.PersNr            |                         2 | int                |
| pruefen             | pruefen.Note              |                         3 | int                |
| #tables             | #tables.table_name        |                         0 | varchar            |
| #columns            | #columns.table_name       |                         0 | varchar            |
| #columns            | #columns.column_name      |                         1 | varchar            |
| #columns            | #columns.ordinal_position |                         2 | int                |
| #columns            | #columns.data_type        |                         3 | varchar            |
+---------------------+---------------------------+---------------------------+--------------------+
```

### Print AST from Parser

All queries that do not solely consist of a table name need to be parsed using the [provided parser](../code/src/mosaic/parser) or, if your are using a language other than Python, a parser written on your own supporting the same grammar. If such a query is entered in the prompt, you need to print the abstract syntax tree (AST) of that query.

For the query `pi hoeren.MatrNr hoeren;`, your system should, for example, print the following AST:

```TXT
<Node called "command" matching "pi hoeren.MatrNr hoeren">
    <Node called "query" matching "pi hoeren.MatrNr hoeren">
        <Node called "set_factor" matching "pi hoeren.MatrNr hoeren">
            <Node called "join_factor" matching "pi hoeren.MatrNr hoeren">
                <Node called "projection" matching "pi hoeren.MatrNr hoeren">
                    <Node matching "pi hoeren.MatrNr hoeren">
                        <Node called "projection_kw" matching "pi">
                            <RegexNode matching "pi">
                        <RegexNode called "mandatory_ws" matching " ">
                        <Node called "column_list" matching "hoeren.MatrNr ">
                            <Node called "column_reference" matching "hoeren.MatrNr ">
                                <Node called "name" matching "hoeren.MatrNr ">
                                    <RegexNode matching "hoeren.MatrNr">
                                    <RegexNode called "ws" matching " ">
                        <Node called "join_factor" matching "hoeren">
                            <Node called "relation_reference" matching "hoeren">
                                <Node matching "hoeren">
                                    <Node matching "">
                                    <RegexNode called "table_name" matching "hoeren">
                                    <RegexNode called "ws" matching "">
            <Node matching "">
        <Node matching "">
```

If you have written your own parser, the output may look different.

In case of parsing errors, print a meaningful error message and return to the prompt for further input.

### Print Error Messages

If an unknown command is given or if executing/parsing the query (table names followed by a semicolon are queries) fails, print a meaningful error message and allow a new input in the command prompt.

### Execute Command

Similar to the query execution command line option you have to provide a `\execute <query-file>` command that loads the given query file and executes the query in it.

Both queries that print the content of a relation and those that print an AST need to be supported and they need to have the same behavior as if the query were directly given via the prompt.

If execution problems arise, print a meaningful error message and return to the prompt for further input.

### Quit Application Command

Your CLI has to support a `\quit` command. If a user enters the `\quit` command in the command prompt of your application, the shutdown message `Bye!` should be printed and the application terminated.

### Help Command

Your CLI has to support a `\help` command. If a user enters the `\help` command in the command prompt of your application, you have to print an explanation for all available commands.

The output might look like the following:

```txt
\help                   shows this output.
\execute <query-file>   executes the query loaded from query-file.
\quit                   quits the application.

<query>                 executes a query that needs to be terminated by ";".
```

## Submission

- All needed files present in your repository
- All instructions (how to compile, test and run) in the README.

## Grading

For this milestone, your can achieve a maximum of ten points. The points will be allocated as follows:

- Eight points based on how well your systems supports the provided uses cases. To receive all eight points, your system needs to support everything described in the given use cases, and adequately handle error cases (i.e., be stable, don't crash etc.).
- Two points for the quality of your code. Try to develop clean and maintainable code - you will thank yourselves for it later in the project.
