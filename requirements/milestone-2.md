# Milestone 2

- **Submission Due**: 2022-05-09

## Description

Your task is parsing queries and turning them into execution plans. You need to implement the basic plan operators (relation access, selection, projection, set operations, cross joins, sorting, and duplicate elimination).

## Compiling Queries

For this milestone, you will have to compile queries into execution plans. The following section will give you a few pointers as to how this can be done. This is only applicable if you use the query parser provided by us.

The parser framework we use for our parser, Parsimonious, uses a visitor pattern to traverse generated abstract syntax trees. You should use such a visitor to turn your AST into an execution plan. We provide you with a template for the visitor class (see `compiler.py`). This template contains all the methods your visitor needs to compile queries written for the grammar of our query language.

For some of the methods, we provide you with commented example code that could be used in a solution. Keep in mind that this example code is written for our reference implementation. You will therefore not be able to use it exactly as-is, but it should give you a good idea about the compiler logic. The methods that do not contain commented example code are either trivial, or can be solved with very similar logic to methods that do contain example code. You are expected to recognize similarities and transfer the logic accordingly by yourselves.

For a minimal example of a working compiler see the [calculator.py](calculator.py) sample code.

## Query Language

In the following you find a description of the query language features that your system should support. They are presented in form of a brief operator description and example queries. Your system will be tested using the queries given in the use cases section.

### Projection

You need to implement the projection operator. Look at the following example for a basic query using projection on a table. Keep in mind that the operator needs to work on temporary relations as well.

Input query:

```txt
pi column_name, #columns.data_type, ordinal_position #columns;
```

Expected output example:

```txt
+---------------------------+--------------------+---------------------------+
| #columns.column_name      | #columns.data_type | #columns.ordinal_position |
+---------------------------+--------------------+---------------------------+
| professoren.PersNr        | int                |                         0 |
| professoren.Name          | varchar            |                         1 |
| professoren.Rang          | varchar            |                         2 |
| professoren.Raum          | varchar            |                         3 |
| vorlesungen.VorlNr        | int                |                         0 |
| vorlesungen.Titel         | varchar            |                         1 |
| vorlesungen.SWS           | int                |                         2 |
| vorlesungen.gelesenVon    | int                |                         3 |
| voraussetzen.Vorgaenger   | int                |                         0 |
| voraussetzen.Nachfolger   | int                |                         1 |
| assistenten.PersNr        | int                |                         0 |
| assistenten.Name          | varchar            |                         1 |
| assistenten.Fachgebiet    | varchar            |                         2 |
| assistenten.Boss          | int                |                         3 |
| studenten.MatrNr          | int                |                         0 |
| studenten.Name            | varchar            |                         1 |
| studenten.Semester        | int                |                         2 |
| hoeren.MatrNr             | int                |                         0 |
| hoeren.VorlNr             | int                |                         1 |
| pruefen.MatrNr            | int                |                         0 |
| pruefen.VorlNr            | int                |                         1 |
| pruefen.PersNr            | int                |                         2 |
| pruefen.Note              | int                |                         3 |
| #tables.table_name        | varchar            |                         0 |
| #columns.table_name       | varchar            |                         0 |
| #columns.column_name      | varchar            |                         1 |
| #columns.ordinal_position | int                |                         2 |
| #columns.data_type        | varchar            |                         3 |
+---------------------------+--------------------+---------------------------+
Executed query in 2.072e-04 seconds.
```

### Rename Column Name

Renaming columns should be implemented using the `as` keyword in the column list of the projection operator. Look at the following example.

Input query:

```txt
pi PersNr, Name, FullName as "Prof. " + professoren.Name, NewName as Name professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------+
| professoren.PersNr | professoren.Name | FullName         | NewName    |
+--------------------+------------------+------------------+------------+
|               2125 | Sokrates         | Prof. Sokrates   | Sokrates   |
|               2126 | Russel           | Prof. Russel     | Russel     |
|               2127 | Kopernikus       | Prof. Kopernikus | Kopernikus |
|               2133 | Popper           | Prof. Popper     | Popper     |
|               2134 | Augustinus       | Prof. Augustinus | Augustinus |
|               2136 | Curie            | Prof. Curie      | Curie      |
|               2137 | Kant             | Prof. Kant       | Kant       |
+--------------------+------------------+------------------+------------+
Executed query in 1.452e-04 seconds.
```

### Selection

You need to implement the selection operator. Look at the following example for a basic query using selection on a table. Keep in mind that the operator needs to work on temporary relations as well.

Input query:

```txt
sigma Rang > "C3" professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2125 | Sokrates         | C4               | 226              |
|               2126 | Russel           | C4               | 232              |
|               2136 | Curie            | C4               | 36               |
|               2137 | Kant             | C4               | 7                |
+--------------------+------------------+------------------+------------------+
Executed query in 1.416e-04 seconds.
```

### Rename Relation

Renaming relations should be implemented using the `as` keyword. Look at the following example. Keep in mind that this only needs to work for tables.

Input query:

```txt
professoren as employee;
```

Expected output example:

```txt
+-----------------+---------------+---------------+---------------+
| employee.PersNr | employee.Name | employee.Rang | employee.Raum |
+-----------------+---------------+---------------+---------------+
|            2125 | Sokrates      | C4            | 226           |
|            2126 | Russel        | C4            | 232           |
|            2127 | Kopernikus    | C3            | 310           |
|            2133 | Popper        | C3            | 52            |
|            2134 | Augustinus    | C3            | 309           |
|            2136 | Curie         | C4            | 36            |
|            2137 | Kant          | C4            | 7             |
+-----------------+---------------+---------------+---------------+
Executed query in 1.001e-05 seconds.
```

### Union

The union operator should be implemented using the `union` keyword. Look at the following example for a basic query using union.
The implementation of the union operator has to follow the semantic of a "union all", which means that the result needs to preserve duplicates.

Input query:

```txt
pi PersNr, Name, Stelle as "Professor" professoren union pi PersNr, Name, Stelle as "Assistent" assistenten;
```

Expected output example:

```txt
+--------------------+------------------+-----------+
| professoren.PersNr | professoren.Name | Stelle    |
+--------------------+------------------+-----------+
|               2125 | Sokrates         | Professor |
|               2126 | Russel           | Professor |
|               2127 | Kopernikus       | Professor |
|               2133 | Popper           | Professor |
|               2134 | Augustinus       | Professor |
|               2136 | Curie            | Professor |
|               2137 | Kant             | Professor |
|               3002 | Platon           | Assistent |
|               3003 | Aristoteles      | Assistent |
|               3004 | Wittgenstein     | Assistent |
|               3005 | Rhetikus         | Assistent |
|               3006 | Newton           | Assistent |
|               3007 | Spinoza          | Assistent |
+--------------------+------------------+-----------+
Executed query in 1.655e-04 seconds.
```

### Intersection

The intersection operator should be implemented using the `intersect` keyword. Look at the following example for a basic query using intersection.

Input query:

```txt
sigma PersNr < 2135 professoren intersect sigma PersNr > 2125 professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2126 | Russel           | C4               | 232              |
|               2133 | Popper           | C3               | 52               |
|               2134 | Augustinus       | C3               | 309              |
|               2127 | Kopernikus       | C3               | 310              |
+--------------------+------------------+------------------+------------------+
Executed query in 2.408e-04 seconds.
```

### Difference

The difference operator should be implemented using the `except` keyword. Look at the following example for a basic query using difference.

Input query:

```txt
professoren except sigma PersNr >= 2127 professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2126 | Russel           | C4               | 232              |
|               2125 | Sokrates         | C4               | 226              |
+--------------------+------------------+------------------+------------------+
Executed query in 1.614e-04 seconds.
```

### Cross Product/Join

You need to implement the cross join operator using the keyword `cross join`. Look at the following example for a basic query using cross product.

Input query:

```txt
pi Rang, Name professoren cross join pi Name assistenten;
```

Expected output example:

```txt
+------------------+------------------+------------------+
| professoren.Rang | professoren.Name | assistenten.Name |
+------------------+------------------+------------------+
| C4               | Sokrates         | Platon           |
| C4               | Sokrates         | Aristoteles      |
| C4               | Sokrates         | Wittgenstein     |
| C4               | Sokrates         | Rhetikus         |
| C4               | Sokrates         | Newton           |
| C4               | Sokrates         | Spinoza          |
| C4               | Russel           | Platon           |
| C4               | Russel           | Aristoteles      |
| C4               | Russel           | Wittgenstein     |
| C4               | Russel           | Rhetikus         |
| C4               | Russel           | Newton           |
| C4               | Russel           | Spinoza          |
| C3               | Kopernikus       | Platon           |
| C3               | Kopernikus       | Aristoteles      |
| C3               | Kopernikus       | Wittgenstein     |
| C3               | Kopernikus       | Rhetikus         |
| C3               | Kopernikus       | Newton           |
| C3               | Kopernikus       | Spinoza          |
| C3               | Popper           | Platon           |
| C3               | Popper           | Aristoteles      |
| C3               | Popper           | Wittgenstein     |
| C3               | Popper           | Rhetikus         |
| C3               | Popper           | Newton           |
| C3               | Popper           | Spinoza          |
| C3               | Augustinus       | Platon           |
| C3               | Augustinus       | Aristoteles      |
| C3               | Augustinus       | Wittgenstein     |
| C3               | Augustinus       | Rhetikus         |
| C3               | Augustinus       | Newton           |
| C3               | Augustinus       | Spinoza          |
| C4               | Curie            | Platon           |
| C4               | Curie            | Aristoteles      |
| C4               | Curie            | Wittgenstein     |
| C4               | Curie            | Rhetikus         |
| C4               | Curie            | Newton           |
| C4               | Curie            | Spinoza          |
| C4               | Kant             | Platon           |
| C4               | Kant             | Aristoteles      |
| C4               | Kant             | Wittgenstein     |
| C4               | Kant             | Rhetikus         |
| C4               | Kant             | Newton           |
| C4               | Kant             | Spinoza          |
+------------------+------------------+------------------+
Executed query in 6.030e-04 seconds.
```

### Sorting

You need to implement the sorting operator using the keyword `tau`. Look at the following example for a query using sorting. Note that we alway sort in ascending order.

Input query:

```txt
tau Name pi PersNr, Name assistenten;
```

Expected output example:

```txt
+--------------------+------------------+
| assistenten.PersNr | assistenten.Name |
+--------------------+------------------+
|               3003 | Aristoteles      |
|               3006 | Newton           |
|               3002 | Platon           |
|               3005 | Rhetikus         |
|               3007 | Spinoza          |
|               3004 | Wittgenstein     |
+--------------------+------------------+
Executed query in 7.582e-05 seconds.
```

### Explain

You need to implement an explain operator using the keyword `explain`. This operator creates a result relation that describes the execution plan. Therefore, it is necessary to implement the explain for every plan operator in your system. Use proper identation and give necessary information for each operator.

Look at the following two examples of queries using explain.

Input query:

```txt
explain pi PersNr, Name professoren;
```

Expected output example:

```txt
+---------------------------------------------------------------------------------------------------+
| Operator                                                                                          |
+---------------------------------------------------------------------------------------------------+
| -->Projection(columns=[professoren.PersNr=professoren.PersNr, professoren.Name=professoren.Name]) |
| ---->TableScan(professoren)                                                                       |
+---------------------------------------------------------------------------------------------------+
Executed query in 2.027e-05 seconds.
```

Input query:

```txt
explain pi PersNr, Name professoren cross join pi PersNr, Name, Boss assistenten;
```

Expected output example:

```txt
+----------------------------------------------------------------------------------------------------------------------------------------+
| Operator                                                                                                                               |
+----------------------------------------------------------------------------------------------------------------------------------------+
| -->NestedLoopsJoin(cross, natural=True, condition=None)                                                                                |
| ---->Projection(columns=[professoren.PersNr=professoren.PersNr, professoren.Name=professoren.Name])                                    |
| ------>TableScan(professoren)                                                                                                          |
| ---->Projection(columns=[assistenten.PersNr=assistenten.PersNr, assistenten.Name=assistenten.Name, assistenten.Boss=assistenten.Boss]) |
| ------>TableScan(assistenten)                                                                                                          |
+----------------------------------------------------------------------------------------------------------------------------------------+
Executed query in 1.233e-04 seconds.
```

## Use Cases

After the completion of milestone 2, your system needs to at least support the following use cases.
It is assumed that the system was started using the `data/kemper` dataset.
Further, ensure that the given queries have the same results when entered in the prompt and when loaded from a query file (`--query-file` command line option and `\execute` command in prompt).

### Simple Queries

<!-- queries/milestone2/simple01.mql -->

Input query:

```txt
#columns;
```

Expected output example:

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
Executed query in 1.454e-05 seconds.
```

<!-- queries/milestone2/simple02.mql -->

Input query:

```txt
#tables;
```

Expected output example:

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
Executed query in 5.484e-06 seconds.
```

### Alias Queries

<!-- queries/milestone2/alias01.mql -->

Input query:

```txt
professoren as profs;
```

Expected output example:

```txt
+--------------+------------+------------+------------+
| profs.PersNr | profs.Name | profs.Rang | profs.Raum |
+--------------+------------+------------+------------+
|         2125 | Sokrates   | C4         | 226        |
|         2126 | Russel     | C4         | 232        |
|         2127 | Kopernikus | C3         | 310        |
|         2133 | Popper     | C3         | 52         |
|         2134 | Augustinus | C3         | 309        |
|         2136 | Curie      | C4         | 36         |
|         2137 | Kant       | C4         | 7          |
+--------------+------------+------------+------------+
Executed query in 4.768e-06 seconds.
```

<!-- queries/milestone2/alias02.mql -->

Input query:

```txt
pi profs.Name professoren as profs;
```

Expected output example:

```txt
+------------+
| profs.Name |
+------------+
| Sokrates   |
| Russel     |
| Kopernikus |
| Popper     |
| Augustinus |
| Curie      |
| Kant       |
+------------+
Executed query in 1.335e-05 seconds.
```

### Projection Queries

<!-- queries/milestone2/projection01.mql -->

Input query:

```txt
pi distinct table_name #columns;
```

Expected output example:

```txt
+---------------------+
| #columns.table_name |
+---------------------+
| professoren         |
| vorlesungen         |
| voraussetzen        |
| assistenten         |
| studenten           |
| hoeren              |
| pruefen             |
| #tables             |
| #columns            |
+---------------------+
Executed query in 3.505e-05 seconds.
```

### Selection Queries

<!-- queries/milestone2/selection01.mql -->

Input query:

```txt
sigma table_name = "pruefen" #columns;
```

Expected output example:

```txt
+---------------------+----------------------+---------------------------+--------------------+
| #columns.table_name | #columns.column_name | #columns.ordinal_position | #columns.data_type |
+---------------------+----------------------+---------------------------+--------------------+
| pruefen             | pruefen.MatrNr       |                         0 | int                |
| pruefen             | pruefen.VorlNr       |                         1 | int                |
| pruefen             | pruefen.PersNr       |                         2 | int                |
| pruefen             | pruefen.Note         |                         3 | int                |
+---------------------+----------------------+---------------------------+--------------------+
Executed query in 7.129e-05 seconds.
```

<!-- queries/milestone2/selection02.mql -->

Input query:

```txt
sigma Rang = "C4" professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2125 | Sokrates         | C4               | 226              |
|               2126 | Russel           | C4               | 232              |
|               2136 | Curie            | C4               | 36               |
|               2137 | Kant             | C4               | 7                |
+--------------------+------------------+------------------+------------------+
Executed query in 3.314e-05 seconds.
```

<!-- queries/milestone2/selection03.mql -->

Input query:

```txt
sigma Rang = "C4" and Raum >= "3" professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2136 | Curie            | C4               | 36               |
|               2137 | Kant             | C4               | 7                |
+--------------------+------------------+------------------+------------------+
Executed query in 1.185e-04 seconds.
```

<!-- queries/milestone2/selection04.mql -->

Input query:

```txt
sigma Rang = "C4" or Name = "Kopernikus" professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2125 | Sokrates         | C4               | 226              |
|               2126 | Russel           | C4               | 232              |
|               2127 | Kopernikus       | C3               | 310              |
|               2136 | Curie            | C4               | 36               |
|               2137 | Kant             | C4               | 7                |
+--------------------+------------------+------------------+------------------+
Executed query in 5.341e-05 seconds.
```

### Union Queries

<!-- queries/milestone2/union01.mql -->

Input query:

```txt
pi Name professoren union pi Name studenten;
```

Expected output example:

```txt
+------------------+
| professoren.Name |
+------------------+
| Sokrates         |
| Russel           |
| Kopernikus       |
| Popper           |
| Augustinus       |
| Curie            |
| Kant             |
| Xenokrates       |
| Jonas            |
| Fichte           |
| Aristoxenos      |
| Schopenhauer     |
| Carnap           |
| Theophrastos     |
| Feuerbach        |
+------------------+
Executed query in 2.670e-05 seconds.
```

### Join Queries

<!-- queries/milestone2/join01.mql -->

Input query:

```txt
studenten cross join hoeren;
```

Expected output example:

```txt
+------------------+----------------+--------------------+---------------+---------------+
| studenten.MatrNr | studenten.Name | studenten.Semester | hoeren.MatrNr | hoeren.VorlNr |
+------------------+----------------+--------------------+---------------+---------------+
|            24002 | Xenokrates     |                 18 |         26120 |          5001 |
|            24002 | Xenokrates     |                 18 |         27550 |          5001 |
|            24002 | Xenokrates     |                 18 |         27550 |          4052 |
|            24002 | Xenokrates     |                 18 |         28106 |          5041 |
|            24002 | Xenokrates     |                 18 |         28106 |          5052 |
|            24002 | Xenokrates     |                 18 |         28106 |          5216 |
|            24002 | Xenokrates     |                 18 |         28106 |          5259 |
|            24002 | Xenokrates     |                 18 |         29120 |          5001 |
|            24002 | Xenokrates     |                 18 |         29120 |          5041 |
|            24002 | Xenokrates     |                 18 |         29120 |          5049 |
|            25403 | Jonas          |                 12 |         26120 |          5001 |
|            25403 | Jonas          |                 12 |         27550 |          5001 |
|            25403 | Jonas          |                 12 |         27550 |          4052 |
|            25403 | Jonas          |                 12 |         28106 |          5041 |
|            25403 | Jonas          |                 12 |         28106 |          5052 |
|            25403 | Jonas          |                 12 |         28106 |          5216 |
|            25403 | Jonas          |                 12 |         28106 |          5259 |
|            25403 | Jonas          |                 12 |         29120 |          5001 |
|            25403 | Jonas          |                 12 |         29120 |          5041 |
|            25403 | Jonas          |                 12 |         29120 |          5049 |
|            26120 | Fichte         |                 10 |         26120 |          5001 |
|            26120 | Fichte         |                 10 |         27550 |          5001 |
|            26120 | Fichte         |                 10 |         27550 |          4052 |
|            26120 | Fichte         |                 10 |         28106 |          5041 |
|            26120 | Fichte         |                 10 |         28106 |          5052 |
|            26120 | Fichte         |                 10 |         28106 |          5216 |
|            26120 | Fichte         |                 10 |         28106 |          5259 |
|            26120 | Fichte         |                 10 |         29120 |          5001 |
|            26120 | Fichte         |                 10 |         29120 |          5041 |
|            26120 | Fichte         |                 10 |         29120 |          5049 |
|            26830 | Aristoxenos    |                  8 |         26120 |          5001 |
|            26830 | Aristoxenos    |                  8 |         27550 |          5001 |
|            26830 | Aristoxenos    |                  8 |         27550 |          4052 |
|            26830 | Aristoxenos    |                  8 |         28106 |          5041 |
|            26830 | Aristoxenos    |                  8 |         28106 |          5052 |
|            26830 | Aristoxenos    |                  8 |         28106 |          5216 |
|            26830 | Aristoxenos    |                  8 |         28106 |          5259 |
|            26830 | Aristoxenos    |                  8 |         29120 |          5001 |
|            26830 | Aristoxenos    |                  8 |         29120 |          5041 |
|            26830 | Aristoxenos    |                  8 |         29120 |          5049 |
|            27550 | Schopenhauer   |                  6 |         26120 |          5001 |
|            27550 | Schopenhauer   |                  6 |         27550 |          5001 |
|            27550 | Schopenhauer   |                  6 |         27550 |          4052 |
|            27550 | Schopenhauer   |                  6 |         28106 |          5041 |
|            27550 | Schopenhauer   |                  6 |         28106 |          5052 |
|            27550 | Schopenhauer   |                  6 |         28106 |          5216 |
|            27550 | Schopenhauer   |                  6 |         28106 |          5259 |
|            27550 | Schopenhauer   |                  6 |         29120 |          5001 |
|            27550 | Schopenhauer   |                  6 |         29120 |          5041 |
|            27550 | Schopenhauer   |                  6 |         29120 |          5049 |
|            28106 | Carnap         |                  3 |         26120 |          5001 |
|            28106 | Carnap         |                  3 |         27550 |          5001 |
|            28106 | Carnap         |                  3 |         27550 |          4052 |
|            28106 | Carnap         |                  3 |         28106 |          5041 |
|            28106 | Carnap         |                  3 |         28106 |          5052 |
|            28106 | Carnap         |                  3 |         28106 |          5216 |
|            28106 | Carnap         |                  3 |         28106 |          5259 |
|            28106 | Carnap         |                  3 |         29120 |          5001 |
|            28106 | Carnap         |                  3 |         29120 |          5041 |
|            28106 | Carnap         |                  3 |         29120 |          5049 |
|            29120 | Theophrastos   |                  2 |         26120 |          5001 |
|            29120 | Theophrastos   |                  2 |         27550 |          5001 |
|            29120 | Theophrastos   |                  2 |         27550 |          4052 |
|            29120 | Theophrastos   |                  2 |         28106 |          5041 |
|            29120 | Theophrastos   |                  2 |         28106 |          5052 |
|            29120 | Theophrastos   |                  2 |         28106 |          5216 |
|            29120 | Theophrastos   |                  2 |         28106 |          5259 |
|            29120 | Theophrastos   |                  2 |         29120 |          5001 |
|            29120 | Theophrastos   |                  2 |         29120 |          5041 |
|            29120 | Theophrastos   |                  2 |         29120 |          5049 |
|            29555 | Feuerbach      |                  2 |         26120 |          5001 |
|            29555 | Feuerbach      |                  2 |         27550 |          5001 |
|            29555 | Feuerbach      |                  2 |         27550 |          4052 |
|            29555 | Feuerbach      |                  2 |         28106 |          5041 |
|            29555 | Feuerbach      |                  2 |         28106 |          5052 |
|            29555 | Feuerbach      |                  2 |         28106 |          5216 |
|            29555 | Feuerbach      |                  2 |         28106 |          5259 |
|            29555 | Feuerbach      |                  2 |         29120 |          5001 |
|            29555 | Feuerbach      |                  2 |         29120 |          5041 |
|            29555 | Feuerbach      |                  2 |         29120 |          5049 |
+------------------+----------------+--------------------+---------------+---------------+
Executed query in 2.201e-04 seconds.
```

### Ordering Queries

<!-- queries/milestone2/ordering01.mql -->

Input query:

```txt
tau Rang professoren;
```

Expected output example:

```txt
+--------------------+------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | professoren.Rang | professoren.Raum |
+--------------------+------------------+------------------+------------------+
|               2127 | Kopernikus       | C3               | 310              |
|               2133 | Popper           | C3               | 52               |
|               2134 | Augustinus       | C3               | 309              |
|               2125 | Sokrates         | C4               | 226              |
|               2126 | Russel           | C4               | 232              |
|               2136 | Curie            | C4               | 36               |
|               2137 | Kant             | C4               | 7                |
+--------------------+------------------+------------------+------------------+
Executed query in 2.289e-05 seconds.
```

### Explain Queries

<!-- queries/milestone2/explain01.mql -->

Input query:

```txt
explain (pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren));
```

Expected output example:

```txt
+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Operator                                                                                                                                                      |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| -->Projection(columns=[studenten.MatrNr=studenten.MatrNr, studenten.Name=studenten.Name, studenten.Semester=studenten.Semester, hoeren.VorlNr=hoeren.VorlNr]) |
| ---->Selection(condition=(studenten.MatrNr = hoeren.MatrNr))                                                                                                  |
| ------>NestedLoopsJoin(cross, natural=True, condition=None)                                                                                                   |
| -------->TableScan(studenten)                                                                                                                                 |
| -------->TableScan(hoeren)                                                                                                                                    |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
Executed query in 1.547e-04 seconds.
```

### Combo Queries

<!-- queries/milestone2/combo01.mql -->

Input query:

```txt
sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren);
```

Expected output example:

```txt
+------------------+----------------+--------------------+---------------+---------------+
| studenten.MatrNr | studenten.Name | studenten.Semester | hoeren.MatrNr | hoeren.VorlNr |
+------------------+----------------+--------------------+---------------+---------------+
|            26120 | Fichte         |                 10 |         26120 |          5001 |
|            27550 | Schopenhauer   |                  6 |         27550 |          5001 |
|            27550 | Schopenhauer   |                  6 |         27550 |          4052 |
|            28106 | Carnap         |                  3 |         28106 |          5041 |
|            28106 | Carnap         |                  3 |         28106 |          5052 |
|            28106 | Carnap         |                  3 |         28106 |          5216 |
|            28106 | Carnap         |                  3 |         28106 |          5259 |
|            29120 | Theophrastos   |                  2 |         29120 |          5001 |
|            29120 | Theophrastos   |                  2 |         29120 |          5041 |
|            29120 | Theophrastos   |                  2 |         29120 |          5049 |
+------------------+----------------+--------------------+---------------+---------------+
Executed query in 4.096e-04 seconds.
```

<!-- queries/milestone2/combo02.mql -->

Input query:

```txt
(pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren)) except (pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren));
```

Expected output example:

```txt
+------------------+----------------+--------------------+---------------+
| studenten.MatrNr | studenten.Name | studenten.Semester | hoeren.VorlNr |
+------------------+----------------+--------------------+---------------+
+------------------+----------------+--------------------+---------------+
Executed query in 6.649e-04 seconds.
```

<!-- queries/milestone2/combo03.mql -->

Input query:

```txt
((pi VorlNr sigma MatrNr = 29120 hoeren) intersect (pi VorlNr sigma gelesenVon = 2125 vorlesungen));
```

Expected output example:

```txt
+---------------+
| hoeren.VorlNr |
+---------------+
|          5041 |
|          5049 |
+---------------+
Executed query in 3.059e-04 seconds.
```

## Submission

- All needed files present in your repository
- All instructions (how to compile, test and run) in the README.

## Grading

For this milestone, your can achieve a maximum of ten points. The points will be allocated as follows:

- Eight points based on how well your systems supports the provided uses cases. To receive all eight points, your system needs to support everything described in the given use cases, and adequately handle error cases (i.e., be stable, don't crash etc.).
- Two points for the quality of your code. Try to develop clean and maintainable code - you will thank yourselves for it later in the project.
