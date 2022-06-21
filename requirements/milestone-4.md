# Milestone 4

- **Submission Due**: 2022-06-20

## Description

For this milestone, you will implement simple hash indices and make sure your system uses them when possible.

In the following, you find a description of how the data files specify the hash indices your database should create on startup. As always, your system will be tested using the queries given in the use cases section.

## Hash Indices

### Specifying Hash Indices

For this milestone, you get a modified dataset named `kemper02`. You can find the files for it in the repository of the reference implementation.

To specify indices, a new (optional) section is added to the `.table` files. The section is called `[Indices]`. Following it is a list of columns for which indices should be created. Note: Every index in our simple system has only one column it indexes. When a file lists multiple columns for indexing, that means you should create multiple indices. An example `[Indices]` section could look like the following:

```txt
[Indices]
MatrNr
Name
```

For a table file with this `[Indices]` section, you should create two hash indices for the corresponding table:

- one on the column called `MatrNr`
- one on the column called `Name`

Every hash index in your system gets a name. You can choose this name as you like, but a simple solution would be a name in the schema `{table_name}_{column_name}`.

To implement your hash indices, you can simply use Python dictionaries.

### Using Hash Indices

Whenever a query contains a selection on a single column of a table with a constant value, you can potentially make use of a hash index on that column (if one exists). For your implementation, you can use the following logic: if a `TableScan` is preceded by one or multiple `Selection` operators, check if one or more of the selections can be implemented using an existing index. If yes, replace the `TableScan` and the corresponding `Selection` with an `IndexSeek`. If multiple selections could be used, you can choose which one you fold into the `IndexSeek` (remember: only one column per index).

For example, lets assume there is a hash index each on the columns `PersNr` and `Name` of table `professoren`. If we have a query plan like

```txt
-->Selection(condition=(professoren.Name = "Russel"))
---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
------>TableScan(professoren)
------>TableScan(vorlesungen)
```

you can fold the `Selection` and the `TableScan` together into the following plan:

```txt
-->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
---->IndexSeek(professoren_name, professoren.Name = "Russel")
---->TableScan(vorlesungen)
```

This should only be done if optimizations are turned on.

**Hint: There might be a phase during your optimization procedure where you conveniently have all selections split up so that they only refer to single columns and pushed down to directly precede the `TableScan`. Putting the index selection logic in the right position within your optimization pipeline will make things a lot easier.**

## Use Cases

### Indices Information Schema

You have to add a special table `#indices` to your system, which shows the indices that currently exist in the database. Sample entries in that table could look like the following:

```txt
+--------------------+----------------+-----------------+
| #indices.name      | #indices.table | #indices.column |
+--------------------+----------------+-----------------+
| professoren_Name   | professoren    | Name            |
| professoren_PersNr | professoren    | PersNr          |
| studenten_Name     | studenten      | Name            |
+--------------------+----------------+-----------------+
```

### Selection Pushdown Revisited

Selection pushdown has some corner cases that have to be handled correctly. One such case is pushing down a selection over a projection that introduces a column that is then referenced in the selection, and which shadows an already existing column. We cannot push down the selection in that case, as its meaning would change and therefore the query would produce a different result.

Input query:

```txt
explain sigma Name = "C3" pi Name as Rang professoren;
```

Expected output example (optimizations turned on):

```txt
+--------------------------------------------------------------+
| Operator                                                     |
+--------------------------------------------------------------+
| --> Selection(condition=(professoren.Name = "C3"))           |
| ---->Projection(columns=[professoren.Name=professoren.Rang]) |
| ------>TableScan(professoren)                                |
+--------------------------------------------------------------+
Executed query in 2.027e-05 seconds.
```

Expected output example (optimizations turned off): the same.

### Selection Pushdown Results

When running the above query (and similar ones) without the explain the output needs to be the same independent whether optimization is on or off.

Input query:

```txt
sigma Name = "C3" pi Name as Rang professoren;
```

Expected output:

```txt
+------+
| Name |
+------+
| C3   |
| C3   |
| C3   |
+------+
Executed query in 1.674e-04 seconds.
```

### Using An Existing Index

Input query:

```txt
explain sigma PersNr = 2126 (professoren join professoren.PersNr = vorlesungen.gelesenVon vorlesungen);
```

Expected output example (optimizations turned on):

```txt
+---------------------------------------------------------------------------------------------------+
| Operator                                                                                          |
+---------------------------------------------------------------------------------------------------+
| -->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon)) |
| ---->IndexSeek(professoren_PersNr, condition=(professoren.PersNr = 2126))                           |
| ---->TableScan(vorlesungen)                                                                       |
+---------------------------------------------------------------------------------------------------+
Executed query in 2.027e-05 seconds.
```

Input query:

```txt
explain sigma Name = "Fichte" (pi Name professoren union pi Name studenten);
```

Expected output example (optimizations turned on):

```txt
+-------------------------------------------------------------------------+
| Operator                                                                |
+-------------------------------------------------------------------------+
| -->Union()                                                              |
| ---->Projection(columns=[professoren.Name=professoren.Name])            |
| ------>Selection(condition=(professoren.Name = "Fichte"))               |
| -------->TableScan(professoren)                                         |
| ---->Projection(columns=[studenten.Name=studenten.Name])                |
| ------>IndexSeek(studenten_Name, condition=(studenten.Name = "Fichte")) |
+-------------------------------------------------------------------------+
Executed query in 2.027e-05 seconds.
```

### Using An Existing Index Results

When running the above query (and similar ones) without the explain the output needs to be the same independent whether optimization is on or off.

Input query:

```txt
sigma Name = "Fichte" (pi Name professoren union pi Name studenten);
```

Expected output:

```txt
+------------------+
| professoren.Name |
+------------------+
| Fichte           |
+------------------+
Executed query in 2.091e-04 seconds.
```

## Submission

- All needed files present in your repository
- All instructions (how to compile, test and run) in the README.

## Grading

For this milestone, your can achieve a maximum of twenty points. The points will be allocated as follows:

- Eight points based on how well your systems supports the provided uses cases for milestone 4. To receive all eight points, your system needs to support everything described in the given use cases, and adequately handle error cases (i.e., be stable, don't crash etc.).
- Two points for the quality of your code. Try to develop clean and maintainable code - you will thank yourselves for it later in the project.
- Ten (additional) points based on how well your final system implements the use cases for milestones 1 through 3.
