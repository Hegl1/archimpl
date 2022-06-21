# Milestone 3

- **Submission Due**: 2022-06-06

## Description

For this milestone, you will have to solve two major tasks:

- complete the implementation of your query plan operators to also support joins and grouping/aggregation, and
- start implementing a simple optimizer that (for now) performs selection pushdown.

In the following, you find descriptions of the missing query language features that you have to implement (by implementing the necessary plan operators) as well as of the selection pushdown optimization. As always, your system will be tested using the queries given in the use cases section.

## Query Language

### Inner Join

You need to implement the inner join operator using the keyword `join`. Look at the following example for a basic query using inner join.

Input query:

```txt
pi PersNr, Name professoren join professoren.PersNr = assistenten.Boss pi PersNr, Name, Boss assistenten;
```

Expected output example:

```txt
+--------------------+------------------+--------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | assistenten.PersNr | assistenten.Name | assistenten.Boss |
+--------------------+------------------+--------------------+------------------+------------------+
|               2125 | Sokrates         |               3002 | Platon           |             2125 |
|               2125 | Sokrates         |               3003 | Aristoteles      |             2125 |
|               2126 | Russel           |               3004 | Wittgenstein     |             2126 |
|               2127 | Kopernikus       |               3005 | Rhetikus         |             2127 |
|               2127 | Kopernikus       |               3006 | Newton           |             2127 |
|               2134 | Augustinus       |               3007 | Spinoza          |             2134 |
+--------------------+------------------+--------------------+------------------+------------------+
Executed query in 1.057e-03 seconds.
```

### Left Outer Join

You need to implement the left outer join operator using the keyword `left join`. Look at the following example for a basic query using left outer join.

Input query:

```txt
pi PersNr, Name professoren left join professoren.PersNr = assistenten.Boss pi PersNr, Name, Boss assistenten;
```

Expected output example:

```txt
+--------------------+------------------+--------------------+------------------+------------------+
| professoren.PersNr | professoren.Name | assistenten.PersNr | assistenten.Name | assistenten.Boss |
+--------------------+------------------+--------------------+------------------+------------------+
|               2125 | Sokrates         |               3002 | Platon           |             2125 |
|               2125 | Sokrates         |               3003 | Aristoteles      |             2125 |
|               2126 | Russel           |               3004 | Wittgenstein     |             2126 |
|               2127 | Kopernikus       |               3005 | Rhetikus         |             2127 |
|               2127 | Kopernikus       |               3006 | Newton           |             2127 |
|               2133 | Popper           |               NULL | NULL             |             NULL |
|               2134 | Augustinus       |               3007 | Spinoza          |             2134 |
|               2136 | Curie            |               NULL | NULL             |             NULL |
|               2137 | Kant             |               NULL | NULL             |             NULL |
+--------------------+------------------+--------------------+------------------+------------------+
Executed query in 1.044e-03 seconds.
```

### Natural Inner Join

You need to implement the inner join operator using the keyword `natural join`. Look at the following example for a basic query using natural inner join.

Input query:

```txt
pi VorlNr, Titel vorlesungen natural join pi VorlNr, MatrNr, Note pruefen;
```

Expected output example:

```txt
+--------------------+-------------------+----------------+--------------+
| vorlesungen.VorlNr | vorlesungen.Titel | pruefen.MatrNr | pruefen.Note |
+--------------------+-------------------+----------------+--------------+
|               5001 | Grundzuege        |          28106 |            1 |
|               5041 | Ethik             |          25403 |            2 |
|               4630 | Glaube und wissen |          27550 |            2 |
+--------------------+-------------------+----------------+--------------+
```

### Natural Left Outer Join

You need to implement the natural left outer join operator using the keyword `natural left join`. Look at the following example for a basic query using natural left outer join.

Input query:

```txt
pi VorlNr, Titel vorlesungen natural left join pi VorlNr, MatrNr, Note pruefen;
```

Expected output example:

```txt
+--------------------+----------------------+----------------+--------------+
| vorlesungen.VorlNr | vorlesungen.Titel    | pruefen.MatrNr | pruefen.Note |
+--------------------+----------------------+----------------+--------------+
|               5001 | Grundzuege           |          28106 |            1 |
|               5041 | Ethik                |          25403 |            2 |
|               5043 | Erkenntnistheorie    |           NULL |         NULL |
|               5049 | Maeeutik             |           NULL |         NULL |
|               4052 | Logik                |           NULL |         NULL |
|               5052 | Wissenschaftstheorie |           NULL |         NULL |
|               5216 | Bioethik             |           NULL |         NULL |
|               5259 | Der Wiener Kreis     |           NULL |         NULL |
|               5022 | Glaube und wissen    |           NULL |         NULL |
|               4630 | Glaube und wissen    |          27550 |            2 |
+--------------------+----------------------+----------------+--------------+
```

### Grouping/Aggregation

You need to implement the grouping/aggregation operator using the keywords `gamma` and `aggregate`. Look at the following example for a basic query using grouping.

Input query:

```txt
gamma Rang aggregate Anzahl as count(PersNr) professoren;
```

Expected output example:

```txt
+------------------+--------+
| professoren.Rang | Anzahl |
+------------------+--------+
| C4               |      4 |
| C3               |      3 |
+------------------+--------+
Executed query in 3.839e-05 seconds.
```

You need to support the following aggregate functions:

- `sum`
- `avg`
- `min`
- `max`
- `count`

## Selection Pushdown

When optimizations are turned on (see use cases for the command line interface you have to provide to control this), your system has to perform selection pushdown to optimize a given query. As a reminder, selection pushdown is an optimization that pushes `Selection` operators as far towards the leafs of the execution plan as possible. The point behind this is to reduce the size of intermediate result sets as soon as possible, to reduce the work later operators in the plan have to perform. To illustrate this, consider the following execution plan.

### Simple Case

```txt
-->Selection(condition=(professoren.Rang = "C3"))
---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
------>TableScan(professoren)
------>TableScan(vorlesungen)
```

The selection in this plan only depends on the `professoren` relation. We can therefore push it towards the `TableScan` on that relation. After selection pushdown, the execution plan should look like

```txt
-->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
---->Selection(condition=(professoren.Rang = "C3"))
------>TableScan(professoren)
---->TableScan(vorlesungen)
```

### General Case

To perform selection pushdown in the general case, we often have to split up complex selection conditions, because only a part of the condition can be pushed down. As an example, consider the following execution plan:

```txt
-->Selection(condition=((professoren.Rang = "C3") AND (vorlesungen.SWS = 3)))
---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
------>TableScan(professoren)
------>TableScan(vorlesungen)
```

Here, the selection condition references both `professoren` and `vorlesungen`. The condition needs to be split up at the `AND` connective to push the selection down:

```txt
-->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
---->Selection(condition=(professoren.Rang = "C3"))
------>TableScan(professoren)
---->Selection(condition=(vorlesungen.SWS = 3))
------>TableScan(vorlesungen)
```

### Required Capabilities

For your system, we expect you to correctly split up conditions on `AND` connectives, since doing so is trivial. Recall from the relational algebra:

```txt
sigma a and b (R) = sigma a (sigma b  (R))
```

As a suggestion, a relatively simple way to handle this is to

1. (recursively) split all selection conditions on `AND` connectives until no more splits are possible,
1. push down the resulting, simpler selections as far as possible, and
1. merge selection that directly follow each other in the execution plan back together.

To again illustrate this, consider:

```txt
-->Selection(condition=(((professoren.Rang = "C3") AND (professoren.Raum > "200")) AND (vorlesungen.SWS = 3)))
---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
------>TableScan(professoren)
------>TableScan(vorlesungen)
```

The first step of the aforementioned algorithm would transform this into

```txt
-->Selection(condition=(professoren.Rang = "C3"))
---->Selection(condition=(professoren.Raum > "200"))
------>Selection(condition=(vorlesungen.SWS = 3)
-------->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
---------->TableScan(professoren)
---------->TableScan(vorlesungen)
```

The second step would transform this further into

```txt
-->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
---->Selection(condition=(professoren.Rang = "C3"))
------>Selection(condition=(professoren.Raum > "200"))
-------->TableScan(professoren)
---->Selection(condition=(vorlesungen.SWS = 3))
------>TableScan(vorlesungen)
```

And finally, the last step would transform it into

```txt
-->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))
---->Selection(condition=((professoren.Rang = "C3") AND (professoren.Raum > "200")))
------>TableScan(professoren)
---->Selection(condition=(vorlesungen.SWS = 3))
------>TableScan(vorlesungen)
```

One further hint: Sometimes, it is necessary to duplicate selections and rename some column references to push down selections as far as possible. Think about how you would push down a selection over a `Union` operator, for example.

## Use Cases

After the completion of milestone 3, your system needs to at least support the following use cases.
It is assumed that the system was stated using the `data/kemper` dataset.
Further, ensure that the given queries have the same results when entered in the prompt and when loaded from a query file (`--query-file` command line option and `\execute` command in prompt).

### Optimizer Command Line Parameter

Your system has to provide a `--optimize` command line parameter.
This parameter is used to indicate whether optimization is turned on or off.
If that parameter is specified when starting the application, optimization is turned on and your system has to perform selection pushdown.
If it is not specified, optimization is turned off and the system should not optimize any query.
Further, it is necessary that optimizations can be turned on and off when the system is running (see next section below).

### Toggle Optimizer Command

Your system, in addition, has to provide a `\optimize` command to toggle the state of the optimizer (on and off). When executing this command while the optimizer is turned off, the optimizer will be turned on, and vice versa.

### Join Queries

<!-- queries/milestone2/join02.mql -->

Input query:

```txt
studenten natural join hoeren;
```

Expected output example:

```txt
+------------------+----------------+--------------------+---------------+
| studenten.MatrNr | studenten.Name | studenten.Semester | hoeren.VorlNr |
+------------------+----------------+--------------------+---------------+
|            26120 | Fichte         |                 10 |          5001 |
|            27550 | Schopenhauer   |                  6 |          5001 |
|            27550 | Schopenhauer   |                  6 |          4052 |
|            28106 | Carnap         |                  3 |          5041 |
|            28106 | Carnap         |                  3 |          5052 |
|            28106 | Carnap         |                  3 |          5216 |
|            28106 | Carnap         |                  3 |          5259 |
|            29120 | Theophrastos   |                  2 |          5001 |
|            29120 | Theophrastos   |                  2 |          5041 |
|            29120 | Theophrastos   |                  2 |          5049 |
+------------------+----------------+--------------------+---------------+
Executed query in 2.456e-04 seconds.
```

<!-- queries/milestone2/join03.mql -->

Input query:

```txt
studenten natural left join hoeren;
```

Expected output example:

```txt
+------------------+----------------+--------------------+---------------+
| studenten.MatrNr | studenten.Name | studenten.Semester | hoeren.VorlNr |
+------------------+----------------+--------------------+---------------+
|            24002 | Xenokrates     |                 18 |          NULL |
|            25403 | Jonas          |                 12 |          NULL |
|            26120 | Fichte         |                 10 |          5001 |
|            26830 | Aristoxenos    |                  8 |          NULL |
|            27550 | Schopenhauer   |                  6 |          5001 |
|            27550 | Schopenhauer   |                  6 |          4052 |
|            28106 | Carnap         |                  3 |          5041 |
|            28106 | Carnap         |                  3 |          5052 |
|            28106 | Carnap         |                  3 |          5216 |
|            28106 | Carnap         |                  3 |          5259 |
|            29120 | Theophrastos   |                  2 |          5001 |
|            29120 | Theophrastos   |                  2 |          5041 |
|            29120 | Theophrastos   |                  2 |          5049 |
|            29555 | Feuerbach      |                  2 |          NULL |
+------------------+----------------+--------------------+---------------+
Executed query in 2.575e-04 seconds.
```

<!-- queries/milestone2/join04.mql -->

Input query:

```txt
studenten left join studenten.MatrNr = hoeren.MatrNr hoeren;
```

Expected output example:

```txt
+------------------+----------------+--------------------+---------------+---------------+
| studenten.MatrNr | studenten.Name | studenten.Semester | hoeren.MatrNr | hoeren.VorlNr |
+------------------+----------------+--------------------+---------------+---------------+
|            24002 | Xenokrates     |                 18 |          NULL |          NULL |
|            25403 | Jonas          |                 12 |          NULL |          NULL |
|            26120 | Fichte         |                 10 |         26120 |          5001 |
|            26830 | Aristoxenos    |                  8 |          NULL |          NULL |
|            27550 | Schopenhauer   |                  6 |         27550 |          5001 |
|            27550 | Schopenhauer   |                  6 |         27550 |          4052 |
|            28106 | Carnap         |                  3 |         28106 |          5041 |
|            28106 | Carnap         |                  3 |         28106 |          5052 |
|            28106 | Carnap         |                  3 |         28106 |          5216 |
|            28106 | Carnap         |                  3 |         28106 |          5259 |
|            29120 | Theophrastos   |                  2 |         29120 |          5001 |
|            29120 | Theophrastos   |                  2 |         29120 |          5041 |
|            29120 | Theophrastos   |                  2 |         29120 |          5049 |
|            29555 | Feuerbach      |                  2 |          NULL |          NULL |
+------------------+----------------+--------------------+---------------+---------------+
Executed query in 2.649e-04 seconds.
```

### Aggregation Queries

<!-- queries/milestone2/gamma01.mql -->

Input query:

```txt
gamma Semester aggregate Anzahl as count(MatrNr) studenten;
```

Expected output example:

```txt
+--------------------+--------+
| studenten.Semester | Anzahl |
+--------------------+--------+
|                 18 |      1 |
|                 12 |      1 |
|                 10 |      1 |
|                  8 |      1 |
|                  6 |      1 |
|                  3 |      1 |
|                  2 |      2 |
+--------------------+--------+
Executed query in 3.695e-05 seconds.
```

<!-- queries/milestone2/gamma02.mql -->

Input query:

```txt
gamma aggregate AvgSemester as avg(Semester), MinSemester as min(Semester), MaxSemester as max(Semester) studenten;
```

Expected output example:

```txt
+-------------+-------------+-------------+
| AvgSemester | MinSemester | MaxSemester |
+-------------+-------------+-------------+
|       9.875 |           2 |          18 |
+-------------+-------------+-------------+
Executed query in 4.697e-05 seconds.
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
(pi studenten.MatrNr, Name, Semester, VorlNr sigma studenten.MatrNr = hoeren.MatrNr (studenten cross join hoeren)) except (studenten natural join hoeren);
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
((pi VorlNr sigma MatrNr = 29120 hoeren) intersect (pi VorlNr sigma gelesenVon = 2125 vorlesungen)) natural join vorlesungen;
```

Expected output example:

```txt
+---------------+-------------------+-----------------+------------------------+
| hoeren.VorlNr | vorlesungen.Titel | vorlesungen.SWS | vorlesungen.gelesenVon |
+---------------+-------------------+-----------------+------------------------+
|          5041 | Ethik             |               4 |                   2125 |
|          5049 | Maeeutik          |               2 |                   2125 |
+---------------+-------------------+-----------------+------------------------+
Executed query in 1.345e-04 seconds.
```

### Optimized Query Plans

Input query:

```txt
explain sigma Rang = "C3" and Raum > "200" and SWS = 3 (professoren join PersNr = gelesenVon vorlesungen);
```

Expected output example (optimizations turned off):

```txt
+----------------------------------------------------------------------------------------------------------------+
| Operator                                                                                                       |
+----------------------------------------------------------------------------------------------------------------+
| -->Selection(condition=(((professoren.Rang = "C3") AND (professoren.Raum > "200")) AND (vorlesungen.SWS = 3))) |
| ---->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon))            |
| ------>TableScan(professoren)                                                                                  |
| ------>TableScan(vorlesungen)                                                                                  |
+----------------------------------------------------------------------------------------------------------------+
```

Expected output example (optimizations turned on):

```txt
+---------------------------------------------------------------------------------------------------+
| Operator                                                                                          |
+---------------------------------------------------------------------------------------------------+
| -->NestedLoopsJoin(inner, natural=False, condition=(professoren.PersNr = vorlesungen.gelesenVon)) |
| ---->Selection(condition=((professoren.Rang = "C3") AND (professoren.Raum > "200")))              |
| ------>TableScan(professoren)                                                                     |
| ---->Selection(condition=(vorlesungen.SWS = 3))                                                   |
| ------>TableScan(vorlesungen)                                                                     |
+---------------------------------------------------------------------------------------------------+
```

Input query:

```txt
explain sigma Name > "K" (pi Name professoren union pi Name studenten);
```

Expected output example (optimizations turned off):

```txt
+----------------------------------------------------------------+
| Operator                                                       |
+----------------------------------------------------------------+
| -->Selection(condition=(professoren.Name > "K"))               |
| ---->Union()                                                   |
| ------>Projection(columns=[professoren.Name=professoren.Name]) |
| -------->TableScan(professoren)                                |
| ------>Projection(columns=[studenten.Name=studenten.Name])     |
| -------->TableScan(studenten)                                  |
+----------------------------------------------------------------+
```

Hint: Recall that our query language retains the names of the left input for set operations. The `professoren.Name` in the selection here does _not_ only refer to `professoren`. It also refers to the column in the same ordinal position of the right input.

Expected output example (optimizations turned on):

```txt
+--------------------------------------------------------------+
| Operator                                                     |
+--------------------------------------------------------------+
| -->Union()                                                   |
| ---->Projection(columns=[professoren.Name=professoren.Name]) |
| ------>Selection(condition=(professoren.Name > "K"))         |
| -------->TableScan(professoren)                              |
| ---->Projection(columns=[studenten.Name=studenten.Name])     |
| ------>Selection(condition=(studenten.Name > "K"))           |
| -------->TableScan(studenten)                                |
+--------------------------------------------------------------+
```

### Optimized Execution Time

Ensure that an enabled optimizer reduces the excecution time.

Input query:

```txt
sigma studenten.Name = "Theophrastos" and vorlesungen.SWS = 4 and professoren.Name = "Sokrates" and assistenten.Name = "Platon" (studenten cross join vorlesungen cross join professoren cross join assistenten cross join pruefen cross join voraussetzen cross join hoeren);
```

- Expected behavior (optimizations turned off): the query needs multiple seconds to complete.
- Expected behavior (optimizations turned on): the query finishes in under a second.

## Submission

- All needed files present in your repository
- All instructions (how to compile, test and run) in the README.

## Grading

For this milestone, your can achieve a maximum of ten points. The points will be allocated as follows:

- Eight points based on how well your systems supports the provided uses cases. To receive all eight points, your system needs to support everything described in the given use cases, and adequately handle error cases (i.e., be stable, don't crash etc.).
- Two points for the quality of your code. Try to develop clean and maintainable code - you will thank yourselves for it later in the project.

## Competition

In addition to the normal milestone, we will hold a small competition this time. Each of your systems will execute the following query:

```txt
sigma Name > "K" ((pi studenten.Name (sigma studenten.Name = "Theophrastos" and vorlesungen.SWS = 4 and professoren.Name = "Sokrates" and assistenten.Name = "Platon" (studenten cross join vorlesungen cross join professoren cross join assistenten cross join pruefen cross join voraussetzen cross join hoeren))) union (pi Name professoren));
```

We will give extra points based on the following:

1. The team whose system executes this query in the shortest time (with turned on optimization) will receive 1 bonus point.
1. The team whose system achieves the highest relative speedup between unoptimized and optimized execution will receive 1 bonus point.
1. Every team whose system executes the query (with turned on optimization) faster than our exemplary solution will recause 1 bonus point.

All performance measurements will be done by us on a single computer.
