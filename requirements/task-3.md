# Task 3

- **Submission Due**: 2022-04-04

## Description

For this week's task, you will look into how SQL Server handles record and block storage.

_Note: The SQL Server documentation uses the term "page" to refer to what we have so far called "block". Traditionally, there is a distinction between pages, which reside in memory, and blocks, which reside on disk. SQL Server uses the term "page" to refer to both. Don't let yourself be confused by this._

Your presentation, which as always should have a duration of about 10 minutes, should answer at least the following questions:

- How does the layout of a normal SQL Server data page look? How are offset tables used in SQL Server, and how does their layout differ from what we covered in the lecture? Recommended Source: [1]
- How does the record format of SQL Server look? Recommended source: [2]
- How does the record format of SQL Server handle variable-length fields? Recommended source: [2] _(Note: SQL Server implements varchar(n) as a true variable-length datatype of at most n+2 bytes, not as a fixed-length type as we discussed in the lecture. Again, don't get confused by this. The exact definitions vary a lot between different concrete database systems.)_
- How does SQL Server handle rows that are too large to fit on a single page? Recommended Source: [1]

To answer those questions, read at least the following webpages:

- [1] <https://docs.microsoft.com/en-us/sql/relational-databases/pages-and-extents-architecture-guide?view=sql-server-ver15>
- [2] <https://www.red-gate.com/simple-talk/databases/sql-server/database-administration-sql-server/sql-server-storage-internals-101/> (section "Records")

## Submission

- Publish your slides (PDF-Format) supporting your ten minutes presentation in your repository.

## Grading

For this task you can earn up to four points, where zero means "nothing submitted" and four means "excellent".
