# Task 8

- **Submission Due**: 2022-05-30

## Description

For this task, you will look at the cost model of PostgreSQL. Your presentation should have a duration of ten minutes. You can check the following sources for information on the topic:

- the PostgreSQL documentation
- the PostgreSQL Wiki
- a search engine of your choice
- [costsize.c](https://github.com/postgres/postgres/blob/master/src/backend/optimizer/path/costsize.c)

Focus on the following aspects:

- What parameters does PostgreSQL provide to influence cost estimation? (cf. _Planner Cost Constants_)
- How does the estimation for a sequential scan work?
- How does the estimation for an index scan work?

Note: Don't be confused by the terminology here. What PostgreSQL calls an _index scan_ is not what we considered an index scan in the lectures. It is more akin to what we called an _index seek_, with the difference that it doesn't actually fetch tuples, but only tuple identifiers.

## Submission

- Publish your slides (PDF-Format) supporting your ten minutes presentation in your repository.

## Grading

For this task you can earn up to four points, where zero means "nothing submitted" and four means "excellent".
