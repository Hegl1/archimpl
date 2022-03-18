"""This module contains the grammar of the parsers."""
from parsimonious.grammar import Grammar

# flake8: noqa

grammar = Grammar(r"""
    command         =
        explain_command
        / query

    explain_command = ~"explain"i mandatory_ws query

    query           = set_factor set_operation*
    set_operation   = set_operator mandatory_ws set_factor

    set_factor           = join_factor join*
    join            =
        (natural_join_operator mandatory_ws join_factor)
        / (cross_join_operator mandatory_ws join_factor)
        / (join_operator mandatory_ws expression join_factor)
    join_factor     =
        projection
        / selection
        / ordering
        / grouping
        / paren_query
        / relation_reference
    paren_query     =
        (from_kw mandatory_ws)? "(" ws query ")" ws

    set_operator =
        ~"union"i
        / ~"intersect"i
        / ~"except"i

    join_operator =
        ~"left join"i
        / ~"join"i
    natural_join_operator =
        ~"natural left join"i
        / ~"natural join"i
    cross_join_operator =
        ~"cross join"i

    projection          =
        (projection_kw mandatory_ws distinct_kw mandatory_ws column_list join_factor)
        / (projection_kw mandatory_ws column_list join_factor)
    selection           = selection_kw mandatory_ws expression join_factor
    grouping            =
        (grouping_kw mandatory_ws column_list aggregate_kw mandatory_ws aggregate_list join_factor)
        / (grouping_kw mandatory_ws aggregate_kw mandatory_ws aggregate_list join_factor)
    ordering            = ordering_kw mandatory_ws simple_column_list join_factor
    relation_reference  =
        ((from_kw mandatory_ws)? table_name mandatory_ws as_kw mandatory_ws name ws)
        / ((from_kw mandatory_ws)? table_name ws)

    column_list    =
        (column_reference separator column_list)
        / column_reference
    column_reference =
        (name "as" mandatory_ws expression)
        / (name)

    simple_column_list   =
        (expression separator simple_column_list)
        / (expression)

    aggregate_list      =
        (aggregate_column separator aggregate_list)
        / aggregate_column
    aggregate_column    = name as_kw mandatory_ws aggregate_function ws "(" ws expression ")" ws
    aggregate_function  =
        ~"sum"i
        / ~"avg"i
        / ~"min"i
        / ~"max"i
        / ~"count"i

    expression           = conjunctive_term disjunctive*
    disjunctive          = or_kw mandatory_ws conjunctive_term
    conjunctive_term     = comparative_term conjunctive*
    conjunctive          = and_kw mandatory_ws comparative_term
    comparative_term     = additive_term comparative*
    comparative          = comparison_operator ws additive_term
    comparison_operator  = "=" / "!="  / "<=" / "<" / ">=" / ">"
    additive_term        = multiplicative_term additive*
    additive             = addition_operator ws multiplicative_term
    addition_operator    = "+" / "-"
    multiplicative_term  = term multiplicative*
    multiplicative       = multiplication_operator ws term
    multiplication_operator = "*" / "/"
    term                 = parens / literal / column_name
    parens               = "(" ws expression ")" ws

    separator       = "," ws

    int_literal     = ~"(-)?[0-9]+" ws
    float_literal   = ~r"(-)?[0-9]+\.([0-9]*)?" ws
    varchar_literal = ~"\"[^\"]*\"" ws
    null_literal    = ~"null"i ws
    literal         = float_literal / int_literal / varchar_literal / null_literal

    table_name      = ~"[\\#a-zA-Z][\\#a-zA-Z0-9]*"
    column_name     = ~"[\\#a-zA-Z_][\\#a-zA-Z0-9_]*(\\.[\\#a-zA-Z_][\\#a-zA-Z0-9_]*)?" ws
    name            = ~"[\\#a-zA-Z_][\\#a-zA-Z0-9_]*(\\.[\\#a-zA-Z_][\\#a-zA-Z0-9_]*)?" ws

    projection_kw   = ~"pi"i / ~"select"i
    distinct_kw     = ~"distinct"i
    selection_kw    = ~"sigma"i / ~"where"i
    grouping_kw     = ~"gamma"i / ~"group by"i
    aggregate_kw    = ~"aggregate"i
    ordering_kw     = ~"tau"i / ~"order by"i
    as_kw           = ~"as"i
    or_kw           = ~"or"i
    and_kw          = ~"and"i
    from_kw         = ~"from"i

    ws              = ~"\\s*"
    mandatory_ws    = ~"\\s+"
    """)
