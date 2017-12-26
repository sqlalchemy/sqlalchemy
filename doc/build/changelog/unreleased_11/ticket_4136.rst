.. change::
    :tags: bug, mysql
    :tickets: 4136
    :versions: 1.2.0

    Fixed bug where the MySQL "concat" and "match" operators failed to
    propagate kwargs to the left and right expressions, causing compiler
    options such as "literal_binds" to fail.