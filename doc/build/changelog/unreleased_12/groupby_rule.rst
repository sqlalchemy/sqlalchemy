.. change::
    :tags: bug, tests

    Added a new exclusion rule group_by_complex_expression
    which disables tests that use "GROUP BY <expr>", which seems
    to be not viable for at least two third party dialects.
