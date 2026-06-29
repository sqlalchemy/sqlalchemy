.. change::
    :tags: bug, sqlite

    Reworked the regular expression that detects inline ``UNIQUE`` column
    constraints during SQLite ``CREATE TABLE`` reflection so that the
    whitespace separating a column's type from a following clause is matched
    unambiguously.  The previous pattern had three overlapping quantifiers
    that could each consume a space character, so a column definition
    carrying a long run of whitespace in the stored schema made
    :meth:`_reflection.Inspector.get_unique_constraints` spend cubic time
    backtracking before returning.  Fix courtesy of Javid Khan.
