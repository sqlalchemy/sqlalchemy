.. change::
    :tags: oracle, change

    The LIMIT / OFFSET scheme used in Oracle now makes use of named subqueries
    rather than unnamed subqueries when it transparently rewrites a SELECT
    statement to one that uses a subquery that includes ROWNUM.  The change is
    part of a larger change where unnamed subqueries are no longer directly
    supported by Core, as well as to modernize the internal use of the select()
    construct within the Oracle dialect.

