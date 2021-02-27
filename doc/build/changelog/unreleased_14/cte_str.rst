.. change::
    :tags: change, sql

    Altered the compilation for the :class:`.CTE` construct so that a string is
    returned representing the inner SELECT statement if the :class:`.CTE` is
    stringified directly, outside of the context of an enclosing SELECT; This
    is the same behavior of :meth:`_FromClause.alias` and
    :meth:`_SelectStatement.subquery`. Previously, a blank string would be
    returned as the CTE is normally placed above a SELECT after that SELECT has
    been generated, which is generally misleading when debugging.

