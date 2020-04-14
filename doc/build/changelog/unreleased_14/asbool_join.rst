.. change::
    :tags: usecase, sql

    The :func:`.true` and :func:`.false` operators may now be applied as the
    "onclause" of a :func:`_expression.join` on a backend that does not support
    "native boolean" expressions, e.g. Oracle or SQL Server, and the expression
    will render as "1=1" for true and "1=0" false.  This is the behavior that
    was introduced many years ago in :ticket:`2804` for and/or expressions.
