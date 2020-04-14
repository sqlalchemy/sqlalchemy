.. change::
    :tags: sql, change
    :tickets: 4617

    The "clause coercion" system, which is SQLAlchemy Core's system of receiving
    arguments and resolving them into :class:`_expression.ClauseElement` structures in order
    to build up SQL expression objects, has been rewritten from a series of
    ad-hoc functions to a fully consistent class-based system.   This change
    is internal and should have no impact on end users other than more specific
    error messages when the wrong kind of argument is passed to an expression
    object, however the change is part of a larger set of changes involving
    the role and behavior of :func:`_expression.select` objects.

