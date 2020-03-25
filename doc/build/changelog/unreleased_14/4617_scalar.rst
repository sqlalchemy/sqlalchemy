.. change::
    :tags: change, sql
    :tickets: 4617

    The :meth:`.SelectBase.as_scalar` and :meth:`.Query.as_scalar` methods have
    been renamed to :meth:`.SelectBase.scalar_subquery` and
    :meth:`.Query.scalar_subquery`, respectively.  The old names continue to
    exist within 1.4 series with a deprecation warning.  In addition, the
    implicit coercion of :class:`.SelectBase`, :class:`.Alias`, and other
    SELECT oriented objects into scalar subqueries when evaluated in a column
    context is also deprecated, and emits a warning that the
    :meth:`.SelectBase.scalar_subquery` method should be called explicitly.
    This warning will in a later major release become an error, however the
    message will always be clear when :meth:`.SelectBase.scalar_subquery` needs
    to be invoked.   The latter part of the change is for clarity and to reduce
    the implicit decisionmaking by the query coercion system.   The
    :meth:`.Subquery.as_scalar` method, which was previously
    ``Alias.as_scalar``, is also deprecated; ``.scalar_subquery()`` should be
    invoked directly from ` :func:`~.sql.expression.select` object or :class:`.Query` object.

    This change is part of the larger change to convert :func:`~.sql.expression.select` objects
    to no longer be directly part of the "from clause" class hierarchy, which
    also includes an overhaul of the clause coercion system.

