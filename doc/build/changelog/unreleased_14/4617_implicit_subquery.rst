.. change::
    :tags: change, sql
    :tickets: 4617

    Added new method :meth:`.SelectBase.subquery`, which creates a subquery
    that is essentially the same thing as what calling
    :meth:`.FromClause.alias` has always done, e.g. creates a named subquery.
    This method is intended to roughly mirror the same role as that of
    :meth:`.Query.subquery`.   The :meth:`.SelectBase.alias` method is
    being kept for the time being as essentially the same function as that
    of :meth:`.SelectBase.subquery`.

.. change::
    :tags: change, orm
    :tickets: 4617

    The ORM will now warn when asked to coerce a :func:`.select` construct into
    a subquery implicitly.  This occurs within places such as the
    :meth:`.Query.select_entity_from` and  :meth:`.Query.select_from` methods
    as well as within the :func:`.with_polymorphic` function.  When a
    :class:`.SelectBase` (which is what's produced by :func:`.select`) or
    :class:`.Query` object is passed directly to these functions and others,
    the ORM is typically coercing them to be a subquery by calling the
    :meth:`.SelectBase.alias` method automatically (which is now superceded by
    the :meth:`.SelectBase.subquery method).  The historical reason is that
    most databases other than SQLite don't allow a SELECT of a SELECT without
    the inner SELECT being a named subuqery in any case; going forward,
    SQLAlchemy Core is moving towards no longer considering a SELECT statement
    that isn't inside a subquery to be a "FROM" clause, that is, an object that
    can be selected from, in the first place, as part of a larger change to
    unify the interfaces for :func:`.select` and :meth:`.Query`.  The change is
    intended to encourage code to make explicit those places where these
    subqueries have normally been implicitly created.
