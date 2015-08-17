==============================
What's New in SQLAlchemy 1.1?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.0,
    at the moment the current release series of SQLAlchemy,
    and SQLAlchemy version 1.1, which is the current development
    series of SQLAlchemy.

    As the 1.1 series is under development, issues that are targeted
    at this series can be seen under the
    `1.1 milestone <https://bitbucket.org/zzzeek/sqlalchemy/issues?milestone=1.1>`_.
    Please note that the set of issues within the milestone is not fixed;
    some issues may be moved to later milestones in order to allow
    for a timely release.

    Document last updated: July 24, 2015.

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.1,
and also documents changes which affect users migrating
their applications from the 1.0 series of SQLAlchemy to 1.1.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.

Platform / Installer Changes
============================

Setuptools is now required for install
--------------------------------------

SQLAlchemy's ``setup.py`` file has for many years supported operation
both with Setuptools installed and without; supporting a "fallback" mode
that uses straight Distutils.  As a Setuptools-less Python environment is
now unheard of, and in order to support the featureset of Setuptools
more fully, in particular to support py.test's integration with it,
``setup.py`` now depends on Setuptools fully.

.. seealso::

	:ref:`installation`

:ticket:`3489`

Enabling / Disabling C Extension builds is only via environment variable
------------------------------------------------------------------------

The C Extensions build by default during install as long as it is possible.
To disable C extension builds, the ``DISABLE_SQLALCHEMY_CEXT`` environment
variable was made available as of SQLAlchemy 0.8.6 / 0.9.4.  The previous
approach of using the ``--without-cextensions`` argument has been removed,
as it relies on deprecated features of setuptools.

.. seealso::

	:ref:`c_extensions`

:ticket:`3500`


New Features and Improvements - ORM
===================================

.. _change_3499:

Changes regarding "unhashable" types
------------------------------------

The :class:`.Query` object has a well-known behavior of "deduping"
returned rows that contain at least one ORM-mapped entity (e.g., a
full mapped object, as opposed to individual column values). The
primary purpose of this is so that the handling of entities works
smoothly in conjunction with the identity map, including to
accommodate for the duplicate entities normally represented within
joined eager loading, as well as when joins are used for the purposes
of filtering on additional columns.

This deduplication relies upon the hashability of the elements within
the row.  With the introduction of Postgresql's special types like
:class:`.postgresql.ARRAY`, :class:`.postgresql.HSTORE` and
:class:`.postgresql.JSON`, the experience of types within rows being
unhashable and encountering problems here is more prevalent than
it was previously.

In fact, SQLAlchemy has since version 0.8 included a flag on datatypes that
are noted as "unhashable", however this flag was not used consistently
on built in types.  As described in :ref:`change_3499_postgresql`, this
flag is now set consistently for all of Postgresql's "structural" types.

The "unhashable" flag is also set on the :class:`.NullType` type,
as :class:`.NullType` is used to refer to any expression of unknown
type.

Additionally, the treatment of a so-called "unhashable" type is slightly
different than its been in previous releases; internally we are using
the ``id()`` function to get a "hash value" from these structures, just
as we would any ordinary mapped object.   This replaces the previous
approach which applied a counter to the object.

:ticket:`3499`

New Features and Improvements - Core
====================================


.. _change_2528:

A UNION or similar of SELECTs with LIMIT/OFFSET/ORDER BY now parenthesizes the embedded selects
-----------------------------------------------------------------------------------------------

An issue that, like others, was long driven by SQLite's lack of capabilities
has now been enhanced to work on all supporting backends.   We refer to a query that
is a UNION of SELECT statements that themselves contain row-limiting or ordering
features which include LIMIT, OFFSET, and/or ORDER BY::

    (SELECT x FROM table1 ORDER BY y LIMIT 1) UNION
    (SELECT x FROM table2 ORDER BY y LIMIT 2)

The above query requires parenthesis within each sub-select in order to
group the sub-results correctly.  Production of the above statement in
SQLAlchemy Core looks like::

    stmt1 = select([table1.c.x]).order_by(table1.c.y).limit(1)
    stmt2 = select([table1.c.x]).order_by(table2.c.y).limit(2)

    stmt = union(stmt1, stmt2)

Previously, the above construct would not produce parenthesization for the
inner SELECT statements, producing a query that fails on all backends.

The above formats will **continue to fail on SQLite**; additionally, the format
that includes ORDER BY but no LIMIT/SELECT will **continue to fail on Oracle**.
This is not a backwards-incompatible change, because the queries fail without
the parentheses as well; with the fix, the queries at least work on all other
databases.

In all cases, in order to produce a UNION of limited SELECT statements that
also works on SQLite and in all cases on Oracle, the
subqueries must be a SELECT of an ALIAS::

    stmt1 = select([table1.c.x]).order_by(table1.c.y).limit(1).alias().select()
    stmt2 = select([table2.c.x]).order_by(table2.c.y).limit(2).alias().select()

    stmt = union(stmt1, stmt2)

This workaround works on all SQLAlchemy versions.  In the ORM, it looks like::

    stmt1 = session.query(Model1).order_by(Model1.y).limit(1).subquery().select()
    stmt2 = session.query(Model2).order_by(Model2.y).limit(1).subquery().select()

    stmt = session.query(Model1).from_statement(stmt1.union(stmt2))

The behavior here has many parallels to the "join rewriting" behavior
introduced in SQLAlchemy 0.9 in :ref:`feature_joins_09`; however in this case
we have opted not to add new rewriting behavior to accommodate this
case for SQLite.
The existing rewriting behavior is very complicated already, and the case of
UNIONs with parenthesized SELECT statements is much less common than the
"right-nested-join" use case of that feature.

:ticket:`2528`

Key Behavioral Changes - ORM
============================


Key Behavioral Changes - Core
=============================


Dialect Improvements and Changes - Postgresql
=============================================

.. _change_3499_postgresql:

ARRAY and JSON types now correctly specify "unhashable"
-------------------------------------------------------

As described in :ref:`change_3499`, the ORM relies upon being able to
produce a hash function for column values when a query's selected entities
mixes full ORM entities with column expressions.   The ``hashable=False``
flag is now correctly set on all of PG's "data structure" types, including
:class:`.ARRAY` and :class:`.JSON`.  The :class:`.JSONB` and :class:`.HSTORE`
types already included this flag.  For :class:`.ARRAY`,
this is conditional based on the :paramref:`.postgresql.ARRAY.as_tuple`
flag, however it should no longer be necessary to set this flag
in order to have an array value present in a composed ORM row.

.. seealso::

    :ref:`change_3499`

    :ref:`change_3503`

:ticket:`3499`

.. _change_3503:

Correct SQL Types are Established from Indexed Access of ARRAY, JSON, HSTORE
-----------------------------------------------------------------------------

For all three of :class:`~.postgresql.ARRAY`, :class:`~.postgresql.JSON` and :class:`.HSTORE`,
the SQL type assigned to the expression returned by indexed access, e.g.
``col[someindex]``, should be correct in all cases.

This includes:

* The SQL type assigned to indexed access of an :class:`~.postgresql.ARRAY` takes into
  account the number of dimensions configured.   An :class:`~.postgresql.ARRAY` with three
  dimensions will return a SQL expression with a type of :class:`~.postgresql.ARRAY` of
  one less dimension.  Given a column with type ``ARRAY(Integer, dimensions=3)``,
  we can now perform this expression::

      int_expr = col[5][6][7]   # returns an Integer expression object

  Previously, the indexed access to ``col[5]`` would return an expression of
  type :class:`.Integer` where we could no longer perform indexed access
  for the remaining dimensions, unless we used :func:`.cast` or :func:`.type_coerce`.

* The :class:`~.postgresql.JSON` and :class:`~.postgresql.JSONB` types now mirror what Postgresql
  itself does for indexed access.  This means that all indexed access for
  a :class:`~.postgresql.JSON` or :class:`~.postgresql.JSONB` type returns an expression that itself
  is *always* :class:`~.postgresql.JSON` or :class:`~.postgresql.JSONB` itself, unless the
  :attr:`~.postgresql.JSON.Comparator.astext` modifier is used.   This means that whether
  the indexed access of the JSON structure ultimately refers to a string,
  list, number, or other JSON structure, Postgresql always considers it
  to be JSON itself unless it is explicitly cast differently.   Like
  the :class:`~.postgresql.ARRAY` type, this means that it is now straightforward
  to produce JSON expressions with multiple levels of indexed access::

    json_expr = json_col['key1']['attr1'][5]

* The "textual" type that is returned by indexed access of :class:`.HSTORE`
  as well as the "textual" type that is returned by indexed access of
  :class:`~.postgresql.JSON` and :class:`~.postgresql.JSONB` in conjunction with the
  :attr:`~.postgresql.JSON.Comparator.astext` modifier is now configurable; it defaults
  to :class:`.Text` in both cases but can be set to a user-defined
  type using the :paramref:`.postgresql.JSON.astext_type` or
  :paramref:`.postgresql.HSTORE.text_type` parameters.

.. seealso::

  :ref:`change_3503_cast`

:ticket:`3499`
:ticket:`3487`

.. _change_3503_cast:

The JSON cast() operation now requires ``.astext`` is called explicitly
------------------------------------------------------------------------

As part of the changes in :ref:`change_3503`, the workings of the
:meth:`.ColumnElement.cast` operator on :class:`.postgresql.JSON` and
:class:`.postgresql.JSONB` no longer implictly invoke the
:attr:`.JSON.Comparator.astext` modifier; Postgresql's JSON/JSONB types
support CAST operations to each other without the "astext" aspect.

This means that in most cases, an application that was doing this::

    expr = json_col['somekey'].cast(Integer)

Will now need to change to this::

    expr = json_col['somekey'].astext.cast(Integer)



.. _change_3514:

Postgresql JSON "null" is inserted as expected with ORM operations, regardless of column default present
-----------------------------------------------------------------------------------------------------------

The :class:`.JSON` type has a flag :paramref:`.JSON.none_as_null` which
when set to True indicates that the Python value ``None`` should translate
into a SQL NULL rather than a JSON NULL value.  This flag defaults to False,
which means that the column should *never* insert SQL NULL or fall back
to a default unless the :func:`.null` constant were used.  However, this would
fail in the ORM under two circumstances; one is when the column also contained
a default or server_default value, a positive value of ``None`` on the mapped
attribute would still result in the column-level default being triggered,
replacing the ``None`` value::

    obj = MyObject(json_value=None)
    session.add(obj)
    session.commit()   # would fire off default / server_default, not encode "'none'"

The other is when the :meth:`.Session.bulk_insert_mappings`
method were used, ``None`` would be ignored in all cases::

    session.bulk_insert_mappings(
        MyObject,
        [{"json_value": None}])  # would insert SQL NULL and/or trigger defaults

The :class:`.JSON` type now adds a new flag :attr:`.TypeEngine.evaluates_none`
indicating that ``None`` should not be ignored here; it is configured
automatically based on the value of :paramref:`.JSON.none_as_null`.
Thanks to :ticket:`3061`, we can differentiate when the value ``None`` is actively
set by the user versus when it was never set at all.

If the attribute is not set at all, then column level defaults *will*
fire off and/or SQL NULL will be inserted as expected, as was the behavior
previously.  Below, the two variants are illustrated::

    obj = MyObject(json_value=None)
    session.add(obj)
    session.commit()   # *will not* fire off column defaults, will insert JSON 'null'

    obj = MyObject()
    session.add(obj)
    session.commit()   # *will* fire off column defaults, and/or insert SQL NULL

:ticket:`3514`

.. seealso::

  :ref:`change_3514_jsonnull`

.. _change_3514_jsonnull:

New JSON.NULL Constant Added
----------------------------

To ensure that an application can always have full control at the value level
of whether a :class:`.postgresql.JSON` or :class:`.postgresql.JSONB` column
should receive a SQL NULL or JSON ``"null"`` value, the constant
:attr:`.postgresql.JSON.NULL` has been added, which in conjunction with
:func:`.null` can be used to determine fully between SQL NULL and
JSON ``"null"``, regardless of what :paramref:`.JSON.none_as_null` is set
to::

    from sqlalchemy import null
    from sqlalchemy.dialects.postgresql import JSON

    obj1 = MyObject(json_value=null())  # will *always* insert SQL NULL
    obj2 = MyObject(json_value=JSON.NULL)  # will *always* insert JSON string "null"

    session.add_all([obj1, obj2])
    session.commit()

.. seealso::

    :ref:`change_3514`

:ticket:`3514`

Dialect Improvements and Changes - MySQL
=============================================


Dialect Improvements and Changes - SQLite
=============================================


Dialect Improvements and Changes - SQL Server
=============================================

.. _change_3504:

String / varlength types no longer represent "max" explicitly on reflection
---------------------------------------------------------------------------

When reflecting a type such as :class:`.String`, :class:`.Text`, etc.
which includes a length, an "un-lengthed" type under SQL Server would
copy the "length" parameter as the value ``"max"``::

    >>> from sqlalchemy import create_engine, inspect
    >>> engine = create_engine('mssql+pyodbc://scott:tiger@ms_2008', echo=True)
    >>> engine.execute("create table s (x varchar(max), y varbinary(max))")
    >>> insp = inspect(engine)
    >>> for col in insp.get_columns("s"):
    ...     print col['type'].__class__, col['type'].length
    ...
    <class 'sqlalchemy.sql.sqltypes.VARCHAR'> max
    <class 'sqlalchemy.dialects.mssql.base.VARBINARY'> max

The "length" parameter in the base types is expected to be an integer value
or None only; None indicates unbounded length which the SQL Server dialect
interprets as "max".   The fix then is so that these lengths come
out as None, so that the type objects work in non-SQL Server contexts::

    >>> for col in insp.get_columns("s"):
    ...     print col['type'].__class__, col['type'].length
    ...
    <class 'sqlalchemy.sql.sqltypes.VARCHAR'> None
    <class 'sqlalchemy.dialects.mssql.base.VARBINARY'> None

Applications which may have been relying on a direct comparison of the "length"
value to the string "max" should consider the value of ``None`` to mean
the same thing.

:ticket:`3504`

Dialect Improvements and Changes - Oracle
=============================================
