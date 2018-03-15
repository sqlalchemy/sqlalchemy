=============================
What's New in SQLAlchemy 1.3?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.2
    and SQLAlchemy version 1.3.

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.3
and also documents changes which affect users migrating
their applications from the 1.2 series of SQLAlchemy to 1.3.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.

New Features and Improvements - ORM
===================================

Key Behavioral Changes - ORM
=============================

.. _change_4246:

FOR UPDATE clause is rendered within the joined eager load subquery as well as outside
--------------------------------------------------------------------------------------

This change applies specifically to the use of the :func:`.joinedload` loading
strategy in conjunction with a row limited query, e.g. using :meth:`.Query.first`
or :meth:`.Query.limit`, as well as with use of the :class:`.Query.with_for_update` method.

Given a query as::

    session.query(A).options(joinedload(A.b)).limit(5)

The :class:`.Query` object renders a SELECT of the following form when joined
eager loading is combined with LIMIT::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id

This is so that the limit of rows takes place for the primary entity without
affecting the joined eager load of related items.   When the above query is
combined with "SELECT..FOR UPDATE", the behavior has been this::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id FOR UPDATE

However, MySQL due to https://bugs.mysql.com/bug.php?id=90693 does not lock
the rows inside the subquery, unlike that of Postgresql and other databases.
So the above query now renders as::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5 FOR UPDATE
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id FOR UPDATE

On the Oracle dialect, the inner "FOR UPDATE" is not rendered as Oracle does
not support this syntax and the dialect skips any "FOR UPDATE" that is against
a subquery; it isn't necessary in any case since Oracle, like Postgresql,
correctly locks all elements of the returned row.

When using the :paramref:`.Query.with_for_update.of` modifier, typically on
Postgresql, the outer "FOR UPDATE" is omitted, and the OF is now rendered
on the inside; previously, the OF target would not be converted to accommodate
for the subquery correctly.  So
given::

    session.query(A).options(joinedload(A.b)).with_for_update(of=A).limit(5)

The query would now render as::

    SELECT subq.a_id, subq.a_data, b_alias.id, b_alias.data FROM (
        SELECT a.id AS a_id, a.data AS a_data FROM a LIMIT 5 FOR UPDATE OF a
    ) AS subq LEFT OUTER JOIN b ON subq.a_id=b.a_id

The above form should be helpful on Postgresql additionally since Postgresql
will not allow the FOR UPDATE clause to be rendered after the LEFT OUTER JOIN
target.

Overall, FOR UPDATE remains highly specific to the target database in use
and can't easily be generalized for more complex queries.

:ticket:`4246`

New Features and Improvements - Core
====================================

.. _change_3831:

Binary comparison interpretation for SQL functions
--------------------------------------------------

This enhancement is implemented at the Core level, however is applicable
primarily to the ORM.

A SQL function that compares two elements can now be used as a "comparison"
object, suitable for usage in an ORM :func:`.relationship`, by first
creating the function as usual using the :data:`.func` factory, then
when the function is complete calling upon the :meth:`.FunctionElement.as_comparison`
modifier to produce a :class:`.BinaryExpression` that has a "left" and a "right"
side::

    class Venue(Base):
        __tablename__ = 'venue'
        id = Column(Integer, primary_key=True)
        name = Column(String)

        descendants = relationship(
            "Venue",
            primaryjoin=func.instr(
                remote(foreign(name)), name + "/"
            ).as_comparison(1, 2) == 1,
            viewonly=True,
            order_by=name
        )

Above, the :paramref:`.relationship.primaryjoin` of the "descendants" relationship
will produce a "left" and a "right" expression based on the first and second
arguments passed to ``instr()``.   This allows features like the ORM
lazyload to produce SQL like::

    SELECT venue.id AS venue_id, venue.name AS venue_name
    FROM venue
    WHERE instr(venue.name, (? || ?)) = ? ORDER BY venue.name
    ('parent1', '/', 1)

and a joinedload, such as::

    v1 = s.query(Venue).filter_by(name="parent1").options(
        joinedload(Venue.descendants)).one()

to work as::

    SELECT venue.id AS venue_id, venue.name AS venue_name,
      venue_1.id AS venue_1_id, venue_1.name AS venue_1_name
    FROM venue LEFT OUTER JOIN venue AS venue_1
      ON instr(venue_1.name, (venue.name || ?)) = ?
    WHERE venue.name = ? ORDER BY venue_1.name
    ('/', 1, 'parent1')

This feature is expected to help with situations such as making use of
geometric functions in relationship join conditions, or any case where
the ON clause of the SQL join is expressed in terms of a SQL function.

:ticket:`3831`


Key Behavioral Changes - Core
=============================

Dialect Improvements and Changes - PostgreSQL
=============================================

Dialect Improvements and Changes - MySQL
=============================================

.. _change_mysql_ping:

Protocol-level ping now used for pre-ping
------------------------------------------

The MySQL dialects including mysqlclient, python-mysql, PyMySQL and
mysql-connector-python now use the ``connection.ping()`` method for the
pool pre-ping feature, described at :ref:`pool_disconnects_pessimistic`.
This is a much more lightweight ping than the previous method of emitting
"SELECT 1" on the connection.


Dialect Improvements and Changes - SQLite
=============================================

.. _change_3850:

Support for SQLite JSON Added
-----------------------------

A new datatype :class:`.sqlite.JSON` is added which implements SQLite's json
member access functions on behalf of the :class:`.types.JSON`
base datatype.  The SQLite ``JSON_EXTRACT`` and ``JSON_QUOTE`` functions
are used by the implementation to provide basic JSON support.

Note that the name of the datatype itself as rendered in the database is
the name "JSON".   This will create a SQLite datatype with "numeric" affinity,
which normally should not be an issue except in the case of a JSON value that
consists of single integer value.  Nevertheless, following an example
in SQLite's own documentation at https://www.sqlite.org/json1.html the name
JSON is being used for its familiarity.


:ticket:`3850`


Dialect Improvements and Changes - Oracle
=============================================

Dialect Improvements and Changes - SQL Server
=============================================
