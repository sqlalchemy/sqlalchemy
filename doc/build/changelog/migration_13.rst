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

Key Behavioral Changes - Core
=============================

Dialect Improvements and Changes - PostgreSQL
=============================================

Dialect Improvements and Changes - MySQL
=============================================

Dialect Improvements and Changes - SQLite
=============================================

Dialect Improvements and Changes - Oracle
=============================================

Dialect Improvements and Changes - SQL Server
=============================================
