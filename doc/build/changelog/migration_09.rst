==============================
What's New in SQLAlchemy 0.9?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.8,
    undergoing maintenance releases as of May, 2013,
    and SQLAlchemy version 0.9, which is expected for release
    in late 2013.

    Document date: May 29, 2013

Introduction
============

This guide introduces what's new in SQLAlchemy version 0.9,
and also documents changes which affect users migrating
their applications from the 0.8 series of SQLAlchemy to 0.9.

Version 0.9 is a faster-than-usual push from version 0.8,
featuring a more versatile codebase with regards to modern
Python versions.   See :ref:`behavioral_changes_09` for
potentially backwards-incompatible changes.

Platform Support
================

Targeting Python 2.6 and Up Now, Python 3 without 2to3
-------------------------------------------------------

The first achievement of the 0.9 release is to remove the dependency
on the 2to3 tool for Python 3 compatibility.  To make this
more straightforward, the lowest Python release targeted now
is 2.6, which features a wide degree of cross-compatibility with
Python 3.   All SQLAlchemy modules and unit tests are now interpreted
equally well with any Python interpreter from 2.6 forward, including
the 3.1 and 3.2 interpreters.

At the moment, the C extensions are still not fully ported to
Python 3.


.. _behavioral_changes_09:

Behavioral Changes
==================

.. _migration_2736:

:meth:`.Query.select_from` no longer applies the clause to corresponding entities
---------------------------------------------------------------------------------

The :meth:`.Query.select_from` method has been popularized in recent versions
as a means of controlling the first thing that a :class:`.Query` object
"selects from", typically for the purposes of controlling how a JOIN will
render.

Consider the following example against the usual ``User`` mapping::

	select_stmt = select([User]).where(User.id == 7).alias()

	q = session.query(User).\
               join(select_stmt, User.id == select_stmt.c.id).\
               filter(User.name == 'ed')

The above statement predictably renders SQL like the following::

	SELECT "user".id AS user_id, "user".name AS user_name
	FROM "user" JOIN (SELECT "user".id AS id, "user".name AS name
	FROM "user"
	WHERE "user".id = :id_1) AS anon_1 ON "user".id = anon_1.id
	WHERE "user".name = :name_1

If we wanted to reverse the order of the left and right elements of the
JOIN, the documentation would lead us to believe we could use
:meth:`.Query.select_from` to do so::

	q = session.query(User).\
	        select_from(select_stmt).\
	        join(User, User.id == select_stmt.c.id).\
	        filter(User.name == 'ed')

However, in version 0.8 and earlier, the above use of :meth:`.Query.select_from`
would apply the ``select_stmt`` to **replace** the ``User`` entity, as it
selects from the ``user`` table which is compatible with ``User``::

	-- SQLAlchemy 0.8 and earlier...
	SELECT anon_1.id AS anon_1_id, anon_1.name AS anon_1_name
	FROM (SELECT "user".id AS id, "user".name AS name
	FROM "user"
	WHERE "user".id = :id_1) AS anon_1 JOIN "user" ON anon_1.id = anon_1.id
	WHERE anon_1.name = :name_1

The above statement is a mess, the ON clause refers ``anon_1.id = anon_1.id``,
our WHERE clause has been replaced with ``anon_1`` as well.

This behavior is quite intentional, but has a different use case from that
which has become popular for :meth:`.Query.select_from`.  The above behavior
is now available by a new method known as :meth:`.Query.select_entity_from`.
This is a lesser used behavior that in modern SQLAlchemy is roughly equivalent
to selecting from a customized :func:`.aliased` construct::

	select_stmt = select([User]).where(User.id == 7)
	user_from_stmt = aliased(User, select_stmt.alias())

	q = session.query(user_from_stmt).filter(user_from_stmt.name == 'ed')

So with SQLAlchemy 0.9, our query that selects from ``select_stmt`` produces
the SQL we expect::

    -- SQLAlchemy 0.9
    SELECT "user".id AS user_id, "user".name AS user_name
    FROM (SELECT "user".id AS id, "user".name AS name
    FROM "user"
    WHERE "user".id = :id_1) AS anon_1 JOIN "user" ON "user".id = id
    WHERE "user".name = :name_1

The :meth:`.Query.select_entity_from` method will be available in SQLAlchemy
**0.8.2**, so applications which rely on the old behavior can transition
to this method first, ensure all tests continue to function, then upgrade
to 0.9 without issue.

:ticket:`2736`


Dialect Changes
===============

Firebird ``fdb`` is now the default Firebird dialect.
-----------------------------------------------------

The ``fdb`` dialect is now used if an engine is created without a dialect
specifier, i.e. ``firebird://``.  ``fdb`` is a ``kinterbasdb`` compatible
DBAPI which per the Firebird project is now their official Python driver.

:ticket:`2504`


