==============================
What's New in SQLAlchemy 1.0?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.9,
    undergoing maintenance releases as of May, 2014,
    and SQLAlchemy version 1.0, as of yet unreleased.

    Document last updated: May 23, 2014

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.0,
and also documents changes which affect users migrating
their applications from the 0.9 series of SQLAlchemy to 1.0.

Please carefully review
:ref:`behavioral_changes_orm_10` and :ref:`behavioral_changes_core_10` for
potentially backwards-incompatible changes.


.. _behavioral_changes_orm_10:

Behavioral Changes - ORM
========================

.. _migration_3061:

Changes to attribute events and other operations regarding attributes that have no pre-existing value
------------------------------------------------------------------------------------------------------

Given an object with no state::

	>>> obj = Foo()

It has always been SQLAlchemy's behavior such that if we access a scalar
or many-to-one attribute that was never set, it is returned as ``None``::

	>>> obj.someattr
	None

This value of ``None`` is in fact now part of the state of ``obj``, and is
not unlike as though we had set the attribute explicitly, e.g.
``obj.someattr = None``.  However, the "set on get" here would behave
differently as far as history and events.   It would not emit any attribute
event, and additionally if we view history, we see this::

	>>> inspect(obj).attrs.someattr.history
	History(added=(), unchanged=[None], deleted=())	  # 0.9 and below

That is, it's as though the attribute were always ``None`` and were
never changed.  This is explicitly different from if we had set the
attribute first instead::

	>>> obj = Foo()
	>>> obj.someattr = None
	>>> inspect(obj).attrs.someattr.history
	History(added=[None], unchanged=(), deleted=())  # all versions

The above means that the behavior of our "set" operation can be corrupted
by the fact that the value was accessed via "get" earlier.  In 1.0, this
inconsistency has been resolved::

	>>> obj = Foo()
	>>> obj.someattr
	None
	>>> inspect(obj).attrs.someattr.history
	History(added=[None], unchanged=(), deleted=())  # 1.0
	>>> obj.someattr = None
	>>> inspect(obj).attrs.someattr.history
	History(added=[None], unchanged=(), deleted=())

The reason the above behavior hasn't had much impact is because the
INSERT statement in relational databases considers a missing value to be
the same as NULL in most cases.   Whether SQLAlchemy received a history
event for a particular attribute set to None or not would usually not matter;
as the difference between sending None/NULL or not wouldn't have an impact.
However, as :ticket:`3060` illustrates, there are some seldom edge cases
where we do in fact want to positively have ``None`` set.  Also, allowing
the attribute event here means it's now possible to create "default value"
functions for ORM mapped attributes.

As part of this change, the generation of the implicit "None" is now disabled
for other situations where this used to occur; this includes when an
attribute set operation on a many-to-one is received; previously, the "old" value
would be "None" if it had been not set otherwise; it now will send the
value :data:`.orm.attributes.NEVER_SET`, which is a value that may be sent
to an attribute listener now.   This symbol may also be received when
calling on mapper utility functions such as :meth:`.Mapper.primary_key_from_state`;
if the primary key attributes have no setting at all, whereas the value
would be ``None`` before, it will now be the :data:`.orm.attributes.NEVER_SET`
symbol, and no change to the object's state occurs.

Performance-wise, the change has the tradeoff that an attribute will need
to be considered in a unit of work flush process in more cases than before, if it has
been accessed and populated with a default value.   However, performance
is improved in the case where the unit of work inspects a pending object for
an existing primary key value, as the state of the object won't change
in the common case that none was set.

.. _behavioral_changes_core_10:

Behavioral Changes - Core
=========================


New Features
============

.. _feature_3034:

Select/Query LIMIT / OFFSET may be specified as an arbitrary SQL expression
----------------------------------------------------------------------------

The :meth:`.Select.limit` and :meth:`.Select.offset` methods now accept
any SQL expression, in addition to integer values, as arguments.  The ORM
:class:`.Query` object also passes through any expression to the underlying
:class:`.Select` object.   Typically
this is used to allow a bound parameter to be passed, which can be substituted
with a value later::

	sel = select([table]).limit(bindparam('mylimit')).offset(bindparam('myoffset'))

Dialects which don't support non-integer LIMIT or OFFSET expressions may continue
to not support this behavior; third party dialects may also need modification
in order to take advantage of the new behavior.  A dialect which currently
uses the ``._limit`` or ``._offset`` attributes will continue to function
for those cases where the limit/offset was specified as a simple integer value.
However, when a SQL expression is specified, these two attributes will
instead raise a :class:`.CompileError` on access.  A third-party dialect which
wishes to support the new feature should now call upon the ``._limit_clause``
and ``._offset_clause`` attributes to receive the full SQL expression, rather
than the integer value.


Behavioral Improvements
=======================

Dialect Changes
===============
