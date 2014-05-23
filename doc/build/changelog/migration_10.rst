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
