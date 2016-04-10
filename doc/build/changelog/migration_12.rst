==============================
What's New in SQLAlchemy 1.2?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.1
    and SQLAlchemy version 1.2.   1.2 is currently under development
    and is unreleased.


Introduction
============

This guide introduces what's new in SQLAlchemy version 1.2,
and also documents changes which affect users migrating
their applications from the 1.1 series of SQLAlchemy to 1.2.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.

Platform Support
================

Targeting Python 2.7 and Up
---------------------------

SQLAlchemy 1.2 now moves the minimum Python version to 2.7, no longer
supporting 2.6.   New language features are expected to be merged
into the 1.2 series that were not supported in Python 2.6.  For Python 3 support,
SQLAlchemy is currnetly tested on versions 3.5 and 3.6.


New Features and Improvements - ORM
===================================

New Features and Improvements - Core
====================================

.. _change_2694:

New "autoescape" option for startswith(), endswith()
----------------------------------------------------

The "autoescape" parameter is added to :meth:`.Operators.startswith`,
:meth:`.Operators.endswith`, :meth:`.Operators.contains`.  This parameter
does what "escape" does, except that it also automatically performs a search-
and-replace of any wildcard characters to be escaped by that character, as
these operators already add the wildcard expression on the outside of the
given value.

An expression such as::

    >>> column('x').startswith('total%score', autoescape='/')

Renders as::

    x LIKE :x_1 || '%%' ESCAPE '/'

Where the value of the parameter "x_1" is ``'total/%score'``.

:ticket:`2694`

Key Behavioral Changes - ORM
============================

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

.. _change_3276:

Oracle foreign key constraint names are now "name normalized"
-------------------------------------------------------------

The names of foreign key constraints as delivered to a
:class:`.ForeignKeyConstraint` object during table reflection as well as
within the :meth:`.Inspector.get_foreign_keys` method will now be
"name normalized", that is, expressed as lower case for a case insensitive
name, rather than the raw UPPERCASE format that Oracle uses::

	>>> insp.get_indexes("addresses")
	[{'unique': False, 'column_names': [u'user_id'],
	  'name': u'address_idx', 'dialect_options': {}}]

	>>> insp.get_pk_constraint("addresses")
	{'name': u'pk_cons', 'constrained_columns': [u'id']}

	>>> insp.get_foreign_keys("addresses")
	[{'referred_table': u'users', 'referred_columns': [u'id'],
	  'referred_schema': None, 'name': u'user_id_fk',
	  'constrained_columns': [u'user_id']}]

Previously, the foreign keys result would look like::

	[{'referred_table': u'users', 'referred_columns': [u'id'],
	  'referred_schema': None, 'name': 'USER_ID_FK',
	  'constrained_columns': [u'user_id']}]

Where the above could create problems particularly with Alembic autogenerate.

:ticket:`3276`

