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


New Features and Improvements - Core
====================================


Key Behavioral Changes - ORM
============================


Key Behavioral Changes - Core
=============================


Dialect Improvements and Changes - Postgresql
=============================================


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
