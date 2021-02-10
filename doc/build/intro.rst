.. _overview_toplevel:
.. _overview:

========
Overview
========

The SQLAlchemy SQL Toolkit and Object Relational Mapper
is a comprehensive set of tools for working with
databases and Python. It has several distinct areas of
functionality which can be used individually or combined
together. Its major components are illustrated below,
with component dependencies organized into layers:

.. image:: sqla_arch_small.png

Above, the two most significant front-facing portions of
SQLAlchemy are the **Object Relational Mapper (ORM)** and the
**Core**.

Core contains the breadth of SQLAlchemy's SQL and database
integration and description services, the most prominent part of this
being the **SQL Expression Language**.

The SQL Expression Language is a toolkit all its own, independent of the ORM
package, which provides a system of constructing SQL expressions represented by
composable objects, which can then be "executed" against a target database
within the scope of a specific transaction, returning a result set.
Inserts, updates and deletes (i.e. :term:`DML`) are achieved by passing
SQL expression objects representing these statements along with dictionaries
that represent parameters to be used with each statement.

The ORM builds upon Core to provide a means of working with a domain object
model mapped to a database schema. When using the ORM, SQL statements are
constructed in mostly the same way as when using Core, however the task of DML,
which here refers to the persistence of business objects in a database, is
automated using a pattern called :term:`unit of work`, which translates changes
in state against mutable objects into INSERT, UPDATE and DELETE constructs
which are then invoked in terms of those objects. SELECT statements are also
augmented by ORM-specific automations and object-centric querying capabilities.

Whereas working with Core and the SQL Expression language presents a
schema-centric view of the database, along with a programming paradigm that is
oriented around immutability, the ORM builds on top of this a domain-centric
view of the database with a programming paradigm that is more explcitly
object-oriented and reliant upon mutability.  Since a relational database is
itself a mutable service, the difference is that Core/SQL Expression language
is command oriented whereas the ORM is state oriented.


.. _doc_overview:

Documentation Overview
======================

The documentation is separated into four sections:

* :ref:`unified_tutorial` - this all-new tutorial for the 1.4/2.0 series of
  SQLAlchemy introduces the entire library holistically, starting from a
  description of Core and working more and more towards ORM-specific concepts.
  New users, as well as users coming from :term:`1.x style`, who wish to work
  in :term:`2.0 style` should start here.

* :ref:`orm_toplevel` - In this section, reference documentation for the ORM is
  presented; this section also includes the now-legacy
  :ref:`ormtutorial_toplevel`.

* :ref:`core_toplevel` - Here, reference documentation for
  everything else within Core is presented; section also includes the legacy
  :ref:`sqlexpression_toplevel`. SQLAlchemy engine, connection, and pooling
  services are also described here.

* :ref:`dialect_toplevel` - Provides reference documentation
  for all :term:`dialect` implementations, including :term:`DBAPI` specifics.





Code Examples
=============

Working code examples, mostly regarding the ORM, are included in the
SQLAlchemy distribution. A description of all the included example
applications is at :ref:`examples_toplevel`.

There is also a wide variety of examples involving both core SQLAlchemy
constructs as well as the ORM on the wiki.  See
`Theatrum Chemicum <http://www.sqlalchemy.org/trac/wiki/UsageRecipes>`_.

.. _installation:

Installation Guide
==================

Supported Platforms
-------------------

SQLAlchemy has been tested against the following platforms:

* cPython 2.7
* cPython 3.6 and higher
* `PyPy <http://pypy.org/>`_ 2.1 or greater

.. versionchanged:: 1.4
   Within the Python 3 series, 3.6 is now the minimum Python 3 version supported.

   .. seealso::

      :ref:`change_5634`

Supported Installation Methods
-------------------------------

SQLAlchemy installation is via standard Python methodologies that are
based on `setuptools <http://pypi.python.org/pypi/setuptools/>`_, either
by referring to ``setup.py`` directly or by using
`pip <http://pypi.python.org/pypi/pip/>`_ or other setuptools-compatible
approaches.

.. versionchanged:: 1.1 setuptools is now required by the setup.py file;
   plain distutils installs are no longer supported.

Install via pip
---------------

When ``pip`` is available, the distribution can be
downloaded from PyPI and installed in one step::

    pip install SQLAlchemy

This command will download the latest **released** version of SQLAlchemy from the `Python
Cheese Shop <http://pypi.python.org/pypi/SQLAlchemy>`_ and install it to your system.

In order to install the latest **prerelease** version, such as ``1.4.0b1``,
pip requires that the ``--pre`` flag be used::

    pip install --pre SQLAlchemy

Where above, if the most recent version is a prerelease, it will be installed
instead of the latest released version.


Installing using setup.py
----------------------------------

Otherwise, you can install from the distribution using the ``setup.py`` script::

    python setup.py install

.. _c_extensions:

Installing the C Extensions
----------------------------------

SQLAlchemy includes C extensions which provide an extra speed boost for
dealing with result sets.   The extensions are supported on both the 2.xx
and 3.xx series of cPython.

``setup.py`` will automatically build the extensions if an appropriate platform is
detected. If the build of the C extensions fails due to a missing compiler or
other issue, the setup process will output a warning message and re-run the
build without the C extensions upon completion, reporting final status.

To run the build/install without even attempting to compile the C extensions,
the ``DISABLE_SQLALCHEMY_CEXT`` environment variable may be specified.  The
use case for this is either for special testing circumstances, or in the rare
case of compatibility/build issues not overcome by the usual "rebuild"
mechanism::

  export DISABLE_SQLALCHEMY_CEXT=1; python setup.py install

.. versionchanged:: 1.1 The legacy ``--without-cextensions`` flag has been
   removed from the installer as it relies on deprecated features of
   setuptools.


Installing a Database API
----------------------------------

SQLAlchemy is designed to operate with a :term:`DBAPI` implementation built for a
particular database, and includes support for the most popular databases.
The individual database sections in :doc:`/dialects/index` enumerate
the available DBAPIs for each database, including external links.

Checking the Installed SQLAlchemy Version
------------------------------------------

This documentation covers SQLAlchemy version 1.4. If you're working on a
system that already has SQLAlchemy installed, check the version from your
Python prompt like this:

.. sourcecode:: python+sql

     >>> import sqlalchemy
     >>> sqlalchemy.__version__ # doctest: +SKIP
     1.4.0

.. _migration:

1.3 to 1.4 Migration
=====================

Notes on what's changed from 1.3 to 1.4 is available here at :doc:`changelog/migration_14`.
