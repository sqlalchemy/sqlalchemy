.. _overview_toplevel:
.. _overview:

========
Overview
========

The SQLAlchemy SQL Toolkit and Object Relational Mapper
is a comprehensive set of tools for working with
databases and Python. It has several distinct areas of
functionality which can be used individually or combined
together. Its major components are illustrated in below,
with component dependencies organized into layers:

.. image:: sqla_arch_small.png

Above, the two most significant front-facing portions of
SQLAlchemy are the **Object Relational Mapper** and the
**SQL Expression Language**. SQL Expressions can be used
independently of the ORM. When using the ORM, the SQL
Expression language remains part of the public facing API
as it is used within object-relational configurations and
queries.

.. _doc_overview:

Documentation Overview
======================

The documentation is separated into three sections: :ref:`orm_toplevel`,
:ref:`core_toplevel`, and :ref:`dialect_toplevel`.

In :ref:`orm_toplevel`, the Object Relational Mapper is introduced and fully
described. New users should begin with the :ref:`ormtutorial_toplevel`. If you
want to work with higher-level SQL which is constructed automatically for you,
as well as management of Python objects, proceed to this tutorial.

In :ref:`core_toplevel`, the breadth of SQLAlchemy's SQL and database
integration and description services are documented, the core of which is the
SQL Expression language. The SQL Expression Language is a toolkit all its own,
independent of the ORM package, which can be used to construct manipulable SQL
expressions which can be programmatically constructed, modified, and executed,
returning cursor-like result sets. In contrast to the ORM's domain-centric
mode of usage, the expression language provides a schema-centric usage
paradigm. New users should begin here with :ref:`sqlexpression_toplevel`.
SQLAlchemy engine, connection, and pooling services are also described in
:ref:`core_toplevel`.

In :ref:`dialect_toplevel`, reference documentation for all provided
database and DBAPI backends is provided.

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

* cPython since version 2.6, through the 2.xx series
* cPython version 3, throughout all 3.xx series
* `Pypy <http://pypy.org/>`_ 2.1 or greater

.. versionchanged:: 0.9
   Python 2.6 is now the minimum Python version supported.

Platforms that don't currently have support include Jython, IronPython.
Jython has been supported in the past and may be supported in future
releases as well, depending on the state of Jython itself.

Supported Installation Methods
-------------------------------

SQLAlchemy supports installation using standard Python "distutils" or
"setuptools" methodologies. An overview of potential setups is as follows:

* **Plain Python Distutils** - SQLAlchemy can be installed with a clean
  Python install using the services provided via `Python Distutils <http://docs.python.org/distutils/>`_,
  using the ``setup.py`` script. The C extensions as well as Python 3 builds are supported.
* **Setuptools or Distribute** - When using `setuptools <http://pypi.python.org/pypi/setuptools/>`_,
  SQLAlchemy can be installed via ``setup.py`` or ``easy_install``, and the C
  extensions are supported.
* **pip** - `pip <http://pypi.python.org/pypi/pip/>`_ is an installer that
  rides on top of ``setuptools`` or ``distribute``, replacing the usage
  of ``easy_install``.  It is often preferred for its simpler mode of usage.

Install via pip
---------------

When ``pip`` is available, the distribution can be
downloaded from Pypi and installed in one step::

    pip install SQLAlchemy

This command will download the latest **released** version of SQLAlchemy from the `Python
Cheese Shop <http://pypi.python.org/pypi/SQLAlchemy>`_ and install it to your system.

In order to install the latest **prerelease** version, such as ``1.0.0b1``,
pip requires that the ``--pre`` flag be used::

    pip install --pre SQLAlchemy

Where above, if the most recent version is a prerelease, it will be installed
instead of the latest released version.


Installing using setup.py
----------------------------------

Otherwise, you can install from the distribution using the ``setup.py`` script::

    python setup.py install

Installing the C Extensions
----------------------------------

SQLAlchemy includes C extensions which provide an extra speed boost for
dealing with result sets.   The extensions are supported on both the 2.xx
and 3.xx series of cPython.

.. versionchanged:: 0.9.0

    The C extensions now compile on Python 3 as well as Python 2.

``setup.py`` will automatically build the extensions if an appropriate platform is
detected. If the build of the C extensions fails, due to missing compiler or
other issue, the setup process will output a warning message, and re-run the
build without the C extensions, upon completion reporting final status.

To run the build/install without even attempting to compile the C extensions,
the ``DISABLE_SQLALCHEMY_CEXT`` environment variable may be specified.  The
use case for this is either for special testing circumstances, or in the rare
case of compatibility/build issues not overcome by the usual "rebuild"
mechanism::

  # *** only in SQLAlchemy 0.9.4 / 0.8.6 or greater ***
  export DISABLE_SQLALCHEMY_CEXT=1; python setup.py install

.. versionadded:: 0.9.4,0.8.6  Support for disabling the build of
   C extensions using the ``DISABLE_SQLALCHEMY_CEXT`` environment variable
   has been added.  This allows control of C extension building whether or not
   setuptools is available, and additionally works around the fact that
   setuptools will possibly be **removing support** for command-line switches
   such as ``--without-extensions`` in a future release.

   For versions of SQLAlchemy prior to 0.9.4 or 0.8.6, the
   ``--without-cextensions`` option may be used to disable the attempt to build
   C extensions, provided setupools is in use, and provided the ``Feature``
   construct is supported by the installed version of setuptools::

      python setup.py --without-cextensions install

   Or with pip::

      pip install --global-option='--without-cextensions' SQLAlchemy


Installing on Python 3
----------------------------------

SQLAlchemy runs directly on Python 2 or Python 3, and can be installed in
either environment without any adjustments or code conversion.

.. versionchanged:: 0.9.0 Python 3 is now supported in place with no 2to3 step
   required.


Installing a Database API
----------------------------------

SQLAlchemy is designed to operate with a :term:`DBAPI` implementation built for a
particular database, and includes support for the most popular databases.
The individual database sections in :doc:`/dialects/index` enumerate
the available DBAPIs for each database, including external links.

Checking the Installed SQLAlchemy Version
------------------------------------------

This documentation covers SQLAlchemy version 1.0. If you're working on a
system that already has SQLAlchemy installed, check the version from your
Python prompt like this:

.. sourcecode:: python+sql

     >>> import sqlalchemy
     >>> sqlalchemy.__version__ # doctest: +SKIP
     1.0.0

.. _migration:

0.9 to 1.0 Migration
=====================

Notes on what's changed from 0.9 to 1.0 is available here at :doc:`changelog/migration_10`.
