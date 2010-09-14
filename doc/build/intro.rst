.. _overview_toplevel:

=======================
Overview / Installation
=======================

Overview
========


The SQLAlchemy SQL Toolkit and Object Relational Mapper
is a comprehensive set of tools for working with
databases and Python. It has several distinct areas of
functionality which can be used individually or combined
together. Its major components are illustrated below. The
arrows represent the general dependencies of components:

.. image:: sqla_arch_small.png

Above, the two most significant front-facing portions of
SQLAlchemy are the **Object Relational Mapper** and the
**SQL Expression Language**. SQL Expressions can be used
independently of the ORM. When using the ORM, the SQL
Expression language remains part of the public facing API
as it is used within object-relational configurations and
queries.

Documentation Overview
======================

The documentation is separated into three sections: :ref:`orm_toplevel`, :ref:`core_toplevel`, and :ref:`dialect_toplevel`.  

In :ref:`orm_toplevel`, the Object Relational Mapper is introduced and fully
described. New users should begin with the :ref:`ormtutorial_toplevel`. If you
want to work with higher-level SQL which is constructed automatically for you,
as well as management of Python objects, proceed to this tutorial.

In :ref:`core_toplevel`, the breadth of SQLAlchemy's SQL and database
integration and description services are documented, the core of which is the
SQL Expression language.  The SQL Expression Language is a toolkit all its own,
independent of the ORM package, which can be used to construct manipulable SQL
expressions which can be programmatically constructed, modified, and executed,
returning cursor-like result sets.  In contrast to the ORM's domain-centric 
mode of usage, the expression language provides a schema-centric usage
paradigm.  New users should begin here with :ref:`sqlexpression_toplevel`.
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
`<http://www.sqlalchemy.org/trac/wiki/UsageRecipes>`_.

Installing SQLAlchemy
======================

Installing SQLAlchemy from scratch is most easily achieved with `setuptools
<http://pypi.python.org/pypi/setuptools/>`_, or alternatively
`pip <http://pypi.python.org/pypi/pip/>`_. Assuming it's installed, just run
this from the command-line:

.. sourcecode:: none

    # easy_install SQLAlchemy
    
Or with pip:

.. sourcecode:: none

    # pip install SQLAlchemy

This command will download the latest version of SQLAlchemy from the `Python
Cheese Shop <http://pypi.python.org/pypi/SQLAlchemy>`_ and install it to your
system.

Otherwise, you can install from the distribution using the ``setup.py`` script:

.. sourcecode:: none

    # python setup.py install

Installing a Database API
==========================

SQLAlchemy is designed to operate with a `DB-API <http://www.python.org/doc/peps/pep-0249/>`_ implementation built for a particular database, and includes support for the most popular databases.  The current list is at :ref:`supported_dbapis`.

Checking the Installed SQLAlchemy Version
=========================================

This documentation covers SQLAlchemy version 0.6.  If you're working on a system that already has SQLAlchemy installed, check the version from your Python prompt like this:

.. sourcecode:: python+sql

     >>> import sqlalchemy
     >>> sqlalchemy.__version__ # doctest: +SKIP
     0.6.0

0.5 to 0.6 Migration
=====================

Notes on what's changed from 0.5 to 0.6 is available on the SQLAlchemy wiki at `06Migration <http://www.sqlalchemy.org/trac/wiki/06Migration>`_.
