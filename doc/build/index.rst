:orphan:

.. _index_toplevel:

========================
SQLAlchemy Documentation
========================

.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

        Getting Started

  .. container::

    New to SQLAlchemy?   Start here:

    * **For Python Beginners:** :ref:`Installation Guide <installation>` - basic guidance on installing with pip and similar

    * **For Python Veterans:** :doc:`SQLAlchemy Overview <intro>` - brief architectural overview

.. container:: left_right_container

  .. container:: leftmost

    .. rst-class:: h2

        Tutorials

  .. container::

    New users of SQLAlchemy, as well as veterans of older SQLAlchemy
    release series, should start with the
    :doc:`/tutorial/index`, which covers everything an Alchemist needs
    to know when using the ORM or just Core.

    * **For a quick glance:** :doc:`/orm/quickstart` - a glimpse at what working with the ORM looks like

    * **For all users:** :doc:`/tutorial/index` - In depth tutorial for Core and ORM


.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

        Migration Notes

  .. container::

    Users coming SQLAlchemy version 2.0 will want to read:

    * :doc:`What's New in SQLAlchemy 2.1? <changelog/migration_21>` - New features and behaviors in version 2.1

    Users transitioning from 1.x versions of SQLAlchemy, such as version 1.4, will want to
    transition to version 2.0 overall before making any additional changes needed for
    the much smaller transition from 2.0 to 2.1.   Key documentation for the 1.x to 2.x
    transition:

    * :doc:`Migrating to SQLAlchemy 2.0 <changelog/migration_20>` - Complete background on migrating from 1.3 or 1.4 to 2.0
    * :doc:`What's New in SQLAlchemy 2.0? <changelog/whatsnew_20>` - New 2.0 features and behaviors beyond the 1.x migration

    An index of all changelogs and migration documentation is at:

    * :doc:`Changelog catalog <changelog/index>` - Detailed changelogs for all SQLAlchemy Versions


.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

      Reference and How To


  .. container:: orm

    **SQLAlchemy ORM** - Detailed guides and API reference for using the ORM

    * **Mapping Classes:**
      :doc:`Mapping Python Classes <orm/mapper_config>` |
      :doc:`Relationship Configuration <orm/relationships>`

    * **Using the ORM:**
      :doc:`Using the ORM Session <orm/session>` |
      :doc:`ORM Querying Guide <orm/queryguide/index>` |
      :doc:`Using AsyncIO <orm/extensions/asyncio>`

    * **Configuration Extensions:**
      :doc:`Association Proxy <orm/extensions/associationproxy>` |
      :doc:`Hybrid Attributes <orm/extensions/hybrid>` |
      :doc:`Mutable Scalars <orm/extensions/mutable>` |
      :doc:`Automap <orm/extensions/automap>` |
      :doc:`All extensions <orm/extensions/index>`

    * **Extending the ORM:**
      :doc:`ORM Events and Internals <orm/extending>`

    * **Other:**
      :doc:`Introduction to Examples <orm/examples>`

  .. container:: core

    **SQLAlchemy Core** - Detailed guides and API reference for working with Core

    * **Engines, Connections, Pools:**
      :doc:`Engine Configuration <core/engines>` |
      :doc:`Connections, Transactions, Results <core/connections>` |
      :doc:`AsyncIO Support <orm/extensions/asyncio>` |
      :doc:`Connection Pooling <core/pooling>`

    * **Schema Definition:**
      :doc:`Overview <core/schema>` |
      :ref:`Tables and Columns <metadata_describing_toplevel>` |
      :ref:`Database Introspection (Reflection) <metadata_reflection_toplevel>` |
      :ref:`Insert/Update Defaults <metadata_defaults_toplevel>` |
      :ref:`Constraints and Indexes <metadata_constraints_toplevel>` |
      :ref:`Using Data Definition Language (DDL) <metadata_ddl_toplevel>`

    * **SQL Statements:**
      :doc:`SQL Expression Elements <core/sqlelement>` |
      :doc:`Operator Reference <core/operators>` |
      :doc:`SELECT and related constructs <core/selectable>` |
      :doc:`INSERT, UPDATE, DELETE <core/dml>` |
      :doc:`SQL Functions <core/functions>` |
      :doc:`Table of Contents <core/expression_api>`



    * **Datatypes:**
      :ref:`Overview <types_toplevel>` |
      :ref:`Building Custom Types <types_custom>` |
      :ref:`Type API Reference <types_api>`

    * **Core Basics:**
      :doc:`Overview <core/api_basics>` |
      :doc:`Runtime Inspection API <core/inspection>` |
      :doc:`Event System <core/event>` |
      :doc:`Core Event Interfaces <core/events>` |
      :doc:`Creating Custom SQL Constructs <core/compiler>`

.. container:: left_right_container

    .. container:: leftmost

      .. rst-class:: h2

        Dialect Documentation

    .. container::

      The **dialect** is the system SQLAlchemy uses to communicate with various types of DBAPIs and databases.
      This section describes notes, options, and usage patterns regarding individual dialects.

      :doc:`PostgreSQL <dialects/postgresql>` |
      :doc:`MySQL and MariaDB <dialects/mysql>` |
      :doc:`SQLite <dialects/sqlite>` |
      :doc:`Oracle Database <dialects/oracle>` |
      :doc:`Microsoft SQL Server <dialects/mssql>`

      :doc:`More Dialects ... <dialects/index>`

.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

        Supplementary

  .. container::

    * :doc:`Frequently Asked Questions <faq/index>` - A collection of common problems and solutions
    * :doc:`Glossary <glossary>` - Terms used in SQLAlchemy's documentation
    * :doc:`Error Message Guide <errors>` - Explanations of many SQLAlchemy Errors
    * :doc:`Complete table of of contents <contents>`
    * :ref:`Index <genindex>`
