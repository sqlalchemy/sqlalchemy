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

    **SQLAlchemy 1.4 / 2.0**

    SQLAlchemy 2.0 is functionally available as part of SQLAlchemy 1.4, and integrates
    Core and ORM working styles more closely than ever.   The new tutorial introduces
    both concepts in parallel.

    * **For a quick glance:** :doc:`/orm/quickstart` - a glimpse at what working with the ORM looks like

    * **For all users:** :doc:`/tutorial/index` - The new SQLAlchemy 1.4/2.0 Tutorial

  .. container::

    **Legacy SQLAlchemy 1.x Tutorials**

    The 1.x Object Relational Tutorial and Core Tutorial are the legacy tutorials
    that should be consulted for existing SQLAlchemy codebases.

    * :doc:`orm/tutorial`

    * :doc:`core/tutorial`


.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

        Migration Notes

  .. container::

    * :doc:`Migration from 1.3 <changelog/migration_14>` - Migration notes for SQLAlchemy Version 1.4
    * :doc:`SQLAlchemy 2.0 Preview <changelog/migration_20>` - Background on preparing a SQLAlchemy 1.4 application for SQLAlchemy 2.0
    * :doc:`Changelog catalog <changelog/index>` - Detailed changelogs for all SQLAlchemy Versions


.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

      Reference Documentation


  .. container:: orm

    **SQLAlchemy ORM**

    * **ORM Configuration:**
      :doc:`Mapped Class Configuration <orm/mapper_config>` |
      :doc:`Relationship Configuration <orm/relationships>`

    * **ORM Usage:**
      :doc:`Session Usage and Guidelines <orm/session>` |
      :doc:`Querying Data, Loading Objects <orm/loading_objects>` |
      :doc:`AsyncIO Support <orm/extensions/asyncio>`

    * **Configuration Extensions:**
      :doc:`Mypy integration <orm/extensions/mypy>` |
      :doc:`Association Proxy <orm/extensions/associationproxy>` |
      :doc:`Hybrid Attributes <orm/extensions/hybrid>` |
      :doc:`Automap <orm/extensions/automap>` |
      :doc:`Mutable Scalars <orm/extensions/mutable>` |
      :doc:`All extensions <orm/extensions/index>`

    * **Extending the ORM:**
      :doc:`ORM Events and Internals <orm/extending>`

    * **Other:**
      :doc:`Introduction to Examples <orm/examples>`

  .. container:: core

    **SQLAlchemy Core**

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

    * **SQL Reference:**
      :doc:`SQL Expression API docs <core/expression_api>`

    * **Datatypes:**
      :ref:`Overview <types_toplevel>` |
      :ref:`Building Custom Types <types_custom>` |
      :ref:`API <types_api>`

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
      :doc:`MySQL <dialects/mysql>` |
      :doc:`SQLite <dialects/sqlite>` |
      :doc:`Oracle <dialects/oracle>` |
      :doc:`Microsoft SQL Server <dialects/mssql>`

      :doc:`More Dialects ... <dialects/index>`

.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

        Supplementary

  .. container::

    * :doc:`Frequently Asked Questions <faq/index>` - A collection of common problems and solutions
    * :doc:`Glossary <glossary>` - Terms used in SQLAlchemy's documentation
    * :doc:`Error Message Guide <errors>` - Explainations of many SQLAlchemy Errors
    * :doc:`Complete table of of contents <contents>`
    * :ref:`Index <genindex>`
