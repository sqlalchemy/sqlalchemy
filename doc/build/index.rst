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

    A high level view and getting set up.

    :doc:`Overview <intro>` |
    :ref:`Installation Guide <installation>` |
    :doc:`Frequently Asked Questions <faq/index>` |
    :doc:`Migration from 1.3 <changelog/migration_14>` |
    :doc:`Glossary <glossary>` |
    :doc:`Error Messages <errors>` |
    :doc:`Changelog catalog <changelog/index>`


.. container:: left_right_container

  .. container:: leftmost

    .. rst-class:: h2

        Tutorials

  .. container::

    **SQLAlchemy 1.4 / 2.0 Transitional**

    SQLAlchemy 2.0 is functionally available as part of SQLAlchemy 1.4, and integrates
    Core and ORM working styles more closely than ever.   The new tutorial introduces
    both concepts in parallel.  New users and those starting new projects should start here!

    * :doc:`/tutorial/index` - SQLAlchemy 2.0's main tutorial

    * :doc:`Migrating to SQLAlchemy 2.0 <changelog/migration_20>` - Complete background on migrating from 1.3 or 1.4 to 2.0


  .. container::

    **SQLAlchemy 1.x Releases**

    The 1.x Object Relational Tutorial and Core Tutorial are the legacy tutorials
    that should be consulted for existing SQLAlchemy codebases.

    * :doc:`orm/tutorial`

    * :doc:`core/tutorial`


.. container:: left_right_container

  .. container:: leftmost

      .. rst-class:: h2

      Reference Documentation


  .. container:: orm

    **SQLAlchemy ORM**

    * **ORM Configuration:**
      :doc:`Mapper Configuration <orm/mapper_config>` |
      :doc:`Relationship Configuration <orm/relationships>`

    * **ORM Usage:**
      :doc:`Session Usage and Guidelines <orm/session>` |
      :doc:`Querying Data, Loading Objects <orm/loading_objects>` |
      :doc:`AsyncIO Support <orm/extensions/asyncio>`

    * **Configuration Extensions:**
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
      :doc:`Connections, Transactions <core/connections>` |
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

