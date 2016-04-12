:orphan:

.. _index_toplevel:

========================
SQLAlchemy Documentation
========================

Getting Started
===============

A high level view and getting set up.

:doc:`Overview <intro>` |
:ref:`Installation Guide <installation>` |
:doc:`Frequently Asked Questions <faq/index>` |
:doc:`Migration from 1.0 <changelog/migration_11>` |
:doc:`Glossary <glossary>` |
:doc:`Changelog catalog <changelog/index>`

SQLAlchemy ORM
==============

Here, the Object Relational Mapper is introduced and
fully described. If you want to work with higher-level SQL which is
constructed automatically for you, as well as automated persistence
of Python objects, proceed first to the tutorial.

* **Read this first:**
  :doc:`orm/tutorial`

* **ORM Configuration:**
  :doc:`Mapper Configuration <orm/mapper_config>` |
  :doc:`Relationship Configuration <orm/relationships>`

* **Configuration Extensions:**
  :doc:`Declarative Extension <orm/extensions/declarative/index>` |
  :doc:`Association Proxy <orm/extensions/associationproxy>` |
  :doc:`Hybrid Attributes <orm/extensions/hybrid>` |
  :doc:`Automap <orm/extensions/automap>` |
  :doc:`Mutable Scalars <orm/extensions/mutable>` |
  :doc:`Indexable <orm/extensions/indexable>`

* **ORM Usage:**
  :doc:`Session Usage and Guidelines <orm/session>` |
  :doc:`Loading Objects <orm/loading_objects>` |
  :doc:`Cached Query Extension <orm/extensions/baked>`

* **Extending the ORM:**
  :doc:`ORM Events and Internals <orm/extending>`

* **Other:**
  :doc:`Introduction to Examples <orm/examples>`

SQLAlchemy Core
===============

The breadth of SQLAlchemy's SQL rendering engine, DBAPI
integration, transaction integration, and schema description services
are documented here.  In contrast to the ORM's domain-centric mode of usage, the SQL Expression Language provides a schema-centric usage paradigm.

* **Read this first:**
  :doc:`core/tutorial`

* **All the Built In SQL:**
  :doc:`SQL Expression API <core/expression_api>`

* **Engines, Connections, Pools:**
  :doc:`Engine Configuration <core/engines>` |
  :doc:`Connections, Transactions <core/connections>` |
  :doc:`Connection Pooling <core/pooling>`

* **Schema Definition:**
  :doc:`Overview <core/schema>` |
  :ref:`Tables and Columns <metadata_describing_toplevel>` |
  :ref:`Database Introspection (Reflection) <metadata_reflection_toplevel>` |
  :ref:`Insert/Update Defaults <metadata_defaults_toplevel>` |
  :ref:`Constraints and Indexes <metadata_constraints_toplevel>` |
  :ref:`Using Data Definition Language (DDL) <metadata_ddl_toplevel>`

* **Datatypes:**
  :ref:`Overview <types_toplevel>` |
  :ref:`Building Custom Types <types_custom>` |
  :ref:`API <types_api>`

* **Core Basics:**
  :doc:`Overview <core/api_basics>` |
  :doc:`Runtime Inspection API <core/inspection>` |
  :doc:`Event System <core/event>` |
  :doc:`Core Event Interfaces <core/events>` |
  :doc:`Creating Custom SQL Constructs <core/compiler>` |


Dialect Documentation
======================

The **dialect** is the system SQLAlchemy uses to communicate with various types of DBAPIs and databases.
This section describes notes, options, and usage patterns regarding individual dialects.

:doc:`Index of all Dialects <dialects/index>`

