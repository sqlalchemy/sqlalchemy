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
:doc:`Frequently Asked Questions <faq>` |
:doc:`Migration from 0.8 <changelog/migration_09>` |
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
  :doc:`Relationship Configuration <orm/relationships>` |
  :doc:`Inheritance Mapping <orm/inheritance>` |
  :doc:`Advanced Collection Configuration <orm/collections>`

* **Configuration Extensions:**
  :doc:`Declarative Extension <orm/extensions/declarative>` |
  :doc:`Association Proxy <orm/extensions/associationproxy>` |
  :doc:`Hybrid Attributes <orm/extensions/hybrid>` |
  :doc:`Automap <orm/extensions/automap>` (**new**) |
  :doc:`Mutable Scalars <orm/extensions/mutable>` |
  :doc:`Ordered List <orm/extensions/orderinglist>`

* **ORM Usage:**
  :doc:`Session Usage and Guidelines <orm/session>` |
  :doc:`Query API reference <orm/query>` |
  :doc:`Relationship Loading Techniques <orm/loading>`

* **Extending the ORM:**
  :doc:`ORM Event Interfaces <orm/events>` |
  :doc:`Internals API <orm/internals>`

* **Other:**
  :doc:`Introduction to Examples <orm/examples>` |
  :doc:`Deprecated Event Interfaces <orm/deprecated>` |
  :doc:`ORM Exceptions <orm/exceptions>` |
  :doc:`Horizontal Sharding <orm/extensions/horizontal_shard>` |
  :doc:`Alternate Instrumentation <orm/extensions/instrumentation>`

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
  :ref:`Tables and Columns <metadata_describing_toplevel>` |
  :ref:`Database Introspection (Reflection) <metadata_reflection_toplevel>` |
  :ref:`Insert/Update Defaults <metadata_defaults_toplevel>` |
  :ref:`Constraints and Indexes <metadata_constraints_toplevel>` |
  :ref:`Using Data Definition Language (DDL) <metadata_ddl_toplevel>`

* **Datatypes:**
  :ref:`Overview <types_toplevel>` |
  :ref:`Generic Types <types_generic>` |
  :ref:`SQL Standard Types <types_sqlstandard>` |
  :ref:`Vendor Specific Types <types_vendor>` |
  :ref:`Building Custom Types <types_custom>` |
  :ref:`Defining New Operators <types_operators>` |
  :ref:`API <types_api>`

* **Extending the Core:**
  :doc:`SQLAlchemy Events <core/event>` |
  :doc:`Core Event Interfaces <core/events>` |
  :doc:`Creating Custom SQL Constructs <core/compiler>` |
  :doc:`Internals API <core/internals>`

* **Other:**
  :doc:`Runtime Inspection API <core/inspection>` |
  :doc:`core/interfaces` |
  :doc:`core/exceptions`


Dialect Documentation
======================

The **dialect** is the system SQLAlchemy uses to communicate with various types of DBAPIs and databases.
This section describes notes, options, and usage patterns regarding individual dialects.

:doc:`Index of all Dialects <dialects/index>`

