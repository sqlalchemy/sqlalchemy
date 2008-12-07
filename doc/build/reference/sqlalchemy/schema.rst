.. _schema:

Database Schema
===============

.. module:: sqlalchemy.schema

SQLAlchemy schema definition language.  For more usage examples, see :ref:`metadata_toplevel`.

Tables and Columns
------------------

.. autoclass:: Column
    :members:
    :inherited-members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: MetaData
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: Table
    :members:
    :inherited-members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: ThreadLocalMetaData
    :members:
    :undoc-members:
    :show-inheritance:

Constraints
-----------

.. autoclass:: CheckConstraint
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: Constraint
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: ForeignKey
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: ForeignKeyConstraint
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: Index
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: PrimaryKeyConstraint
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: UniqueConstraint
    :members:
    :undoc-members:
    :show-inheritance:

Default Generators and Markers
------------------------------

.. autoclass:: ColumnDefault
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: DefaultClause
    :undoc-members:
    :show-inheritance:

.. autoclass:: DefaultGenerator
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: FetchedValue
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: PassiveDefault
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: Sequence
    :members:
    :undoc-members:
    :show-inheritance:

DDL
---

.. autoclass:: DDL
    :members:
    :undoc-members:
    :show-inheritance:

Internals
---------

.. autoclass:: SchemaItem
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: SchemaVisitor
    :members:
    :undoc-members:
    :show-inheritance:
