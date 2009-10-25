.. _schema_api_toplevel:

Database Schema
===============

.. module:: sqlalchemy.schema

SQLAlchemy schema definition language.  For more usage examples, see :ref:`metadata_toplevel`.

Tables and Columns
------------------

.. autoclass:: Column
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: MetaData
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: Table
    :members:
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

.. _schema_api_ddl:

DDL Generation
--------------

.. autoclass:: DDLElement
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: DDL
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: CreateTable
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: DropTable
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: CreateSequence
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: DropSequence
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: CreateIndex
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: DropIndex
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: AddConstraint
    :members:
    :undoc-members:
    :show-inheritance:

.. autoclass:: DropConstraint
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
