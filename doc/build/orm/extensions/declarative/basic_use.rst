=========
Basic Use
=========

This section has moved to :ref:`orm_declarative_mapping`.

Defining Attributes
===================

This section is covered by :ref:`mapping_columns_toplevel`



Accessing the MetaData
======================

This section has moved to :ref:`orm_declarative_metadata`.


Class Constructor
=================

As a convenience feature, the :func:`declarative_base` sets a default
constructor on classes which takes keyword arguments, and assigns them
to the named attributes::

    e = Engineer(primary_language='python')

Mapper Configuration
====================

This section is moved to :ref:`orm_declarative_mapper_options`.


.. _declarative_sql_expressions:

Defining SQL Expressions
========================

See :ref:`mapper_sql_expressions` for examples on declaratively
mapping attributes to SQL expressions.

