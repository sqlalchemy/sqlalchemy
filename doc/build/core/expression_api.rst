.. _expression_api_toplevel:

SQL Statements and Expressions
==============================

.. module:: sqlalchemy.sql.expression

This section presents the API reference for the SQL Expression Language.  For a full introduction to its usage,
see :ref:`sqlexpression_toplevel`.

Functions
---------

The expression package uses functions to construct SQL expressions.  The return value of each function is an object instance which is a subclass of :class:`~sqlalchemy.sql.expression.ClauseElement`.

.. autofunction:: alias

.. autofunction:: and_

.. autofunction:: asc

.. autofunction:: between

.. autofunction:: bindparam

.. autofunction:: case

.. autofunction:: cast

.. autofunction:: column

.. autofunction:: collate

.. autofunction:: delete

.. autofunction:: desc

.. autofunction:: distinct

.. autofunction:: except_

.. autofunction:: except_all

.. autofunction:: exists

.. autofunction:: extract

.. autodata:: func

.. autofunction:: insert

.. autofunction:: intersect

.. autofunction:: intersect_all

.. autofunction:: join

.. autofunction:: label

.. autofunction:: literal

.. autofunction:: literal_column

.. autofunction:: not_

.. autofunction:: null

.. autofunction:: or_

.. autofunction:: outparam

.. autofunction:: outerjoin

.. autofunction:: select

.. autofunction:: subquery

.. autofunction:: table

.. autofunction:: text

.. autofunction:: tuple_

.. autofunction:: type_coerce

.. autofunction:: union

.. autofunction:: union_all

.. autofunction:: update

Classes
-------

.. autoclass:: Alias
   :members:
   :show-inheritance:

.. autoclass:: _BindParamClause
   :members:
   :show-inheritance:

.. autoclass:: ClauseElement
   :members:
   :show-inheritance:

.. autoclass:: ColumnClause
   :members:
   :show-inheritance:

.. autoclass:: ColumnCollection
   :members:
   :show-inheritance:

.. autoclass:: ColumnElement
   :members:
   :show-inheritance:

.. autoclass:: _CompareMixin
  :members:
  :undoc-members:
  :show-inheritance:

.. autoclass:: ColumnOperators
   :members:
   :undoc-members:
   :inherited-members:

.. autoclass:: CompoundSelect
   :members:
   :show-inheritance:

.. autoclass:: Delete
   :members: where
   :show-inheritance:

.. autoclass:: Executable
   :members:
   :show-inheritance:

.. autoclass:: FunctionElement
   :members:
   :show-inheritance:

.. autoclass:: Function
   :members:
   :show-inheritance:

.. autoclass:: FromClause
   :members:
   :show-inheritance:

.. autoclass:: Insert
   :members: prefix_with, values
   :show-inheritance:

.. autoclass:: Join
   :members:
   :show-inheritance:

.. autoclass:: Select
   :members:
   :show-inheritance:

.. autoclass:: Selectable
   :members:
   :show-inheritance:

.. autoclass:: _SelectBaseMixin
   :members:
   :show-inheritance:

.. autoclass:: TableClause
   :members:
   :show-inheritance:

.. autoclass:: Update
  :members: where, values
  :show-inheritance:

.. _generic_functions:

Generic Functions
-----------------

SQL functions which are known to SQLAlchemy with regards to database-specific
rendering, return types and argument behavior. Generic functions are invoked
like all SQL functions, using the :attr:`func` attribute::

    select([func.count()]).select_from(sometable)

Note that any name not known to :attr:`func` generates the function name as is
- there is no restriction on what SQL functions can be called, known or
unknown to SQLAlchemy, built-in or user defined. The section here only
describes those functions where SQLAlchemy already knows what argument and
return types are in use.

.. automodule:: sqlalchemy.sql.functions
   :members:
   :undoc-members:
   :show-inheritance:


