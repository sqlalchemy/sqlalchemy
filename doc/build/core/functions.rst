.. _functions_toplevel:
.. _generic_functions:

=========================
SQL and Generic Functions
=========================

.. currentmodule:: sqlalchemy.sql.functions

SQL functions which are known to SQLAlchemy with regards to database-specific
rendering, return types and argument behavior. Generic functions are invoked
like all SQL functions, using the :attr:`func` attribute::

    select(func.count()).select_from(sometable)

Note that any name not known to :attr:`func` generates the function name as is
- there is no restriction on what SQL functions can be called, known or
unknown to SQLAlchemy, built-in or user defined. The section here only
describes those functions where SQLAlchemy already knows what argument and
return types are in use.

.. seealso::

    :ref:`tutorial_functions` - in the :ref:`unified_tutorial`

.. automodule:: sqlalchemy.sql.functions
   :members:
   :exclude-members: func


