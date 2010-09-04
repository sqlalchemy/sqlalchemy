SQL Statements and Expressions
==============================

.. module:: sqlalchemy.sql.expression

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

.. attribute:: func
   
   Generate SQL function expressions.
   
   ``func`` is a special object instance which generates SQL functions based on name-based attributes, e.g.::
   
        >>> print func.count(1)
        count(:param_1)

   Any name can be given to `func`.  If the function name is unknown to SQLAlchemy, it will be rendered exactly as is.  For common SQL functions which SQLAlchemy is aware of, the name may be interpreted as a *generic function* which will be compiled appropriately to the target database::
    
        >>> print func.current_timestamp()
        CURRENT_TIMESTAMP
    
   To call functions which are present in dot-separated packages, specify them in the same manner::
    
        >>> print func.stats.yield_curve(5, 10)
        stats.yield_curve(:yield_curve_1, :yield_curve_2)
        
   SQLAlchemy can be made aware of the return type of functions to enable type-specific lexical and result-based behavior.  For example, to ensure that a string-based function returns a Unicode value and is similarly treated as a string in expressions, specify :class:`~sqlalchemy.types.Unicode` as the type:
    
        >>> print func.my_string(u'hi', type_=Unicode) + ' ' + \
        ... func.my_string(u'there', type_=Unicode)
        my_string(:my_string_1) || :my_string_2 || my_string(:my_string_3)
        
   Functions which are interpreted as "generic" functions know how to calculate their return type automatically.   For a listing of known generic functions, see :ref:`generic_functions`.
   
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

SQL functions which are known to SQLAlchemy with regards to database-specific rendering, return types and argument behavior.  Generic functions are invoked like all SQL functions, using the :attr:`func` attribute::
    
    select([func.count()]).select_from(sometable)
    
.. automodule:: sqlalchemy.sql.functions
   :members:
   :undoc-members:
   :show-inheritance:
   
   
