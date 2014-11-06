Column Elements and Expressions
===============================

.. module:: sqlalchemy.sql.expression

The most fundamental part of the SQL expression API are the "column elements",
which allow for basic SQL expression support.   The core of all SQL expression
constructs is the :class:`.ClauseElement`, which is the base for several
sub-branches.  The :class:`.ColumnElement` class is the fundamental unit
used to construct any kind of typed SQL expression.

.. autofunction:: and_

.. autofunction:: asc

.. autofunction:: between

.. autofunction:: bindparam

.. autofunction:: case

.. autofunction:: cast

.. autofunction:: sqlalchemy.sql.expression.column

.. autofunction:: collate

.. autofunction:: desc

.. autofunction:: distinct

.. autofunction:: extract

.. autofunction:: false

.. autodata:: func

.. autofunction:: funcfilter

.. autofunction:: label

.. autofunction:: literal

.. autofunction:: literal_column

.. autofunction:: not_

.. autofunction:: null

.. autofunction:: nullsfirst

.. autofunction:: nullslast

.. autofunction:: or_

.. autofunction:: outparam

.. autofunction:: over

.. autofunction:: text

.. autofunction:: true

.. autofunction:: tuple_

.. autofunction:: type_coerce

.. autoclass:: BinaryExpression
   :members:

.. autoclass:: BindParameter
   :members:

.. autoclass:: Case
   :members:

.. autoclass:: Cast
   :members:

.. autoclass:: ClauseElement
   :members:


.. autoclass:: ClauseList
   :members:


.. autoclass:: ColumnClause
   :members:

.. autoclass:: ColumnCollection
   :members:


.. autoclass:: ColumnElement
   :members:
   :inherited-members:
   :undoc-members:

.. autoclass:: sqlalchemy.sql.operators.ColumnOperators
   :members:
   :special-members:
   :inherited-members:

.. autoclass:: sqlalchemy.sql.base.DialectKWArgs
   :members:

.. autoclass:: Extract
   :members:

.. autoclass:: sqlalchemy.sql.elements.False_
   :members:

.. autoclass:: FunctionFilter
   :members:

.. autoclass:: Label
   :members:

.. autoclass:: sqlalchemy.sql.elements.Null
   :members:

.. autoclass:: Over
   :members:

.. autoclass:: TextClause
   :members:

.. autoclass:: Tuple
   :members:

.. autoclass:: sqlalchemy.sql.elements.True_
   :members:

.. autoclass:: sqlalchemy.sql.operators.custom_op
   :members:

.. autoclass:: sqlalchemy.sql.operators.Operators
   :members:
   :special-members:

.. autoclass:: sqlalchemy.sql.elements.quoted_name

.. autoclass:: UnaryExpression
   :members:



