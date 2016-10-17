Column Elements and Expressions
===============================

.. module:: sqlalchemy.sql.expression

The expression API consists of a series of classes that each represent a
specific lexical element within a SQL string.  Composed together
into a larger structure, they form a statement construct that may
be *compiled* into a string representation that can be passed to a database.
The classes are organized into a
hierarchy that begins at the basemost ClauseElement class. Key subclasses
include ColumnElement, which represents the role of any column-based expression
in a SQL statement, such as in the columns clause, WHERE clause, and ORDER BY
clause, and FromClause, which represents the role of a token that is placed in
the FROM clause of a SELECT statement.

.. autofunction:: all_

.. autofunction:: and_

.. autofunction:: any_

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

.. autofunction:: within_group

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

.. autoclass:: WithinGroup
   :members:

.. autoclass:: sqlalchemy.sql.elements.True_
   :members:

.. autoclass:: TypeCoerce
   :members:

.. autoclass:: sqlalchemy.sql.operators.custom_op
   :members:

.. autoclass:: sqlalchemy.sql.operators.Operators
   :members:
   :special-members:

.. autoclass:: sqlalchemy.sql.elements.quoted_name

.. autoclass:: UnaryExpression
   :members:



