Column Elements and Expressions
===============================

.. currentmodule:: sqlalchemy.sql.expression

The expression API consists of a series of classes each of which represents a
specific lexical element within a SQL string.  Composed together
into a larger structure, they form a statement construct that may
be *compiled* into a string representation that can be passed to a database.
The classes are organized into a hierarchy that begins at the basemost
:class:`.ClauseElement` class. Key subclasses include :class:`.ColumnElement`,
which represents the role of any column-based expression
in a SQL statement, such as in the columns clause, WHERE clause, and ORDER BY
clause, and :class:`.FromClause`, which represents the role of a token that
is placed in the FROM clause of a SELECT statement.

.. autofunction:: all_

.. autofunction:: and_

.. autofunction:: any_

.. autofunction:: asc

.. autofunction:: between

.. autofunction:: bindparam

.. autofunction:: case

.. autofunction:: cast

.. autofunction:: column

.. autofunction:: collate

.. autofunction:: desc

.. autofunction:: distinct

.. autofunction:: extract

.. autofunction:: false

.. autodata:: func

.. autofunction:: funcfilter

.. autofunction:: label

.. autofunction:: lambda_stmt

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

.. autoclass:: CacheKey
   :members:

.. autoclass:: Case
   :members:

.. autoclass:: Cast
   :members:

.. autoclass:: ClauseElement
   :members:
   :inherited-members:


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

.. autoclass:: ColumnOperators
   :members:
   :special-members:
   :inherited-members:

.. autoclass:: sqlalchemy.sql.base.DialectKWArgs
   :members:

.. autoclass:: Extract
   :members:

.. autoclass:: False_
   :members:

.. autoclass:: FunctionFilter
   :members:

.. autoclass:: Label
   :members:

.. autoclass:: LambdaElement
   :members:

.. autoclass:: Null
   :members:

.. autoclass:: Over
   :members:

.. autoclass:: StatementLambdaElement
   :members:

.. autoclass:: TextClause
   :members:

.. autoclass:: Tuple
   :members:

.. autoclass:: WithinGroup
   :members:

.. autoclass:: sqlalchemy.sql.elements.WrapsColumnExpression
   :members:

.. autoclass:: True_
   :members:

.. autoclass:: TypeCoerce
   :members:

.. autoclass:: custom_op
   :members:

.. autoclass:: Operators
   :members:
   :special-members:

.. autoclass:: quoted_name

   .. attribute:: quote

      whether the string should be unconditionally quoted

.. autoclass:: UnaryExpression
   :members:



