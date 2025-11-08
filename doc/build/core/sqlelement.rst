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

.. _sqlelement_foundational_constructors:

Column Element Foundational Constructors
-----------------------------------------

Standalone functions imported from the ``sqlalchemy`` namespace which are
used when building up SQLAlchemy Expression Language constructs.

.. autofunction:: aggregate_order_by

.. autofunction:: and_

.. autofunction:: bindparam

.. autofunction:: bitwise_not

.. autofunction:: case

.. autofunction:: cast

.. autofunction:: column

.. autoclass:: custom_op
   :members:

.. autofunction:: distinct

.. autofunction:: extract

.. autofunction:: false

.. autofunction:: from_dml_column

.. autodata:: func

.. autofunction:: lambda_stmt

.. autofunction:: literal

.. autofunction:: literal_column

.. autofunction:: not_

.. autofunction:: null

.. autofunction:: or_

.. autofunction:: outparam

.. autofunction:: text

.. autofunction:: true

.. autofunction:: try_cast

.. autofunction:: tuple_

.. autofunction:: type_coerce

.. autoclass:: quoted_name

   .. attribute:: quote

      whether the string should be unconditionally quoted


.. _sqlelement_modifier_constructors:

Column Element Modifier Constructors
-------------------------------------

Functions listed here are more commonly available as methods from any
:class:`_sql.ColumnElement` construct, for example, the
:func:`_sql.label` function is usually invoked via the
:meth:`_sql.ColumnElement.label` method.

.. autofunction:: all_

.. autofunction:: any_

.. autofunction:: asc

.. autofunction:: between

.. autofunction:: collate

.. autofunction:: desc

.. autofunction:: funcfilter

.. autofunction:: label

.. autofunction:: nulls_first

.. function:: nullsfirst

   Synonym for the :func:`_sql.nulls_first` function.

   .. versionchanged:: 2.0.5 restored missing legacy symbol :func:`.nullsfirst`.

.. autofunction:: nulls_last

.. function:: nullslast

   Legacy synonym for the :func:`_sql.nulls_last` function.

   .. versionchanged:: 2.0.5 restored missing legacy symbol :func:`.nullslast`.

.. autofunction:: over

.. autofunction:: within_group

Column Element Class Documentation
-----------------------------------

The classes here are generated using the constructors listed at
:ref:`sqlelement_foundational_constructors` and
:ref:`sqlelement_modifier_constructors`.


.. autoclass:: BinaryExpression
   :members:

.. autoclass:: BindParameter
   :members:

.. autoclass:: Case
   :members:

.. autoclass:: Cast
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

.. data:: ColumnExpressionArgument

   General purpose "column expression" argument.

   .. versionadded:: 2.0.13

   This type is used for "column" kinds of expressions that typically represent
   a single SQL column expression, including :class:`_sql.ColumnElement`, as
   well as ORM-mapped attributes that will have a ``__clause_element__()``
   method.

.. autoclass:: AggregateOrderBy
   :members:

.. autoclass:: ColumnOperators
   :members:
   :special-members:
   :inherited-members:

.. autoclass:: DMLTargetCopy

.. autoclass:: Extract
   :members:

.. autoclass:: False_
   :members:

.. autoclass:: FunctionFilter
   :members:

.. autoclass:: Label
   :members:

.. autoclass:: Null
   :members:

.. autoclass:: OperatorClass
   :members:
   :undoc-members:

.. autoclass:: Operators
   :members:
   :special-members:

.. autoclass:: OrderByList
   :members:

.. autoclass:: Over
   :members:

.. autoclass:: FrameClause
   :members:

.. autoclass:: FrameClauseType
   :members:

.. autoclass:: SQLColumnExpression

.. autoclass:: TextClause
   :members:

.. autoclass:: TryCast
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

.. autoclass:: UnaryExpression
   :members:

Column Element Typing Utilities
-------------------------------

Standalone utility functions imported from the ``sqlalchemy`` namespace
to improve support by type checkers.


.. autofunction:: sqlalchemy.NotNullable

.. autofunction:: sqlalchemy.Nullable
