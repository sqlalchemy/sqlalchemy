SELECT and Related Constructs
=================================

The term "selectable" refers to any object that represents database rows. In
SQLAlchemy, these objects descend from :class:`_expression.Selectable`, the
most prominent being :class:`_expression.Select`, which represents a SQL SELECT
statement. A subset of :class:`_expression.Selectable` is
:class:`_expression.FromClause`, which represents objects that can be within
the FROM clause of a :class:`.Select` statement. A distinguishing feature of
:class:`_expression.FromClause` is the :attr:`_expression.FromClause.c`
attribute, which is a namespace of all the columns contained within the FROM
clause (these elements are themselves :class:`_expression.ColumnElement`
subclasses).

.. currentmodule:: sqlalchemy.sql.expression

.. _selectable_foundational_constructors:

Selectable Foundational Constructors
--------------------------------------

Top level "FROM clause" and "SELECT" constructors.


.. autofunction:: except_

.. autofunction:: except_all

.. autofunction:: exists

.. autofunction:: intersect

.. autofunction:: intersect_all

.. autofunction:: select

.. autofunction:: table

.. autofunction:: union

.. autofunction:: union_all

.. autofunction:: values


.. _fromclause_modifier_constructors:

Selectable Modifier Constructors
---------------------------------

Functions listed here are more commonly available as methods from
:class:`_sql.FromClause` and :class:`_sql.Selectable` elements, for example,
the :func:`_sql.alias` function is usually invoked via the
:meth:`_sql.FromClause.alias` method.

.. autofunction:: alias

.. autofunction:: cte

.. autofunction:: join

.. autofunction:: lateral

.. autofunction:: outerjoin

.. autofunction:: tablesample


Selectable Class Documentation
--------------------------------

The classes here are generated using the constructors listed at
:ref:`selectable_foundational_constructors` and
:ref:`fromclause_modifier_constructors`.

.. autoclass:: Alias
   :members:

.. autoclass:: AliasedReturnsRows
   :members:

.. autoclass:: CompoundSelect
   :inherited-members:  ClauseElement
   :members:

.. autoclass:: CTE
   :members:

.. autoclass:: Executable
   :members:

.. autoclass:: Exists
   :members:

.. autoclass:: FromClause
   :members:

.. autoclass:: GenerativeSelect
   :members:

.. autoclass:: HasCTE
   :members:

.. autoclass:: HasPrefixes
   :members:

.. autoclass:: HasSuffixes
   :members:

.. autoclass:: Join
   :members:

.. autoclass:: Lateral
   :members:

.. autoclass:: ReturnsRows
   :members:
   :inherited-members: ClauseElement

.. autoclass:: ScalarSelect
   :members:

.. autoclass:: Select
   :members:
   :inherited-members:  ClauseElement
   :exclude-members: __new__, memoized_attribute, memoized_instancemethod, append_correlation, append_column, append_prefix, append_whereclause, append_having, append_from, append_order_by, append_group_by


.. autoclass:: Selectable
   :members:
   :inherited-members: ClauseElement

.. autoclass:: SelectBase
   :members:
   :inherited-members:  ClauseElement
   :exclude-members: __new__, memoized_attribute, memoized_instancemethod

.. autoclass:: Subquery
   :members:

.. autoclass:: TableClause
   :members:
   :inherited-members:

.. autoclass:: TableSample
   :members:

.. autoclass:: TableValuedAlias
   :members:

.. autoclass:: TextualSelect
   :members:
   :inherited-members:

.. autoclass:: Values
   :members:
   :inherited-members: ClauseElement, FromClause, HasTraverseInternals, Selectable

.. autoclass:: ScalarValues
   :members:

Label Style Constants
---------------------

Constants used with the :meth:`_sql.GenerativeSelect.set_label_style`
method.

.. autoclass:: SelectLabelStyle
    :members:


.. seealso::

    :meth:`_sql.Select.set_label_style`

    :meth:`_sql.Select.get_label_style`

