Selectables, Tables, FROM objects
=================================

The term "selectable" refers to any object that rows can be selected from;
in SQLAlchemy, these objects descend from :class:`_expression.FromClause` and their
distinguishing feature is their :attr:`_expression.FromClause.c` attribute, which is
a namespace of all the columns contained within the FROM clause (these
elements are themselves :class:`_expression.ColumnElement` subclasses).

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
   :exclude-members: memoized_attribute, memoized_instancemethod, append_correlation, append_column, append_prefix, append_whereclause, append_having, append_from, append_order_by, append_group_by


.. autoclass:: Selectable
   :members:
   :inherited-members: ClauseElement

.. autoclass:: SelectBase
   :members:
   :inherited-members:  ClauseElement
   :exclude-members: memoized_attribute, memoized_instancemethod

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

Label Style Constants
---------------------

Constants used with the :meth:`_sql.GenerativeSelect.set_label_style`
method.

.. autodata:: LABEL_STYLE_DISAMBIGUATE_ONLY

.. autodata:: LABEL_STYLE_NONE

.. autodata:: LABEL_STYLE_TABLENAME_PLUS_COL

.. data:: LABEL_STYLE_DEFAULT

  The default label style, refers to :data:`_sql.LABEL_STYLE_DISAMBIGUATE_ONLY`.

  .. versionadded:: 1.4

  .. seealso::

      :meth:`_sql.Select.set_label_style`

      :meth:`_sql.Select.get_label_style`

