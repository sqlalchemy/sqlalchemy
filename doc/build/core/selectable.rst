Selectables, Tables, FROM objects
=================================

The term "selectable" refers to any object that rows can be selected from;
in SQLAlchemy, these objects descend from :class:`.FromClause` and their
distinguishing feature is their :attr:`.FromClause.c` attribute, which is
a namespace of all the columns contained within the FROM clause (these
elements are themselves :class:`.ColumnElement` subclasses).

.. module:: sqlalchemy.sql.expression

.. autofunction:: alias

.. autofunction:: except_

.. autofunction:: except_all

.. autofunction:: exists

.. autofunction:: intersect

.. autofunction:: intersect_all

.. autofunction:: join

.. autofunction:: lateral

.. autofunction:: outerjoin

.. autofunction:: select

.. autofunction:: sqlalchemy.sql.expression.table

.. autofunction:: tablesample

.. autofunction:: union

.. autofunction:: union_all

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

.. autoclass:: ScalarSelect
   :members:

.. autoclass:: Select
   :members:
   :inherited-members:

.. autoclass:: Selectable
   :members:

.. autoclass:: SelectBase
   :members:

.. autoclass:: Subquery
   :members:

.. autoclass:: TableClause
   :members:
   :inherited-members:

.. autoclass:: TableSample
   :members:

.. autoclass:: TextualSelect
   :members:
