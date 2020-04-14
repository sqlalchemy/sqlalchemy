.. change::
    :tags: change, sql
    :tickets: 4617

    As part of the SQLAlchemy 2.0 migration project, a conceptual change has
    been made to the role of the :class:`_expression.SelectBase` class hierarchy,
    which is the root of all "SELECT" statement constructs, in that they no
    longer serve directly as FROM clauses, that is, they no longer subclass
    :class:`_expression.FromClause`.  For end users, the change mostly means that any
    placement of a :func:`_expression.select` construct in the FROM clause of another
    :func:`_expression.select` requires first that it be wrapped in a subquery first,
    which historically is through the use of the :meth:`_expression.SelectBase.alias`
    method, and is now also available through the use of
    :meth:`_expression.SelectBase.subquery`.    This was usually a requirement in any
    case since several databases don't accept unnamed SELECT subqueries
    in their FROM clause in any case.

    .. seealso::

        :ref:`change_4617`

.. change::
    :tags: change, sql
    :tickets: 4617

    Added a new Core class :class:`.Subquery`, which takes the place of
    :class:`_expression.Alias` when creating named subqueries against a :class:`_expression.SelectBase`
    object.   :class:`.Subquery` acts in the same way as :class:`_expression.Alias`
    and is produced from the :meth:`_expression.SelectBase.subquery` method; for
    ease of use and backwards compatibility, the :meth:`_expression.SelectBase.alias`
    method is synonymous with this new method.

    .. seealso::

        :ref:`change_4617`

.. change::
    :tags: change, orm
    :tickets: 4617

    The ORM will now warn when asked to coerce a :func:`_expression.select` construct into
    a subquery implicitly.  This occurs within places such as the
    :meth:`_query.Query.select_entity_from` and  :meth:`_query.Query.select_from` methods
    as well as within the :func:`.with_polymorphic` function.  When a
    :class:`_expression.SelectBase` (which is what's produced by :func:`_expression.select`) or
    :class:`_query.Query` object is passed directly to these functions and others,
    the ORM is typically coercing them to be a subquery by calling the
    :meth:`_expression.SelectBase.alias` method automatically (which is now superceded by
    the :meth:`_expression.SelectBase.subquery` method).   See the migration notes linked
    below for further details.

    .. seealso::

        :ref:`change_4617`

.. change::
    :tags: bug, sql
    :tickets: 4617

    The ORDER BY clause of a :class:`_selectable.CompoundSelect`, e.g. UNION, EXCEPT, etc.
    will not render the table name associated with a given column when applying
    :meth:`_selectable.CompoundSelect.order_by` in terms of a :class:`_schema.Table` - bound
    column.   Most databases require that the names in the ORDER BY clause be
    expressed as label names only which are matched to names in the first
    SELECT statement.    The change is related to :ticket:`4617` in that a
    previous workaround was to refer to the ``.c`` attribute of the
    :class:`_selectable.CompoundSelect` in order to get at a column that has no table
    name.  As the subquery is now named, this change allows both the workaround
    to continue to work, as well as allows table-bound columns as well as the
    :attr:`_selectable.CompoundSelect.selected_columns` collections to be usable in the
    :meth:`_selectable.CompoundSelect.order_by` method.