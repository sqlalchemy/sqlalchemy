# sql/selectable.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The :class:`.FromClause` class of SQL expression elements, representing
SQL tables and derived rowsets.

"""

import collections
import itertools
import operator
from operator import attrgetter

from sqlalchemy.sql.visitors import Visitable
from . import operators
from . import type_api
from .annotation import Annotated
from .base import _from_objects
from .base import _generative
from .base import ColumnCollection
from .base import ColumnSet
from .base import Executable
from .base import Generative
from .base import Immutable
from .elements import _anonymous_label
from .elements import _clause_element_as_expr
from .elements import _clone
from .elements import _cloned_difference
from .elements import _cloned_intersection
from .elements import _document_text_coercion
from .elements import _expand_cloned
from .elements import _interpret_as_column_or_from
from .elements import _literal_and_labels_as_label_reference
from .elements import _literal_as_label_reference
from .elements import _literal_as_text
from .elements import _no_text_coercion
from .elements import _select_iterables
from .elements import and_
from .elements import BindParameter
from .elements import ClauseElement
from .elements import ClauseList
from .elements import Grouping
from .elements import literal_column
from .elements import True_
from .elements import UnaryExpression
from .. import exc
from .. import inspection
from .. import util


def _interpret_as_from(element):
    insp = inspection.inspect(element, raiseerr=False)
    if insp is None:
        if isinstance(element, util.string_types):
            _no_text_coercion(element)
    try:
        return insp.selectable
    except AttributeError:
        raise exc.ArgumentError("FROM expression expected")


def _interpret_as_select(element):
    element = _interpret_as_from(element)
    if isinstance(element, Alias):
        element = element.original
    if not isinstance(element, SelectBase):
        element = element.select()
    return element


class _OffsetLimitParam(BindParameter):
    @property
    def _limit_offset_value(self):
        return self.effective_value


def _offset_or_limit_clause(element, name=None, type_=None):
    """Convert the given value to an "offset or limit" clause.

    This handles incoming integers and converts to an expression; if
    an expression is already given, it is passed through.

    """
    if element is None:
        return None
    elif hasattr(element, "__clause_element__"):
        return element.__clause_element__()
    elif isinstance(element, Visitable):
        return element
    else:
        value = util.asint(element)
        return _OffsetLimitParam(name, value, type_=type_, unique=True)


def _offset_or_limit_clause_asint(clause, attrname):
    """Convert the "offset or limit" clause of a select construct to an
    integer.

    This is only possible if the value is stored as a simple bound parameter.
    Otherwise, a compilation error is raised.

    """
    if clause is None:
        return None
    try:
        value = clause._limit_offset_value
    except AttributeError:
        raise exc.CompileError(
            "This SELECT structure does not use a simple "
            "integer value for %s" % attrname
        )
    else:
        return util.asint(value)


def subquery(alias, *args, **kwargs):
    r"""Return an :class:`.Alias` object derived
    from a :class:`.Select`.

    name
      alias name

    \*args, \**kwargs

      all other arguments are delivered to the
      :func:`select` function.

    """
    return Select(*args, **kwargs).alias(alias)


class Selectable(ClauseElement):
    """mark a class as being selectable"""

    __visit_name__ = "selectable"

    is_selectable = True

    @property
    def selectable(self):
        return self


class HasPrefixes(object):
    _prefixes = ()

    @_generative
    @_document_text_coercion(
        "expr",
        ":meth:`.HasPrefixes.prefix_with`",
        ":paramref:`.HasPrefixes.prefix_with.*expr`",
    )
    def prefix_with(self, *expr, **kw):
        r"""Add one or more expressions following the statement keyword, i.e.
        SELECT, INSERT, UPDATE, or DELETE. Generative.

        This is used to support backend-specific prefix keywords such as those
        provided by MySQL.

        E.g.::

            stmt = table.insert().prefix_with("LOW_PRIORITY", dialect="mysql")

            # MySQL 5.7 optimizer hints
            stmt = select([table]).prefix_with(
                "/*+ BKA(t1) */", dialect="mysql")

        Multiple prefixes can be specified by multiple calls
        to :meth:`.prefix_with`.

        :param \*expr: textual or :class:`.ClauseElement` construct which
         will be rendered following the INSERT, UPDATE, or DELETE
         keyword.
        :param \**kw: A single keyword 'dialect' is accepted.  This is an
         optional string dialect name which will
         limit rendering of this prefix to only that dialect.

        """
        dialect = kw.pop("dialect", None)
        if kw:
            raise exc.ArgumentError(
                "Unsupported argument(s): %s" % ",".join(kw)
            )
        self._setup_prefixes(expr, dialect)

    def _setup_prefixes(self, prefixes, dialect=None):
        self._prefixes = self._prefixes + tuple(
            [
                (_literal_as_text(p, allow_coercion_to_text=True), dialect)
                for p in prefixes
            ]
        )


class HasSuffixes(object):
    _suffixes = ()

    @_generative
    @_document_text_coercion(
        "expr",
        ":meth:`.HasSuffixes.suffix_with`",
        ":paramref:`.HasSuffixes.suffix_with.*expr`",
    )
    def suffix_with(self, *expr, **kw):
        r"""Add one or more expressions following the statement as a whole.

        This is used to support backend-specific suffix keywords on
        certain constructs.

        E.g.::

            stmt = select([col1, col2]).cte().suffix_with(
                "cycle empno set y_cycle to 1 default 0", dialect="oracle")

        Multiple suffixes can be specified by multiple calls
        to :meth:`.suffix_with`.

        :param \*expr: textual or :class:`.ClauseElement` construct which
         will be rendered following the target clause.
        :param \**kw: A single keyword 'dialect' is accepted.  This is an
         optional string dialect name which will
         limit rendering of this suffix to only that dialect.

        """
        dialect = kw.pop("dialect", None)
        if kw:
            raise exc.ArgumentError(
                "Unsupported argument(s): %s" % ",".join(kw)
            )
        self._setup_suffixes(expr, dialect)

    def _setup_suffixes(self, suffixes, dialect=None):
        self._suffixes = self._suffixes + tuple(
            [
                (_literal_as_text(p, allow_coercion_to_text=True), dialect)
                for p in suffixes
            ]
        )


class FromClause(Selectable):
    """Represent an element that can be used within the ``FROM``
    clause of a ``SELECT`` statement.

    The most common forms of :class:`.FromClause` are the
    :class:`.Table` and the :func:`.select` constructs.  Key
    features common to all :class:`.FromClause` objects include:

    * a :attr:`.c` collection, which provides per-name access to a collection
      of :class:`.ColumnElement` objects.
    * a :attr:`.primary_key` attribute, which is a collection of all those
      :class:`.ColumnElement` objects that indicate the ``primary_key`` flag.
    * Methods to generate various derivations of a "from" clause, including
      :meth:`.FromClause.alias`, :meth:`.FromClause.join`,
      :meth:`.FromClause.select`.


    """

    __visit_name__ = "fromclause"
    named_with_column = False
    _hide_froms = []

    _is_join = False
    _is_select = False
    _is_from_container = False

    _is_lateral = False

    _textual = False
    """a marker that allows us to easily distinguish a :class:`.TextAsFrom`
    or similar object from other kinds of :class:`.FromClause` objects."""

    schema = None
    """Define the 'schema' attribute for this :class:`.FromClause`.

    This is typically ``None`` for most objects except that of
    :class:`.Table`, where it is taken as the value of the
    :paramref:`.Table.schema` argument.

    """

    def _translate_schema(self, effective_schema, map_):
        return effective_schema

    _memoized_property = util.group_expirable_memoized_property(["_columns"])

    @util.deprecated(
        "1.1",
        message="The :meth:`.FromClause.count` method is deprecated, "
        "and will be removed in a future release.   Please use the "
        ":class:`.functions.count` function available from the "
        ":attr:`.func` namespace.",
    )
    @util.dependencies("sqlalchemy.sql.functions")
    def count(self, functions, whereclause=None, **params):
        """return a SELECT COUNT generated against this
        :class:`.FromClause`.

        .. seealso::

            :class:`.functions.count`

        """

        if self.primary_key:
            col = list(self.primary_key)[0]
        else:
            col = list(self.columns)[0]
        return Select(
            [functions.func.count(col).label("tbl_row_count")],
            whereclause,
            from_obj=[self],
            **params
        )

    def select(self, whereclause=None, **params):
        """return a SELECT of this :class:`.FromClause`.

        .. seealso::

            :func:`~.sql.expression.select` - general purpose
            method which allows for arbitrary column lists.

        """

        return Select([self], whereclause, **params)

    def join(self, right, onclause=None, isouter=False, full=False):
        """Return a :class:`.Join` from this :class:`.FromClause`
        to another :class:`FromClause`.

        E.g.::

            from sqlalchemy import join

            j = user_table.join(address_table,
                            user_table.c.id == address_table.c.user_id)
            stmt = select([user_table]).select_from(j)

        would emit SQL along the lines of::

            SELECT user.id, user.name FROM user
            JOIN address ON user.id = address.user_id

        :param right: the right side of the join; this is any
         :class:`.FromClause` object such as a :class:`.Table` object, and
         may also be a selectable-compatible object such as an ORM-mapped
         class.

        :param onclause: a SQL expression representing the ON clause of the
         join.  If left at ``None``, :meth:`.FromClause.join` will attempt to
         join the two tables based on a foreign key relationship.

        :param isouter: if True, render a LEFT OUTER JOIN, instead of JOIN.

        :param full: if True, render a FULL OUTER JOIN, instead of LEFT OUTER
         JOIN.  Implies :paramref:`.FromClause.join.isouter`.

         .. versionadded:: 1.1

        .. seealso::

            :func:`.join` - standalone function

            :class:`.Join` - the type of object produced

        """

        return Join(self, right, onclause, isouter, full)

    def outerjoin(self, right, onclause=None, full=False):
        """Return a :class:`.Join` from this :class:`.FromClause`
        to another :class:`FromClause`, with the "isouter" flag set to
        True.

        E.g.::

            from sqlalchemy import outerjoin

            j = user_table.outerjoin(address_table,
                            user_table.c.id == address_table.c.user_id)

        The above is equivalent to::

            j = user_table.join(
                address_table,
                user_table.c.id == address_table.c.user_id,
                isouter=True)

        :param right: the right side of the join; this is any
         :class:`.FromClause` object such as a :class:`.Table` object, and
         may also be a selectable-compatible object such as an ORM-mapped
         class.

        :param onclause: a SQL expression representing the ON clause of the
         join.  If left at ``None``, :meth:`.FromClause.join` will attempt to
         join the two tables based on a foreign key relationship.

        :param full: if True, render a FULL OUTER JOIN, instead of
         LEFT OUTER JOIN.

         .. versionadded:: 1.1

        .. seealso::

            :meth:`.FromClause.join`

            :class:`.Join`

        """

        return Join(self, right, onclause, True, full)

    def alias(self, name=None, flat=False):
        """return an alias of this :class:`.FromClause`.

        This is shorthand for calling::

            from sqlalchemy import alias
            a = alias(self, name=name)

        See :func:`~.expression.alias` for details.

        """

        return Alias._construct(self, name)

    def lateral(self, name=None):
        """Return a LATERAL alias of this :class:`.FromClause`.

        The return value is the :class:`.Lateral` construct also
        provided by the top-level :func:`~.expression.lateral` function.

        .. versionadded:: 1.1

        .. seealso::

            :ref:`lateral_selects` -  overview of usage.

        """
        return Lateral._construct(self, name)

    def tablesample(self, sampling, name=None, seed=None):
        """Return a TABLESAMPLE alias of this :class:`.FromClause`.

        The return value is the :class:`.TableSample` construct also
        provided by the top-level :func:`~.expression.tablesample` function.

        .. versionadded:: 1.1

        .. seealso::

            :func:`~.expression.tablesample` - usage guidelines and parameters

        """
        return TableSample._construct(self, sampling, name, seed)

    def is_derived_from(self, fromclause):
        """Return True if this FromClause is 'derived' from the given
        FromClause.

        An example would be an Alias of a Table is derived from that Table.

        """
        # this is essentially an "identity" check in the base class.
        # Other constructs override this to traverse through
        # contained elements.
        return fromclause in self._cloned_set

    def _is_lexical_equivalent(self, other):
        """Return True if this FromClause and the other represent
        the same lexical identity.

        This tests if either one is a copy of the other, or
        if they are the same via annotation identity.

        """
        return self._cloned_set.intersection(other._cloned_set)

    @util.dependencies("sqlalchemy.sql.util")
    def replace_selectable(self, sqlutil, old, alias):
        """replace all occurrences of FromClause 'old' with the given Alias
        object, returning a copy of this :class:`.FromClause`.

        """

        return sqlutil.ClauseAdapter(alias).traverse(self)

    def correspond_on_equivalents(self, column, equivalents):
        """Return corresponding_column for the given column, or if None
        search for a match in the given dictionary.

        """
        col = self.corresponding_column(column, require_embedded=True)
        if col is None and col in equivalents:
            for equiv in equivalents[col]:
                nc = self.corresponding_column(equiv, require_embedded=True)
                if nc:
                    return nc
        return col

    def corresponding_column(self, column, require_embedded=False):
        """Given a :class:`.ColumnElement`, return the exported
        :class:`.ColumnElement` object from this :class:`.Selectable`
        which corresponds to that original
        :class:`~sqlalchemy.schema.Column` via a common ancestor
        column.

        :param column: the target :class:`.ColumnElement` to be matched

        :param require_embedded: only return corresponding columns for
         the given :class:`.ColumnElement`, if the given
         :class:`.ColumnElement` is actually present within a sub-element
         of this :class:`.FromClause`.  Normally the column will match if
         it merely shares a common ancestor with one of the exported
         columns of this :class:`.FromClause`.

        """

        def embedded(expanded_proxy_set, target_set):
            for t in target_set.difference(expanded_proxy_set):
                if not set(_expand_cloned([t])).intersection(
                    expanded_proxy_set
                ):
                    return False
            return True

        # don't dig around if the column is locally present
        if self.c.contains_column(column):
            return column
        col, intersect = None, None
        target_set = column.proxy_set
        cols = self.c._all_columns
        for c in cols:
            expanded_proxy_set = set(_expand_cloned(c.proxy_set))
            i = target_set.intersection(expanded_proxy_set)
            if i and (
                not require_embedded
                or embedded(expanded_proxy_set, target_set)
            ):
                if col is None:

                    # no corresponding column yet, pick this one.

                    col, intersect = c, i
                elif len(i) > len(intersect):

                    # 'c' has a larger field of correspondence than
                    # 'col'. i.e. selectable.c.a1_x->a1.c.x->table.c.x
                    # matches a1.c.x->table.c.x better than
                    # selectable.c.x->table.c.x does.

                    col, intersect = c, i
                elif i == intersect:

                    # they have the same field of correspondence. see
                    # which proxy_set has fewer columns in it, which
                    # indicates a closer relationship with the root
                    # column. Also take into account the "weight"
                    # attribute which CompoundSelect() uses to give
                    # higher precedence to columns based on vertical
                    # position in the compound statement, and discard
                    # columns that have no reference to the target
                    # column (also occurs with CompoundSelect)

                    col_distance = util.reduce(
                        operator.add,
                        [
                            sc._annotations.get("weight", 1)
                            for sc in col.proxy_set
                            if sc.shares_lineage(column)
                        ],
                    )
                    c_distance = util.reduce(
                        operator.add,
                        [
                            sc._annotations.get("weight", 1)
                            for sc in c.proxy_set
                            if sc.shares_lineage(column)
                        ],
                    )
                    if c_distance < col_distance:
                        col, intersect = c, i
        return col

    @property
    def description(self):
        """a brief description of this FromClause.

        Used primarily for error message formatting.

        """
        return getattr(self, "name", self.__class__.__name__ + " object")

    def _reset_exported(self):
        """delete memoized collections when a FromClause is cloned."""

        self._memoized_property.expire_instance(self)

    @_memoized_property
    def columns(self):
        """A named-based collection of :class:`.ColumnElement` objects
        maintained by this :class:`.FromClause`.

        The :attr:`.columns`, or :attr:`.c` collection, is the gateway
        to the construction of SQL expressions using table-bound or
        other selectable-bound columns::

            select([mytable]).where(mytable.c.somecolumn == 5)

        """

        if "_columns" not in self.__dict__:
            self._init_collections()
            self._populate_column_collection()
        return self._columns.as_immutable()

    @_memoized_property
    def primary_key(self):
        """Return the collection of Column objects which comprise the
        primary key of this FromClause."""

        self._init_collections()
        self._populate_column_collection()
        return self.primary_key

    @_memoized_property
    def foreign_keys(self):
        """Return the collection of ForeignKey objects which this
        FromClause references."""

        self._init_collections()
        self._populate_column_collection()
        return self.foreign_keys

    c = property(
        attrgetter("columns"),
        doc="An alias for the :attr:`.columns` attribute.",
    )
    _select_iterable = property(attrgetter("columns"))

    def _init_collections(self):
        assert "_columns" not in self.__dict__
        assert "primary_key" not in self.__dict__
        assert "foreign_keys" not in self.__dict__

        self._columns = ColumnCollection()
        self.primary_key = ColumnSet()
        self.foreign_keys = set()

    @property
    def _cols_populated(self):
        return "_columns" in self.__dict__

    def _populate_column_collection(self):
        """Called on subclasses to establish the .c collection.

        Each implementation has a different way of establishing
        this collection.

        """

    def _refresh_for_new_column(self, column):
        """Given a column added to the .c collection of an underlying
        selectable, produce the local version of that column, assuming this
        selectable ultimately should proxy this column.

        this is used to "ping" a derived selectable to add a new column
        to its .c. collection when a Column has been added to one of the
        Table objects it ultimtely derives from.

        If the given selectable hasn't populated its .c. collection yet,
        it should at least pass on the message to the contained selectables,
        but it will return None.

        This method is currently used by Declarative to allow Table
        columns to be added to a partially constructed inheritance
        mapping that may have already produced joins.  The method
        isn't public right now, as the full span of implications
        and/or caveats aren't yet clear.

        It's also possible that this functionality could be invoked by
        default via an event, which would require that
        selectables maintain a weak referencing collection of all
        derivations.

        """
        if not self._cols_populated:
            return None
        elif column.key in self.columns and self.columns[column.key] is column:
            return column
        else:
            return None


class Join(FromClause):
    """represent a ``JOIN`` construct between two :class:`.FromClause`
    elements.

    The public constructor function for :class:`.Join` is the module-level
    :func:`.join()` function, as well as the :meth:`.FromClause.join` method
    of any :class:`.FromClause` (e.g. such as :class:`.Table`).

    .. seealso::

        :func:`.join`

        :meth:`.FromClause.join`

    """

    __visit_name__ = "join"

    _is_join = True

    def __init__(self, left, right, onclause=None, isouter=False, full=False):
        """Construct a new :class:`.Join`.

        The usual entrypoint here is the :func:`~.expression.join`
        function or the :meth:`.FromClause.join` method of any
        :class:`.FromClause` object.

        """
        self.left = _interpret_as_from(left)
        self.right = _interpret_as_from(right).self_group()

        if onclause is None:
            self.onclause = self._match_primaries(self.left, self.right)
        else:
            self.onclause = onclause

        self.isouter = isouter
        self.full = full

    @classmethod
    def _create_outerjoin(cls, left, right, onclause=None, full=False):
        """Return an ``OUTER JOIN`` clause element.

        The returned object is an instance of :class:`.Join`.

        Similar functionality is also available via the
        :meth:`~.FromClause.outerjoin()` method on any
        :class:`.FromClause`.

        :param left: The left side of the join.

        :param right: The right side of the join.

        :param onclause:  Optional criterion for the ``ON`` clause, is
          derived from foreign key relationships established between
          left and right otherwise.

        To chain joins together, use the :meth:`.FromClause.join` or
        :meth:`.FromClause.outerjoin` methods on the resulting
        :class:`.Join` object.

        """
        return cls(left, right, onclause, isouter=True, full=full)

    @classmethod
    def _create_join(
        cls, left, right, onclause=None, isouter=False, full=False
    ):
        """Produce a :class:`.Join` object, given two :class:`.FromClause`
        expressions.

        E.g.::

            j = join(user_table, address_table,
                     user_table.c.id == address_table.c.user_id)
            stmt = select([user_table]).select_from(j)

        would emit SQL along the lines of::

            SELECT user.id, user.name FROM user
            JOIN address ON user.id = address.user_id

        Similar functionality is available given any
        :class:`.FromClause` object (e.g. such as a :class:`.Table`) using
        the :meth:`.FromClause.join` method.

        :param left: The left side of the join.

        :param right: the right side of the join; this is any
         :class:`.FromClause` object such as a :class:`.Table` object, and
         may also be a selectable-compatible object such as an ORM-mapped
         class.

        :param onclause: a SQL expression representing the ON clause of the
         join.  If left at ``None``, :meth:`.FromClause.join` will attempt to
         join the two tables based on a foreign key relationship.

        :param isouter: if True, render a LEFT OUTER JOIN, instead of JOIN.

        :param full: if True, render a FULL OUTER JOIN, instead of JOIN.

         .. versionadded:: 1.1

        .. seealso::

            :meth:`.FromClause.join` - method form, based on a given left side

            :class:`.Join` - the type of object produced

        """

        return cls(left, right, onclause, isouter, full)

    @property
    def description(self):
        return "Join object on %s(%d) and %s(%d)" % (
            self.left.description,
            id(self.left),
            self.right.description,
            id(self.right),
        )

    def is_derived_from(self, fromclause):
        return (
            fromclause is self
            or self.left.is_derived_from(fromclause)
            or self.right.is_derived_from(fromclause)
        )

    def self_group(self, against=None):
        return FromGrouping(self)

    @util.dependencies("sqlalchemy.sql.util")
    def _populate_column_collection(self, sqlutil):
        columns = [c for c in self.left.columns] + [
            c for c in self.right.columns
        ]

        self.primary_key.extend(
            sqlutil.reduce_columns(
                (c for c in columns if c.primary_key), self.onclause
            )
        )
        self._columns.update((col._label, col) for col in columns)
        self.foreign_keys.update(
            itertools.chain(*[col.foreign_keys for col in columns])
        )

    def _refresh_for_new_column(self, column):
        col = self.left._refresh_for_new_column(column)
        if col is None:
            col = self.right._refresh_for_new_column(column)
        if col is not None:
            if self._cols_populated:
                self._columns[col._label] = col
                self.foreign_keys.update(col.foreign_keys)
                if col.primary_key:
                    self.primary_key.add(col)
                return col
        return None

    def _copy_internals(self, clone=_clone, **kw):
        self._reset_exported()
        self.left = clone(self.left, **kw)
        self.right = clone(self.right, **kw)
        self.onclause = clone(self.onclause, **kw)

    def get_children(self, **kwargs):
        return self.left, self.right, self.onclause

    def _match_primaries(self, left, right):
        if isinstance(left, Join):
            left_right = left.right
        else:
            left_right = None
        return self._join_condition(left, right, a_subset=left_right)

    @classmethod
    @util.deprecated_params(
        ignore_nonexistent_tables=(
            "0.9",
            "The :paramref:`.join_condition.ignore_nonexistent_tables` "
            "parameter is deprecated and will be removed in a future "
            "release.  Tables outside of the two tables being handled "
            "are no longer considered.",
        )
    )
    def _join_condition(
        cls,
        a,
        b,
        ignore_nonexistent_tables=False,
        a_subset=None,
        consider_as_foreign_keys=None,
    ):
        """create a join condition between two tables or selectables.

        e.g.::

            join_condition(tablea, tableb)

        would produce an expression along the lines of::

            tablea.c.id==tableb.c.tablea_id

        The join is determined based on the foreign key relationships
        between the two selectables.   If there are multiple ways
        to join, or no way to join, an error is raised.

        :param ignore_nonexistent_tables: unused - tables outside of the
         two tables being handled are not considered.

        :param a_subset: An optional expression that is a sub-component
         of ``a``.  An attempt will be made to join to just this sub-component
         first before looking at the full ``a`` construct, and if found
         will be successful even if there are other ways to join to ``a``.
         This allows the "right side" of a join to be passed thereby
         providing a "natural join".

        """
        constraints = cls._joincond_scan_left_right(
            a, a_subset, b, consider_as_foreign_keys
        )

        if len(constraints) > 1:
            cls._joincond_trim_constraints(
                a, b, constraints, consider_as_foreign_keys
            )

        if len(constraints) == 0:
            if isinstance(b, FromGrouping):
                hint = (
                    " Perhaps you meant to convert the right side to a "
                    "subquery using alias()?"
                )
            else:
                hint = ""
            raise exc.NoForeignKeysError(
                "Can't find any foreign key relationships "
                "between '%s' and '%s'.%s"
                % (a.description, b.description, hint)
            )

        crit = [(x == y) for x, y in list(constraints.values())[0]]
        if len(crit) == 1:
            return crit[0]
        else:
            return and_(*crit)

    @classmethod
    def _can_join(cls, left, right, consider_as_foreign_keys=None):
        if isinstance(left, Join):
            left_right = left.right
        else:
            left_right = None

        constraints = cls._joincond_scan_left_right(
            a=left,
            b=right,
            a_subset=left_right,
            consider_as_foreign_keys=consider_as_foreign_keys,
        )

        return bool(constraints)

    @classmethod
    def _joincond_scan_left_right(
        cls, a, a_subset, b, consider_as_foreign_keys
    ):
        constraints = collections.defaultdict(list)

        for left in (a_subset, a):
            if left is None:
                continue
            for fk in sorted(
                b.foreign_keys, key=lambda fk: fk.parent._creation_order
            ):
                if (
                    consider_as_foreign_keys is not None
                    and fk.parent not in consider_as_foreign_keys
                ):
                    continue
                try:
                    col = fk.get_referent(left)
                except exc.NoReferenceError as nrte:
                    if nrte.table_name == left.name:
                        raise
                    else:
                        continue

                if col is not None:
                    constraints[fk.constraint].append((col, fk.parent))
            if left is not b:
                for fk in sorted(
                    left.foreign_keys, key=lambda fk: fk.parent._creation_order
                ):
                    if (
                        consider_as_foreign_keys is not None
                        and fk.parent not in consider_as_foreign_keys
                    ):
                        continue
                    try:
                        col = fk.get_referent(b)
                    except exc.NoReferenceError as nrte:
                        if nrte.table_name == b.name:
                            raise
                        else:
                            continue

                    if col is not None:
                        constraints[fk.constraint].append((col, fk.parent))
            if constraints:
                break
        return constraints

    @classmethod
    def _joincond_trim_constraints(
        cls, a, b, constraints, consider_as_foreign_keys
    ):
        # more than one constraint matched.  narrow down the list
        # to include just those FKCs that match exactly to
        # "consider_as_foreign_keys".
        if consider_as_foreign_keys:
            for const in list(constraints):
                if set(f.parent for f in const.elements) != set(
                    consider_as_foreign_keys
                ):
                    del constraints[const]

        # if still multiple constraints, but
        # they all refer to the exact same end result, use it.
        if len(constraints) > 1:
            dedupe = set(tuple(crit) for crit in constraints.values())
            if len(dedupe) == 1:
                key = list(constraints)[0]
                constraints = {key: constraints[key]}

        if len(constraints) != 1:
            raise exc.AmbiguousForeignKeysError(
                "Can't determine join between '%s' and '%s'; "
                "tables have more than one foreign key "
                "constraint relationship between them. "
                "Please specify the 'onclause' of this "
                "join explicitly." % (a.description, b.description)
            )

    def select(self, whereclause=None, **kwargs):
        r"""Create a :class:`.Select` from this :class:`.Join`.

        The equivalent long-hand form, given a :class:`.Join` object
        ``j``, is::

            from sqlalchemy import select
            j = select([j.left, j.right], **kw).\
                        where(whereclause).\
                        select_from(j)

        :param whereclause: the WHERE criterion that will be sent to
          the :func:`select()` function

        :param \**kwargs: all other kwargs are sent to the
          underlying :func:`select()` function.

        """
        collist = [self.left, self.right]

        return Select(collist, whereclause, from_obj=[self], **kwargs)

    @property
    def bind(self):
        return self.left.bind or self.right.bind

    @util.dependencies("sqlalchemy.sql.util")
    def alias(self, sqlutil, name=None, flat=False):
        r"""return an alias of this :class:`.Join`.

        The default behavior here is to first produce a SELECT
        construct from this :class:`.Join`, then to produce an
        :class:`.Alias` from that.  So given a join of the form::

            j = table_a.join(table_b, table_a.c.id == table_b.c.a_id)

        The JOIN by itself would look like::

            table_a JOIN table_b ON table_a.id = table_b.a_id

        Whereas the alias of the above, ``j.alias()``, would in a
        SELECT context look like::

            (SELECT table_a.id AS table_a_id, table_b.id AS table_b_id,
                table_b.a_id AS table_b_a_id
                FROM table_a
                JOIN table_b ON table_a.id = table_b.a_id) AS anon_1

        The equivalent long-hand form, given a :class:`.Join` object
        ``j``, is::

            from sqlalchemy import select, alias
            j = alias(
                select([j.left, j.right]).\
                    select_from(j).\
                    with_labels(True).\
                    correlate(False),
                name=name
            )

        The selectable produced by :meth:`.Join.alias` features the same
        columns as that of the two individual selectables presented under
        a single name - the individual columns are "auto-labeled", meaning
        the ``.c.`` collection of the resulting :class:`.Alias` represents
        the names of the individual columns using a
        ``<tablename>_<columname>`` scheme::

            j.c.table_a_id
            j.c.table_b_a_id

        :meth:`.Join.alias` also features an alternate
        option for aliasing joins which produces no enclosing SELECT and
        does not normally apply labels to the column names.  The
        ``flat=True`` option will call :meth:`.FromClause.alias`
        against the left and right sides individually.
        Using this option, no new ``SELECT`` is produced;
        we instead, from a construct as below::

            j = table_a.join(table_b, table_a.c.id == table_b.c.a_id)
            j = j.alias(flat=True)

        we get a result like this::

            table_a AS table_a_1 JOIN table_b AS table_b_1 ON
            table_a_1.id = table_b_1.a_id

        The ``flat=True`` argument is also propagated to the contained
        selectables, so that a composite join such as::

            j = table_a.join(
                    table_b.join(table_c,
                            table_b.c.id == table_c.c.b_id),
                    table_b.c.a_id == table_a.c.id
                ).alias(flat=True)

        Will produce an expression like::

            table_a AS table_a_1 JOIN (
                    table_b AS table_b_1 JOIN table_c AS table_c_1
                    ON table_b_1.id = table_c_1.b_id
            ) ON table_a_1.id = table_b_1.a_id

        The standalone :func:`~.expression.alias` function as well as the
        base :meth:`.FromClause.alias` method also support the ``flat=True``
        argument as a no-op, so that the argument can be passed to the
        ``alias()`` method of any selectable.

        .. versionadded:: 0.9.0 Added the ``flat=True`` option to create
          "aliases" of joins without enclosing inside of a SELECT
          subquery.

        :param name: name given to the alias.

        :param flat: if True, produce an alias of the left and right
         sides of this :class:`.Join` and return the join of those
         two selectables.   This produces join expression that does not
         include an enclosing SELECT.

         .. versionadded:: 0.9.0

        .. seealso::

            :func:`~.expression.alias`

        """
        if flat:
            assert name is None, "Can't send name argument with flat"
            left_a, right_a = (
                self.left.alias(flat=True),
                self.right.alias(flat=True),
            )
            adapter = sqlutil.ClauseAdapter(left_a).chain(
                sqlutil.ClauseAdapter(right_a)
            )

            return left_a.join(
                right_a,
                adapter.traverse(self.onclause),
                isouter=self.isouter,
                full=self.full,
            )
        else:
            return self.select(use_labels=True, correlate=False).alias(name)

    @property
    def _hide_froms(self):
        return itertools.chain(
            *[_from_objects(x.left, x.right) for x in self._cloned_set]
        )

    @property
    def _from_objects(self):
        return (
            [self]
            + self.onclause._from_objects
            + self.left._from_objects
            + self.right._from_objects
        )


class Alias(FromClause):
    """Represents an table or selectable alias (AS).

    Represents an alias, as typically applied to any table or
    sub-select within a SQL statement using the ``AS`` keyword (or
    without the keyword on certain databases such as Oracle).

    This object is constructed from the :func:`~.expression.alias` module
    level function as well as the :meth:`.FromClause.alias` method available
    on all :class:`.FromClause` subclasses.

    """

    __visit_name__ = "alias"
    named_with_column = True

    _is_from_container = True

    def __init__(self, *arg, **kw):
        raise NotImplementedError(
            "The %s class is not intended to be constructed "
            "directly.  Please use the %s() standalone "
            "function or the %s() method available from appropriate "
            "selectable objects."
            % (
                self.__class__.__name__,
                self.__class__.__name__.lower(),
                self.__class__.__name__.lower(),
            )
        )

    @classmethod
    def _construct(cls, *arg, **kw):
        obj = cls.__new__(cls)
        obj._init(*arg, **kw)
        return obj

    @classmethod
    def _factory(cls, selectable, name=None, flat=False):
        """Return an :class:`.Alias` object.

        An :class:`.Alias` represents any :class:`.FromClause`
        with an alternate name assigned within SQL, typically using the ``AS``
        clause when generated, e.g. ``SELECT * FROM table AS aliasname``.

        Similar functionality is available via the
        :meth:`~.FromClause.alias` method
        available on all :class:`.FromClause` subclasses.

        When an :class:`.Alias` is created from a :class:`.Table` object,
        this has the effect of the table being rendered
        as ``tablename AS aliasname`` in a SELECT statement.

        For :func:`.select` objects, the effect is that of creating a named
        subquery, i.e. ``(select ...) AS aliasname``.

        The ``name`` parameter is optional, and provides the name
        to use in the rendered SQL.  If blank, an "anonymous" name
        will be deterministically generated at compile time.
        Deterministic means the name is guaranteed to be unique against
        other constructs used in the same statement, and will also be the
        same name for each successive compilation of the same statement
        object.

        :param selectable: any :class:`.FromClause` subclass,
            such as a table, select statement, etc.

        :param name: string name to be assigned as the alias.
            If ``None``, a name will be deterministically generated
            at compile time.

        :param flat: Will be passed through to if the given selectable
         is an instance of :class:`.Join` - see :meth:`.Join.alias`
         for details.

         .. versionadded:: 0.9.0

        """
        return _interpret_as_from(selectable).alias(name=name, flat=flat)

    def _init(self, selectable, name=None):
        baseselectable = selectable
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.element
        self.original = baseselectable
        self.supports_execution = baseselectable.supports_execution
        if self.supports_execution:
            self._execution_options = baseselectable._execution_options
        self.element = selectable
        if name is None:
            if self.original.named_with_column:
                name = getattr(self.original, "name", None)
            name = _anonymous_label("%%(%d %s)s" % (id(self), name or "anon"))
        self.name = name

    def self_group(self, against=None):
        if (
            isinstance(against, CompoundSelect)
            and isinstance(self.original, Select)
            and self.original._needs_parens_for_grouping()
        ):
            return FromGrouping(self)

        return super(Alias, self).self_group(against=against)

    @property
    def description(self):
        if util.py3k:
            return self.name
        else:
            return self.name.encode("ascii", "backslashreplace")

    def as_scalar(self):
        try:
            return self.element.as_scalar()
        except AttributeError:
            raise AttributeError(
                "Element %s does not support " "'as_scalar()'" % self.element
            )

    def is_derived_from(self, fromclause):
        if fromclause in self._cloned_set:
            return True
        return self.element.is_derived_from(fromclause)

    def _populate_column_collection(self):
        for col in self.element.columns._all_columns:
            col._make_proxy(self)

    def _refresh_for_new_column(self, column):
        col = self.element._refresh_for_new_column(column)
        if col is not None:
            if not self._cols_populated:
                return None
            else:
                return col._make_proxy(self)
        else:
            return None

    def _copy_internals(self, clone=_clone, **kw):
        # don't apply anything to an aliased Table
        # for now.   May want to drive this from
        # the given **kw.
        if isinstance(self.element, TableClause):
            return
        self._reset_exported()
        self.element = clone(self.element, **kw)
        baseselectable = self.element
        while isinstance(baseselectable, Alias):
            baseselectable = baseselectable.element
        self.original = baseselectable

    def get_children(self, column_collections=True, **kw):
        if column_collections:
            for c in self.c:
                yield c
        yield self.element

    @property
    def _from_objects(self):
        return [self]

    @property
    def bind(self):
        return self.element.bind


class Lateral(Alias):
    """Represent a LATERAL subquery.

    This object is constructed from the :func:`~.expression.lateral` module
    level function as well as the :meth:`.FromClause.lateral` method available
    on all :class:`.FromClause` subclasses.

    While LATERAL is part of the SQL standard, currently only more recent
    PostgreSQL versions provide support for this keyword.

    .. versionadded:: 1.1

    .. seealso::

        :ref:`lateral_selects` -  overview of usage.

    """

    __visit_name__ = "lateral"
    _is_lateral = True

    @classmethod
    def _factory(cls, selectable, name=None):
        """Return a :class:`.Lateral` object.

        :class:`.Lateral` is an :class:`.Alias` subclass that represents
        a subquery with the LATERAL keyword applied to it.

        The special behavior of a LATERAL subquery is that it appears in the
        FROM clause of an enclosing SELECT, but may correlate to other
        FROM clauses of that SELECT.   It is a special case of subquery
        only supported by a small number of backends, currently more recent
        PostgreSQL versions.

        .. versionadded:: 1.1

        .. seealso::

            :ref:`lateral_selects` -  overview of usage.

        """
        return _interpret_as_from(selectable).lateral(name=name)


class TableSample(Alias):
    """Represent a TABLESAMPLE clause.

    This object is constructed from the :func:`~.expression.tablesample` module
    level function as well as the :meth:`.FromClause.tablesample` method
    available on all :class:`.FromClause` subclasses.

    .. versionadded:: 1.1

    .. seealso::

        :func:`~.expression.tablesample`

    """

    __visit_name__ = "tablesample"

    @classmethod
    def _factory(cls, selectable, sampling, name=None, seed=None):
        """Return a :class:`.TableSample` object.

        :class:`.TableSample` is an :class:`.Alias` subclass that represents
        a table with the TABLESAMPLE clause applied to it.
        :func:`~.expression.tablesample`
        is also available from the :class:`.FromClause` class via the
        :meth:`.FromClause.tablesample` method.

        The TABLESAMPLE clause allows selecting a randomly selected approximate
        percentage of rows from a table. It supports multiple sampling methods,
        most commonly BERNOULLI and SYSTEM.

        e.g.::

            from sqlalchemy import func

            selectable = people.tablesample(
                        func.bernoulli(1),
                        name='alias',
                        seed=func.random())
            stmt = select([selectable.c.people_id])

        Assuming ``people`` with a column ``people_id``, the above
        statement would render as::

            SELECT alias.people_id FROM
            people AS alias TABLESAMPLE bernoulli(:bernoulli_1)
            REPEATABLE (random())

        .. versionadded:: 1.1

        :param sampling: a ``float`` percentage between 0 and 100 or
            :class:`.functions.Function`.

        :param name: optional alias name

        :param seed: any real-valued SQL expression.  When specified, the
         REPEATABLE sub-clause is also rendered.

        """
        return _interpret_as_from(selectable).tablesample(
            sampling, name=name, seed=seed
        )

    def _init(self, selectable, sampling, name=None, seed=None):
        self.sampling = sampling
        self.seed = seed
        super(TableSample, self)._init(selectable, name=name)

    @util.dependencies("sqlalchemy.sql.functions")
    def _get_method(self, functions):
        if isinstance(self.sampling, functions.Function):
            return self.sampling
        else:
            return functions.func.system(self.sampling)


class CTE(Generative, HasSuffixes, Alias):
    """Represent a Common Table Expression.

    The :class:`.CTE` object is obtained using the
    :meth:`.SelectBase.cte` method from any selectable.
    See that method for complete examples.

    """

    __visit_name__ = "cte"

    @classmethod
    def _factory(cls, selectable, name=None, recursive=False):
        r"""Return a new :class:`.CTE`, or Common Table Expression instance.

        Please see :meth:`.HasCte.cte` for detail on CTE usage.

        """
        return _interpret_as_from(selectable).cte(
            name=name, recursive=recursive
        )

    def _init(
        self,
        selectable,
        name=None,
        recursive=False,
        _cte_alias=None,
        _restates=frozenset(),
        _suffixes=None,
    ):
        self.recursive = recursive
        self._cte_alias = _cte_alias
        self._restates = _restates
        if _suffixes:
            self._suffixes = _suffixes
        super(CTE, self)._init(selectable, name=name)

    def _copy_internals(self, clone=_clone, **kw):
        super(CTE, self)._copy_internals(clone, **kw)
        if self._cte_alias is not None:
            self._cte_alias = clone(self._cte_alias, **kw)
        self._restates = frozenset(
            [clone(elem, **kw) for elem in self._restates]
        )

    @util.dependencies("sqlalchemy.sql.dml")
    def _populate_column_collection(self, dml):
        if isinstance(self.element, dml.UpdateBase):
            for col in self.element._returning:
                col._make_proxy(self)
        else:
            for col in self.element.columns._all_columns:
                col._make_proxy(self)

    def alias(self, name=None, flat=False):
        return CTE._construct(
            self.original,
            name=name,
            recursive=self.recursive,
            _cte_alias=self,
            _suffixes=self._suffixes,
        )

    def union(self, other):
        return CTE._construct(
            self.original.union(other),
            name=self.name,
            recursive=self.recursive,
            _restates=self._restates.union([self]),
            _suffixes=self._suffixes,
        )

    def union_all(self, other):
        return CTE._construct(
            self.original.union_all(other),
            name=self.name,
            recursive=self.recursive,
            _restates=self._restates.union([self]),
            _suffixes=self._suffixes,
        )


class HasCTE(object):
    """Mixin that declares a class to include CTE support.

    .. versionadded:: 1.1

    """

    def cte(self, name=None, recursive=False):
        r"""Return a new :class:`.CTE`, or Common Table Expression instance.

        Common table expressions are a SQL standard whereby SELECT
        statements can draw upon secondary statements specified along
        with the primary statement, using a clause called "WITH".
        Special semantics regarding UNION can also be employed to
        allow "recursive" queries, where a SELECT statement can draw
        upon the set of rows that have previously been selected.

        CTEs can also be applied to DML constructs UPDATE, INSERT
        and DELETE on some databases, both as a source of CTE rows
        when combined with RETURNING, as well as a consumer of
        CTE rows.

        SQLAlchemy detects :class:`.CTE` objects, which are treated
        similarly to :class:`.Alias` objects, as special elements
        to be delivered to the FROM clause of the statement as well
        as to a WITH clause at the top of the statement.

        .. versionchanged:: 1.1 Added support for UPDATE/INSERT/DELETE as
           CTE, CTEs added to UPDATE/INSERT/DELETE.

        :param name: name given to the common table expression.  Like
         :meth:`._FromClause.alias`, the name can be left as ``None``
         in which case an anonymous symbol will be used at query
         compile time.
        :param recursive: if ``True``, will render ``WITH RECURSIVE``.
         A recursive common table expression is intended to be used in
         conjunction with UNION ALL in order to derive rows
         from those already selected.

        The following examples include two from PostgreSQL's documentation at
        http://www.postgresql.org/docs/current/static/queries-with.html,
        as well as additional examples.

        Example 1, non recursive::

            from sqlalchemy import (Table, Column, String, Integer,
                                    MetaData, select, func)

            metadata = MetaData()

            orders = Table('orders', metadata,
                Column('region', String),
                Column('amount', Integer),
                Column('product', String),
                Column('quantity', Integer)
            )

            regional_sales = select([
                                orders.c.region,
                                func.sum(orders.c.amount).label('total_sales')
                            ]).group_by(orders.c.region).cte("regional_sales")


            top_regions = select([regional_sales.c.region]).\
                    where(
                        regional_sales.c.total_sales >
                        select([
                            func.sum(regional_sales.c.total_sales)/10
                        ])
                    ).cte("top_regions")

            statement = select([
                        orders.c.region,
                        orders.c.product,
                        func.sum(orders.c.quantity).label("product_units"),
                        func.sum(orders.c.amount).label("product_sales")
                ]).where(orders.c.region.in_(
                    select([top_regions.c.region])
                )).group_by(orders.c.region, orders.c.product)

            result = conn.execute(statement).fetchall()

        Example 2, WITH RECURSIVE::

            from sqlalchemy import (Table, Column, String, Integer,
                                    MetaData, select, func)

            metadata = MetaData()

            parts = Table('parts', metadata,
                Column('part', String),
                Column('sub_part', String),
                Column('quantity', Integer),
            )

            included_parts = select([
                                parts.c.sub_part,
                                parts.c.part,
                                parts.c.quantity]).\
                                where(parts.c.part=='our part').\
                                cte(recursive=True)


            incl_alias = included_parts.alias()
            parts_alias = parts.alias()
            included_parts = included_parts.union_all(
                select([
                    parts_alias.c.sub_part,
                    parts_alias.c.part,
                    parts_alias.c.quantity
                ]).
                    where(parts_alias.c.part==incl_alias.c.sub_part)
            )

            statement = select([
                        included_parts.c.sub_part,
                        func.sum(included_parts.c.quantity).
                          label('total_quantity')
                    ]).\
                    group_by(included_parts.c.sub_part)

            result = conn.execute(statement).fetchall()

        Example 3, an upsert using UPDATE and INSERT with CTEs::

            from datetime import date
            from sqlalchemy import (MetaData, Table, Column, Integer,
                                    Date, select, literal, and_, exists)

            metadata = MetaData()

            visitors = Table('visitors', metadata,
                Column('product_id', Integer, primary_key=True),
                Column('date', Date, primary_key=True),
                Column('count', Integer),
            )

            # add 5 visitors for the product_id == 1
            product_id = 1
            day = date.today()
            count = 5

            update_cte = (
                visitors.update()
                .where(and_(visitors.c.product_id == product_id,
                            visitors.c.date == day))
                .values(count=visitors.c.count + count)
                .returning(literal(1))
                .cte('update_cte')
            )

            upsert = visitors.insert().from_select(
                [visitors.c.product_id, visitors.c.date, visitors.c.count],
                select([literal(product_id), literal(day), literal(count)])
                    .where(~exists(update_cte.select()))
            )

            connection.execute(upsert)

        .. seealso::

            :meth:`.orm.query.Query.cte` - ORM version of
            :meth:`.HasCTE.cte`.

        """
        return CTE._construct(self, name=name, recursive=recursive)


class FromGrouping(FromClause):
    """Represent a grouping of a FROM clause"""

    __visit_name__ = "grouping"

    def __init__(self, element):
        self.element = element

    def _init_collections(self):
        pass

    @property
    def columns(self):
        return self.element.columns

    @property
    def primary_key(self):
        return self.element.primary_key

    @property
    def foreign_keys(self):
        return self.element.foreign_keys

    def is_derived_from(self, element):
        return self.element.is_derived_from(element)

    def alias(self, **kw):
        return FromGrouping(self.element.alias(**kw))

    @property
    def _hide_froms(self):
        return self.element._hide_froms

    def get_children(self, **kwargs):
        return (self.element,)

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    @property
    def _from_objects(self):
        return self.element._from_objects

    def __getattr__(self, attr):
        return getattr(self.element, attr)

    def __getstate__(self):
        return {"element": self.element}

    def __setstate__(self, state):
        self.element = state["element"]


class TableClause(Immutable, FromClause):
    """Represents a minimal "table" construct.

    This is a lightweight table object that has only a name and a
    collection of columns, which are typically produced
    by the :func:`.expression.column` function::

        from sqlalchemy import table, column

        user = table("user",
                column("id"),
                column("name"),
                column("description"),
        )

    The :class:`.TableClause` construct serves as the base for
    the more commonly used :class:`~.schema.Table` object, providing
    the usual set of :class:`~.expression.FromClause` services including
    the ``.c.`` collection and statement generation methods.

    It does **not** provide all the additional schema-level services
    of :class:`~.schema.Table`, including constraints, references to other
    tables, or support for :class:`.MetaData`-level services.  It's useful
    on its own as an ad-hoc construct used to generate quick SQL
    statements when a more fully fledged :class:`~.schema.Table`
    is not on hand.

    """

    __visit_name__ = "table"

    named_with_column = True

    implicit_returning = False
    """:class:`.TableClause` doesn't support having a primary key or column
    -level defaults, so implicit returning doesn't apply."""

    _autoincrement_column = None
    """No PK or default support so no autoincrement column."""

    def __init__(self, name, *columns):
        """Produce a new :class:`.TableClause`.

        The object returned is an instance of :class:`.TableClause`, which
        represents the "syntactical" portion of the schema-level
        :class:`~.schema.Table` object.
        It may be used to construct lightweight table constructs.

        .. versionchanged:: 1.0.0 :func:`.expression.table` can now
           be imported from the plain ``sqlalchemy`` namespace like any
           other SQL element.

        :param name: Name of the table.

        :param columns: A collection of :func:`.expression.column` constructs.

        """

        super(TableClause, self).__init__()
        self.name = self.fullname = name
        self._columns = ColumnCollection()
        self.primary_key = ColumnSet()
        self.foreign_keys = set()
        for c in columns:
            self.append_column(c)

    def _init_collections(self):
        pass

    @util.memoized_property
    def description(self):
        if util.py3k:
            return self.name
        else:
            return self.name.encode("ascii", "backslashreplace")

    def append_column(self, c):
        self._columns[c.key] = c
        c.table = self

    def get_children(self, column_collections=True, **kwargs):
        if column_collections:
            return [c for c in self.c]
        else:
            return []

    @util.dependencies("sqlalchemy.sql.dml")
    def insert(self, dml, values=None, inline=False, **kwargs):
        """Generate an :func:`.insert` construct against this
        :class:`.TableClause`.

        E.g.::

            table.insert().values(name='foo')

        See :func:`.insert` for argument and usage information.

        """

        return dml.Insert(self, values=values, inline=inline, **kwargs)

    @util.dependencies("sqlalchemy.sql.dml")
    def update(
        self, dml, whereclause=None, values=None, inline=False, **kwargs
    ):
        """Generate an :func:`.update` construct against this
        :class:`.TableClause`.

        E.g.::

            table.update().where(table.c.id==7).values(name='foo')

        See :func:`.update` for argument and usage information.

        """

        return dml.Update(
            self,
            whereclause=whereclause,
            values=values,
            inline=inline,
            **kwargs
        )

    @util.dependencies("sqlalchemy.sql.dml")
    def delete(self, dml, whereclause=None, **kwargs):
        """Generate a :func:`.delete` construct against this
        :class:`.TableClause`.

        E.g.::

            table.delete().where(table.c.id==7)

        See :func:`.delete` for argument and usage information.

        """

        return dml.Delete(self, whereclause, **kwargs)

    @property
    def _from_objects(self):
        return [self]


class ForUpdateArg(ClauseElement):
    @classmethod
    def parse_legacy_select(self, arg):
        """Parse the for_update argument of :func:`.select`.

        :param mode: Defines the lockmode to use.

            ``None`` - translates to no lockmode

            ``'update'`` - translates to ``FOR UPDATE``
            (standard SQL, supported by most dialects)

            ``'nowait'`` - translates to ``FOR UPDATE NOWAIT``
            (supported by Oracle, PostgreSQL 8.1 upwards)

            ``'read'`` - translates to ``LOCK IN SHARE MODE`` (for MySQL),
            and ``FOR SHARE`` (for PostgreSQL)

            ``'read_nowait'`` - translates to ``FOR SHARE NOWAIT``
            (supported by PostgreSQL). ``FOR SHARE`` and
            ``FOR SHARE NOWAIT`` (PostgreSQL).

        """
        if arg in (None, False):
            return None

        nowait = read = False
        if arg == "nowait":
            nowait = True
        elif arg == "read":
            read = True
        elif arg == "read_nowait":
            read = nowait = True
        elif arg is not True:
            raise exc.ArgumentError("Unknown for_update argument: %r" % arg)

        return ForUpdateArg(read=read, nowait=nowait)

    @property
    def legacy_for_update_value(self):
        if self.read and not self.nowait:
            return "read"
        elif self.read and self.nowait:
            return "read_nowait"
        elif self.nowait:
            return "nowait"
        else:
            return True

    def __eq__(self, other):
        return (
            isinstance(other, ForUpdateArg)
            and other.nowait == self.nowait
            and other.read == self.read
            and other.skip_locked == self.skip_locked
            and other.key_share == self.key_share
            and other.of is self.of
        )

    def __hash__(self):
        return id(self)

    def _copy_internals(self, clone=_clone, **kw):
        if self.of is not None:
            self.of = [clone(col, **kw) for col in self.of]

    def __init__(
        self,
        nowait=False,
        read=False,
        of=None,
        skip_locked=False,
        key_share=False,
    ):
        """Represents arguments specified to :meth:`.Select.for_update`.

        .. versionadded:: 0.9.0

        """

        self.nowait = nowait
        self.read = read
        self.skip_locked = skip_locked
        self.key_share = key_share
        if of is not None:
            self.of = [
                _interpret_as_column_or_from(elem) for elem in util.to_list(of)
            ]
        else:
            self.of = None


class SelectBase(HasCTE, Executable, FromClause):
    """Base class for SELECT statements.


    This includes :class:`.Select`, :class:`.CompoundSelect` and
    :class:`.TextAsFrom`.


    """

    def as_scalar(self):
        """return a 'scalar' representation of this selectable, which can be
        used as a column expression.

        Typically, a select statement which has only one column in its columns
        clause is eligible to be used as a scalar expression.

        The returned object is an instance of
        :class:`ScalarSelect`.

        """
        return ScalarSelect(self)

    def label(self, name):
        """return a 'scalar' representation of this selectable, embedded as a
        subquery with a label.

        .. seealso::

            :meth:`~.SelectBase.as_scalar`.

        """
        return self.as_scalar().label(name)

    @_generative
    @util.deprecated(
        "0.6",
        message="The :meth:`.SelectBase.autocommit` method is deprecated, "
        "and will be removed in a future release.   Please use the "
        "the :paramref:`.Connection.execution_options.autocommit` "
        "parameter in conjunction with the "
        ":meth:`.Executable.execution_options` method.",
    )
    def autocommit(self):
        """return a new selectable with the 'autocommit' flag set to
        True.
        """

        self._execution_options = self._execution_options.union(
            {"autocommit": True}
        )

    def _generate(self):
        """Override the default _generate() method to also clear out
        exported collections."""

        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        s._reset_exported()
        return s

    @property
    def _from_objects(self):
        return [self]


class GenerativeSelect(SelectBase):
    """Base class for SELECT statements where additional elements can be
    added.

    This serves as the base for :class:`.Select` and :class:`.CompoundSelect`
    where elements such as ORDER BY, GROUP BY can be added and column
    rendering can be controlled.  Compare to :class:`.TextAsFrom`, which,
    while it subclasses :class:`.SelectBase` and is also a SELECT construct,
    represents a fixed textual string which cannot be altered at this level,
    only wrapped as a subquery.

    .. versionadded:: 0.9.0 :class:`.GenerativeSelect` was added to
       provide functionality specific to :class:`.Select` and
       :class:`.CompoundSelect` while allowing :class:`.SelectBase` to be
       used for other SELECT-like objects, e.g. :class:`.TextAsFrom`.

    """

    _order_by_clause = ClauseList()
    _group_by_clause = ClauseList()
    _limit_clause = None
    _offset_clause = None
    _for_update_arg = None

    def __init__(
        self,
        use_labels=False,
        for_update=False,
        limit=None,
        offset=None,
        order_by=None,
        group_by=None,
        bind=None,
        autocommit=None,
    ):
        self.use_labels = use_labels

        if for_update is not False:
            self._for_update_arg = ForUpdateArg.parse_legacy_select(for_update)

        if autocommit is not None:
            util.warn_deprecated(
                "The select.autocommit parameter is deprecated and will be "
                "removed in a future release.  Please refer to the "
                "Select.execution_options.autocommit` parameter."
            )
            self._execution_options = self._execution_options.union(
                {"autocommit": autocommit}
            )
        if limit is not None:
            self._limit_clause = _offset_or_limit_clause(limit)
        if offset is not None:
            self._offset_clause = _offset_or_limit_clause(offset)
        self._bind = bind

        if order_by is not None:
            self._order_by_clause = ClauseList(
                *util.to_list(order_by),
                _literal_as_text=_literal_and_labels_as_label_reference
            )
        if group_by is not None:
            self._group_by_clause = ClauseList(
                *util.to_list(group_by),
                _literal_as_text=_literal_as_label_reference
            )

    @property
    def for_update(self):
        """Provide legacy dialect support for the ``for_update`` attribute.
        """
        if self._for_update_arg is not None:
            return self._for_update_arg.legacy_for_update_value
        else:
            return None

    @for_update.setter
    def for_update(self, value):
        self._for_update_arg = ForUpdateArg.parse_legacy_select(value)

    @_generative
    def with_for_update(
        self,
        nowait=False,
        read=False,
        of=None,
        skip_locked=False,
        key_share=False,
    ):
        """Specify a ``FOR UPDATE`` clause for this :class:`.GenerativeSelect`.

        E.g.::

            stmt = select([table]).with_for_update(nowait=True)

        On a database like PostgreSQL or Oracle, the above would render a
        statement like::

            SELECT table.a, table.b FROM table FOR UPDATE NOWAIT

        on other backends, the ``nowait`` option is ignored and instead
        would produce::

            SELECT table.a, table.b FROM table FOR UPDATE

        When called with no arguments, the statement will render with
        the suffix ``FOR UPDATE``.   Additional arguments can then be
        provided which allow for common database-specific
        variants.

        :param nowait: boolean; will render ``FOR UPDATE NOWAIT`` on Oracle
         and PostgreSQL dialects.

        :param read: boolean; will render ``LOCK IN SHARE MODE`` on MySQL,
         ``FOR SHARE`` on PostgreSQL.  On PostgreSQL, when combined with
         ``nowait``, will render ``FOR SHARE NOWAIT``.

        :param of: SQL expression or list of SQL expression elements
         (typically :class:`.Column` objects or a compatible expression) which
         will render into a ``FOR UPDATE OF`` clause; supported by PostgreSQL
         and Oracle.  May render as a table or as a column depending on
         backend.

        :param skip_locked: boolean, will render ``FOR UPDATE SKIP LOCKED``
         on Oracle and PostgreSQL dialects or ``FOR SHARE SKIP LOCKED`` if
         ``read=True`` is also specified.

         .. versionadded:: 1.1.0

        :param key_share: boolean, will render ``FOR NO KEY UPDATE``,
         or if combined with ``read=True`` will render ``FOR KEY SHARE``,
         on the PostgreSQL dialect.

         .. versionadded:: 1.1.0

        """
        self._for_update_arg = ForUpdateArg(
            nowait=nowait,
            read=read,
            of=of,
            skip_locked=skip_locked,
            key_share=key_share,
        )

    @_generative
    def apply_labels(self):
        """return a new selectable with the 'use_labels' flag set to True.

        This will result in column expressions being generated using labels
        against their table name, such as "SELECT somecolumn AS
        tablename_somecolumn". This allows selectables which contain multiple
        FROM clauses to produce a unique set of column names regardless of
        name conflicts among the individual FROM clauses.

        """
        self.use_labels = True

    @property
    def _limit(self):
        """Get an integer value for the limit.  This should only be used
        by code that cannot support a limit as a BindParameter or
        other custom clause as it will throw an exception if the limit
        isn't currently set to an integer.

        """
        return _offset_or_limit_clause_asint(self._limit_clause, "limit")

    @property
    def _simple_int_limit(self):
        """True if the LIMIT clause is a simple integer, False
        if it is not present or is a SQL expression.
        """
        return isinstance(self._limit_clause, _OffsetLimitParam)

    @property
    def _simple_int_offset(self):
        """True if the OFFSET clause is a simple integer, False
        if it is not present or is a SQL expression.
        """
        return isinstance(self._offset_clause, _OffsetLimitParam)

    @property
    def _offset(self):
        """Get an integer value for the offset.  This should only be used
        by code that cannot support an offset as a BindParameter or
        other custom clause as it will throw an exception if the
        offset isn't currently set to an integer.

        """
        return _offset_or_limit_clause_asint(self._offset_clause, "offset")

    @_generative
    def limit(self, limit):
        """return a new selectable with the given LIMIT criterion
        applied.

        This is a numerical value which usually renders as a ``LIMIT``
        expression in the resulting select.  Backends that don't
        support ``LIMIT`` will attempt to provide similar
        functionality.

        .. versionchanged:: 1.0.0 - :meth:`.Select.limit` can now
           accept arbitrary SQL expressions as well as integer values.

        :param limit: an integer LIMIT parameter, or a SQL expression
         that provides an integer result.

        """

        self._limit_clause = _offset_or_limit_clause(limit)

    @_generative
    def offset(self, offset):
        """return a new selectable with the given OFFSET criterion
        applied.


        This is a numeric value which usually renders as an ``OFFSET``
        expression in the resulting select.  Backends that don't
        support ``OFFSET`` will attempt to provide similar
        functionality.


        .. versionchanged:: 1.0.0 - :meth:`.Select.offset` can now
           accept arbitrary SQL expressions as well as integer values.

        :param offset: an integer OFFSET parameter, or a SQL expression
         that provides an integer result.

        """

        self._offset_clause = _offset_or_limit_clause(offset)

    @_generative
    def order_by(self, *clauses):
        """return a new selectable with the given list of ORDER BY
        criterion applied.

        The criterion will be appended to any pre-existing ORDER BY
        criterion.

        """

        self.append_order_by(*clauses)

    @_generative
    def group_by(self, *clauses):
        """return a new selectable with the given list of GROUP BY
        criterion applied.

        The criterion will be appended to any pre-existing GROUP BY
        criterion.

        """

        self.append_group_by(*clauses)

    def append_order_by(self, *clauses):
        """Append the given ORDER BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing ORDER BY criterion.

        This is an **in-place** mutation method; the
        :meth:`~.GenerativeSelect.order_by` method is preferred, as it
        provides standard :term:`method chaining`.

        """
        if len(clauses) == 1 and clauses[0] is None:
            self._order_by_clause = ClauseList()
        else:
            if getattr(self, "_order_by_clause", None) is not None:
                clauses = list(self._order_by_clause) + list(clauses)
            self._order_by_clause = ClauseList(
                *clauses,
                _literal_as_text=_literal_and_labels_as_label_reference
            )

    def append_group_by(self, *clauses):
        """Append the given GROUP BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing GROUP BY criterion.

        This is an **in-place** mutation method; the
        :meth:`~.GenerativeSelect.group_by` method is preferred, as it
        provides standard :term:`method chaining`.

        """
        if len(clauses) == 1 and clauses[0] is None:
            self._group_by_clause = ClauseList()
        else:
            if getattr(self, "_group_by_clause", None) is not None:
                clauses = list(self._group_by_clause) + list(clauses)
            self._group_by_clause = ClauseList(
                *clauses, _literal_as_text=_literal_as_label_reference
            )

    @property
    def _label_resolve_dict(self):
        raise NotImplementedError()

    def _copy_internals(self, clone=_clone, **kw):
        if self._limit_clause is not None:
            self._limit_clause = clone(self._limit_clause, **kw)
        if self._offset_clause is not None:
            self._offset_clause = clone(self._offset_clause, **kw)


class CompoundSelect(GenerativeSelect):
    """Forms the basis of ``UNION``, ``UNION ALL``, and other
        SELECT-based set operations.


    .. seealso::

        :func:`.union`

        :func:`.union_all`

        :func:`.intersect`

        :func:`.intersect_all`

        :func:`.except`

        :func:`.except_all`

    """

    __visit_name__ = "compound_select"

    UNION = util.symbol("UNION")
    UNION_ALL = util.symbol("UNION ALL")
    EXCEPT = util.symbol("EXCEPT")
    EXCEPT_ALL = util.symbol("EXCEPT ALL")
    INTERSECT = util.symbol("INTERSECT")
    INTERSECT_ALL = util.symbol("INTERSECT ALL")

    _is_from_container = True

    def __init__(self, keyword, *selects, **kwargs):
        self._auto_correlate = kwargs.pop("correlate", False)
        self.keyword = keyword
        self.selects = []

        numcols = None

        # some DBs do not like ORDER BY in the inner queries of a UNION, etc.
        for n, s in enumerate(selects):
            s = _clause_element_as_expr(s)

            if not numcols:
                numcols = len(s.c._all_columns)
            elif len(s.c._all_columns) != numcols:
                raise exc.ArgumentError(
                    "All selectables passed to "
                    "CompoundSelect must have identical numbers of "
                    "columns; select #%d has %d columns, select "
                    "#%d has %d"
                    % (
                        1,
                        len(self.selects[0].c._all_columns),
                        n + 1,
                        len(s.c._all_columns),
                    )
                )

            self.selects.append(s.self_group(against=self))

        GenerativeSelect.__init__(self, **kwargs)

    @property
    def _label_resolve_dict(self):
        d = dict((c.key, c) for c in self.c)
        return d, d, d

    @classmethod
    def _create_union(cls, *selects, **kwargs):
        r"""Return a ``UNION`` of multiple selectables.

        The returned object is an instance of
        :class:`.CompoundSelect`.

        A similar :func:`union()` method is available on all
        :class:`.FromClause` subclasses.

        \*selects
          a list of :class:`.Select` instances.

        \**kwargs
           available keyword arguments are the same as those of
           :func:`select`.

        """
        return CompoundSelect(CompoundSelect.UNION, *selects, **kwargs)

    @classmethod
    def _create_union_all(cls, *selects, **kwargs):
        r"""Return a ``UNION ALL`` of multiple selectables.

        The returned object is an instance of
        :class:`.CompoundSelect`.

        A similar :func:`union_all()` method is available on all
        :class:`.FromClause` subclasses.

        \*selects
          a list of :class:`.Select` instances.

        \**kwargs
          available keyword arguments are the same as those of
          :func:`select`.

        """
        return CompoundSelect(CompoundSelect.UNION_ALL, *selects, **kwargs)

    @classmethod
    def _create_except(cls, *selects, **kwargs):
        r"""Return an ``EXCEPT`` of multiple selectables.

        The returned object is an instance of
        :class:`.CompoundSelect`.

        \*selects
          a list of :class:`.Select` instances.

        \**kwargs
          available keyword arguments are the same as those of
          :func:`select`.

        """
        return CompoundSelect(CompoundSelect.EXCEPT, *selects, **kwargs)

    @classmethod
    def _create_except_all(cls, *selects, **kwargs):
        r"""Return an ``EXCEPT ALL`` of multiple selectables.

        The returned object is an instance of
        :class:`.CompoundSelect`.

        \*selects
          a list of :class:`.Select` instances.

        \**kwargs
          available keyword arguments are the same as those of
          :func:`select`.

        """
        return CompoundSelect(CompoundSelect.EXCEPT_ALL, *selects, **kwargs)

    @classmethod
    def _create_intersect(cls, *selects, **kwargs):
        r"""Return an ``INTERSECT`` of multiple selectables.

        The returned object is an instance of
        :class:`.CompoundSelect`.

        \*selects
          a list of :class:`.Select` instances.

        \**kwargs
          available keyword arguments are the same as those of
          :func:`select`.

        """
        return CompoundSelect(CompoundSelect.INTERSECT, *selects, **kwargs)

    @classmethod
    def _create_intersect_all(cls, *selects, **kwargs):
        r"""Return an ``INTERSECT ALL`` of multiple selectables.

        The returned object is an instance of
        :class:`.CompoundSelect`.

        \*selects
          a list of :class:`.Select` instances.

        \**kwargs
          available keyword arguments are the same as those of
          :func:`select`.

        """
        return CompoundSelect(CompoundSelect.INTERSECT_ALL, *selects, **kwargs)

    def _scalar_type(self):
        return self.selects[0]._scalar_type()

    def self_group(self, against=None):
        return FromGrouping(self)

    def is_derived_from(self, fromclause):
        for s in self.selects:
            if s.is_derived_from(fromclause):
                return True
        return False

    def _populate_column_collection(self):
        for cols in zip(*[s.c._all_columns for s in self.selects]):

            # this is a slightly hacky thing - the union exports a
            # column that resembles just that of the *first* selectable.
            # to get at a "composite" column, particularly foreign keys,
            # you have to dig through the proxies collection which we
            # generate below.  We may want to improve upon this, such as
            # perhaps _make_proxy can accept a list of other columns
            # that are "shared" - schema.column can then copy all the
            # ForeignKeys in. this would allow the union() to have all
            # those fks too.

            proxy = cols[0]._make_proxy(
                self,
                name=cols[0]._label if self.use_labels else None,
                key=cols[0]._key_label if self.use_labels else None,
            )

            # hand-construct the "_proxies" collection to include all
            # derived columns place a 'weight' annotation corresponding
            # to how low in the list of select()s the column occurs, so
            # that the corresponding_column() operation can resolve
            # conflicts

            proxy._proxies = [
                c._annotate({"weight": i + 1}) for (i, c) in enumerate(cols)
            ]

    def _refresh_for_new_column(self, column):
        for s in self.selects:
            s._refresh_for_new_column(column)

        if not self._cols_populated:
            return None

        raise NotImplementedError(
            "CompoundSelect constructs don't support "
            "addition of columns to underlying "
            "selectables"
        )

    def _copy_internals(self, clone=_clone, **kw):
        super(CompoundSelect, self)._copy_internals(clone, **kw)
        self._reset_exported()
        self.selects = [clone(s, **kw) for s in self.selects]
        if hasattr(self, "_col_map"):
            del self._col_map
        for attr in (
            "_order_by_clause",
            "_group_by_clause",
            "_for_update_arg",
        ):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr), **kw))

    def get_children(self, column_collections=True, **kwargs):
        return (
            (column_collections and list(self.c) or [])
            + [self._order_by_clause, self._group_by_clause]
            + list(self.selects)
        )

    def bind(self):
        if self._bind:
            return self._bind
        for s in self.selects:
            e = s.bind
            if e:
                return e
        else:
            return None

    def _set_bind(self, bind):
        self._bind = bind

    bind = property(bind, _set_bind)


class Select(HasPrefixes, HasSuffixes, GenerativeSelect):
    """Represents a ``SELECT`` statement.

    """

    __visit_name__ = "select"

    _prefixes = ()
    _suffixes = ()
    _hints = util.immutabledict()
    _statement_hints = ()
    _distinct = False
    _from_cloned = None
    _correlate = ()
    _correlate_except = None
    _memoized_property = SelectBase._memoized_property
    _is_select = True

    @util.deprecated_params(
        autocommit=(
            "0.6",
            "The :paramref:`.select.autocommit` parameter is deprecated "
            "and will be removed in a future release.  Please refer to "
            "the :paramref:`.Connection.execution_options.autocommit` "
            "parameter in conjunction with the the "
            ":meth:`.Executable.execution_options` method in order to "
            "affect the autocommit behavior for a statement.",
        ),
        for_update=(
            "0.9",
            "The :paramref:`.select.for_update` parameter is deprecated and "
            "will be removed in a future release.  Please refer to the "
            ":meth:`.Select.with_for_update` to specify the "
            "structure of the ``FOR UPDATE`` clause.",
        ),
    )
    def __init__(
        self,
        columns=None,
        whereclause=None,
        from_obj=None,
        distinct=False,
        having=None,
        correlate=True,
        prefixes=None,
        suffixes=None,
        **kwargs
    ):
        """Construct a new :class:`.Select`.

        Similar functionality is also available via the
        :meth:`.FromClause.select` method on any :class:`.FromClause`.

        All arguments which accept :class:`.ClauseElement` arguments also
        accept string arguments, which will be converted as appropriate into
        either :func:`text()` or :func:`literal_column()` constructs.

        .. seealso::

            :ref:`coretutorial_selecting` - Core Tutorial description of
            :func:`.select`.

        :param columns:
          A list of :class:`.ColumnElement` or :class:`.FromClause`
          objects which will form the columns clause of the resulting
          statement.   For those objects that are instances of
          :class:`.FromClause` (typically :class:`.Table` or :class:`.Alias`
          objects), the :attr:`.FromClause.c` collection is extracted
          to form a collection of :class:`.ColumnElement` objects.

          This parameter will also accept :class:`.Text` constructs as
          given, as well as ORM-mapped classes.

          .. note::

            The :paramref:`.select.columns` parameter is not available
            in the method form of :func:`.select`, e.g.
            :meth:`.FromClause.select`.

          .. seealso::

            :meth:`.Select.column`

            :meth:`.Select.with_only_columns`

        :param whereclause:
          A :class:`.ClauseElement` expression which will be used to form the
          ``WHERE`` clause.   It is typically preferable to add WHERE
          criterion to an existing :class:`.Select` using method chaining
          with :meth:`.Select.where`.

          .. seealso::

            :meth:`.Select.where`

        :param from_obj:
          A list of :class:`.ClauseElement` objects which will be added to the
          ``FROM`` clause of the resulting statement.  This is equivalent
          to calling :meth:`.Select.select_from` using method chaining on
          an existing :class:`.Select` object.

          .. seealso::

            :meth:`.Select.select_from` - full description of explicit
            FROM clause specification.

        :param autocommit: legacy autocommit parameter.

        :param bind=None:
          an :class:`~.Engine` or :class:`~.Connection` instance
          to which the
          resulting :class:`.Select` object will be bound.  The
          :class:`.Select` object will otherwise automatically bind to
          whatever :class:`~.base.Connectable` instances can be located within
          its contained :class:`.ClauseElement` members.

        :param correlate=True:
          indicates that this :class:`.Select` object should have its
          contained :class:`.FromClause` elements "correlated" to an enclosing
          :class:`.Select` object.  It is typically preferable to specify
          correlations on an existing :class:`.Select` construct using
          :meth:`.Select.correlate`.

          .. seealso::

            :meth:`.Select.correlate` - full description of correlation.

        :param distinct=False:
          when ``True``, applies a ``DISTINCT`` qualifier to the columns
          clause of the resulting statement.

          The boolean argument may also be a column expression or list
          of column expressions - this is a special calling form which
          is understood by the PostgreSQL dialect to render the
          ``DISTINCT ON (<columns>)`` syntax.

          ``distinct`` is also available on an existing :class:`.Select`
          object via the :meth:`~.Select.distinct` method.

          .. seealso::

            :meth:`.Select.distinct`

        :param for_update=False:
          when ``True``, applies ``FOR UPDATE`` to the end of the
          resulting statement.

          ``for_update`` accepts various string values interpreted by
          specific backends, including:

          * ``"read"`` - on MySQL, translates to ``LOCK IN SHARE MODE``;
            on PostgreSQL, translates to ``FOR SHARE``.
          * ``"nowait"`` - on PostgreSQL and Oracle, translates to
            ``FOR UPDATE NOWAIT``.
          * ``"read_nowait"`` - on PostgreSQL, translates to
            ``FOR SHARE NOWAIT``.

         .. seealso::

            :meth:`.Select.with_for_update` - improved API for
            specifying the ``FOR UPDATE`` clause.

        :param group_by:
          a list of :class:`.ClauseElement` objects which will comprise the
          ``GROUP BY`` clause of the resulting select.  This parameter
          is typically specified more naturally using the
          :meth:`.Select.group_by` method on an existing :class:`.Select`.

          .. seealso::

            :meth:`.Select.group_by`

        :param having:
          a :class:`.ClauseElement` that will comprise the ``HAVING`` clause
          of the resulting select when ``GROUP BY`` is used.  This parameter
          is typically specified more naturally using the
          :meth:`.Select.having` method on an existing :class:`.Select`.

          .. seealso::

            :meth:`.Select.having`

        :param limit=None:
          a numerical value which usually renders as a ``LIMIT``
          expression in the resulting select.  Backends that don't
          support ``LIMIT`` will attempt to provide similar
          functionality.    This parameter is typically specified more
          naturally using the :meth:`.Select.limit` method on an existing
          :class:`.Select`.

          .. seealso::

            :meth:`.Select.limit`

        :param offset=None:
          a numeric value which usually renders as an ``OFFSET``
          expression in the resulting select.  Backends that don't
          support ``OFFSET`` will attempt to provide similar
          functionality.  This parameter is typically specified more naturally
          using the :meth:`.Select.offset` method on an existing
          :class:`.Select`.

          .. seealso::

            :meth:`.Select.offset`

        :param order_by:
          a scalar or list of :class:`.ClauseElement` objects which will
          comprise the ``ORDER BY`` clause of the resulting select.
          This parameter is typically specified more naturally using the
          :meth:`.Select.order_by` method on an existing :class:`.Select`.

          .. seealso::

            :meth:`.Select.order_by`

        :param use_labels=False:
          when ``True``, the statement will be generated using labels
          for each column in the columns clause, which qualify each
          column with its parent table's (or aliases) name so that name
          conflicts between columns in different tables don't occur.
          The format of the label is <tablename>_<column>.  The "c"
          collection of the resulting :class:`.Select` object will use these
          names as well for targeting column members.

          This parameter can also be specified on an existing
          :class:`.Select` object using the :meth:`.Select.apply_labels`
          method.

          .. seealso::

            :meth:`.Select.apply_labels`

        """
        self._auto_correlate = correlate
        if distinct is not False:
            if distinct is True:
                self._distinct = True
            else:
                self._distinct = [
                    _literal_as_text(e) for e in util.to_list(distinct)
                ]

        if from_obj is not None:
            self._from_obj = util.OrderedSet(
                _interpret_as_from(f) for f in util.to_list(from_obj)
            )
        else:
            self._from_obj = util.OrderedSet()

        try:
            cols_present = bool(columns)
        except TypeError:
            raise exc.ArgumentError(
                "columns argument to select() must "
                "be a Python list or other iterable"
            )

        if cols_present:
            self._raw_columns = []
            for c in columns:
                c = _interpret_as_column_or_from(c)
                if isinstance(c, ScalarSelect):
                    c = c.self_group(against=operators.comma_op)
                self._raw_columns.append(c)
        else:
            self._raw_columns = []

        if whereclause is not None:
            self._whereclause = _literal_as_text(whereclause).self_group(
                against=operators._asbool
            )
        else:
            self._whereclause = None

        if having is not None:
            self._having = _literal_as_text(having).self_group(
                against=operators._asbool
            )
        else:
            self._having = None

        if prefixes:
            self._setup_prefixes(prefixes)

        if suffixes:
            self._setup_suffixes(suffixes)

        GenerativeSelect.__init__(self, **kwargs)

    @property
    def _froms(self):
        # would love to cache this,
        # but there's just enough edge cases, particularly now that
        # declarative encourages construction of SQL expressions
        # without tables present, to just regen this each time.
        froms = []
        seen = set()
        translate = self._from_cloned

        for item in itertools.chain(
            _from_objects(*self._raw_columns),
            _from_objects(self._whereclause)
            if self._whereclause is not None
            else (),
            self._from_obj,
        ):
            if item is self:
                raise exc.InvalidRequestError(
                    "select() construct refers to itself as a FROM"
                )
            if translate and item in translate:
                item = translate[item]
            if not seen.intersection(item._cloned_set):
                froms.append(item)
            seen.update(item._cloned_set)

        return froms

    def _get_display_froms(
        self, explicit_correlate_froms=None, implicit_correlate_froms=None
    ):
        """Return the full list of 'from' clauses to be displayed.

        Takes into account a set of existing froms which may be
        rendered in the FROM clause of enclosing selects; this Select
        may want to leave those absent if it is automatically
        correlating.

        """
        froms = self._froms

        toremove = set(
            itertools.chain(*[_expand_cloned(f._hide_froms) for f in froms])
        )
        if toremove:
            # if we're maintaining clones of froms,
            # add the copies out to the toremove list.  only include
            # clones that are lexical equivalents.
            if self._from_cloned:
                toremove.update(
                    self._from_cloned[f]
                    for f in toremove.intersection(self._from_cloned)
                    if self._from_cloned[f]._is_lexical_equivalent(f)
                )
            # filter out to FROM clauses not in the list,
            # using a list to maintain ordering
            froms = [f for f in froms if f not in toremove]

        if self._correlate:
            to_correlate = self._correlate
            if to_correlate:
                froms = [
                    f
                    for f in froms
                    if f
                    not in _cloned_intersection(
                        _cloned_intersection(
                            froms, explicit_correlate_froms or ()
                        ),
                        to_correlate,
                    )
                ]

        if self._correlate_except is not None:

            froms = [
                f
                for f in froms
                if f
                not in _cloned_difference(
                    _cloned_intersection(
                        froms, explicit_correlate_froms or ()
                    ),
                    self._correlate_except,
                )
            ]

        if (
            self._auto_correlate
            and implicit_correlate_froms
            and len(froms) > 1
        ):

            froms = [
                f
                for f in froms
                if f
                not in _cloned_intersection(froms, implicit_correlate_froms)
            ]

            if not len(froms):
                raise exc.InvalidRequestError(
                    "Select statement '%s"
                    "' returned no FROM clauses "
                    "due to auto-correlation; "
                    "specify correlate(<tables>) "
                    "to control correlation "
                    "manually." % self
                )

        return froms

    def _scalar_type(self):
        elem = self._raw_columns[0]
        cols = list(elem._select_iterable)
        return cols[0].type

    @property
    def froms(self):
        """Return the displayed list of FromClause elements."""

        return self._get_display_froms()

    def with_statement_hint(self, text, dialect_name="*"):
        """add a statement hint to this :class:`.Select`.

        This method is similar to :meth:`.Select.with_hint` except that
        it does not require an individual table, and instead applies to the
        statement as a whole.

        Hints here are specific to the backend database and may include
        directives such as isolation levels, file directives, fetch directives,
        etc.

        .. versionadded:: 1.0.0

        .. seealso::

            :meth:`.Select.with_hint`

            :meth:.`.Select.prefix_with` - generic SELECT prefixing which also
            can suit some database-specific HINT syntaxes such as MySQL
            optimizer hints

        """
        return self.with_hint(None, text, dialect_name)

    @_generative
    def with_hint(self, selectable, text, dialect_name="*"):
        r"""Add an indexing or other executional context hint for the given
        selectable to this :class:`.Select`.

        The text of the hint is rendered in the appropriate
        location for the database backend in use, relative
        to the given :class:`.Table` or :class:`.Alias` passed as the
        ``selectable`` argument. The dialect implementation
        typically uses Python string substitution syntax
        with the token ``%(name)s`` to render the name of
        the table or alias. E.g. when using Oracle, the
        following::

            select([mytable]).\
                with_hint(mytable, "index(%(name)s ix_mytable)")

        Would render SQL as::

            select /*+ index(mytable ix_mytable) */ ... from mytable

        The ``dialect_name`` option will limit the rendering of a particular
        hint to a particular backend. Such as, to add hints for both Oracle
        and Sybase simultaneously::

            select([mytable]).\
                with_hint(mytable, "index(%(name)s ix_mytable)", 'oracle').\
                with_hint(mytable, "WITH INDEX ix_mytable", 'sybase')

        .. seealso::

            :meth:`.Select.with_statement_hint`

        """
        if selectable is None:
            self._statement_hints += ((dialect_name, text),)
        else:
            self._hints = self._hints.union({(selectable, dialect_name): text})

    @property
    def type(self):
        raise exc.InvalidRequestError(
            "Select objects don't have a type.  "
            "Call as_scalar() on this Select "
            "object to return a 'scalar' version "
            "of this Select."
        )

    @_memoized_property.method
    def locate_all_froms(self):
        """return a Set of all FromClause elements referenced by this Select.

        This set is a superset of that returned by the ``froms`` property,
        which is specifically for those FromClause elements that would
        actually be rendered.

        """
        froms = self._froms
        return froms + list(_from_objects(*froms))

    @property
    def inner_columns(self):
        """an iterator of all ColumnElement expressions which would
        be rendered into the columns clause of the resulting SELECT statement.

        """
        return _select_iterables(self._raw_columns)

    @_memoized_property
    def _label_resolve_dict(self):
        with_cols = dict(
            (c._resolve_label or c._label or c.key, c)
            for c in _select_iterables(self._raw_columns)
            if c._allow_label_resolve
        )
        only_froms = dict(
            (c.key, c)
            for c in _select_iterables(self.froms)
            if c._allow_label_resolve
        )
        only_cols = with_cols.copy()
        for key, value in only_froms.items():
            with_cols.setdefault(key, value)

        return with_cols, only_froms, only_cols

    def is_derived_from(self, fromclause):
        if self in fromclause._cloned_set:
            return True

        for f in self.locate_all_froms():
            if f.is_derived_from(fromclause):
                return True
        return False

    def _copy_internals(self, clone=_clone, **kw):
        super(Select, self)._copy_internals(clone, **kw)

        # Select() object has been cloned and probably adapted by the
        # given clone function.  Apply the cloning function to internal
        # objects

        # 1. keep a dictionary of the froms we've cloned, and what
        # they've become.  This is consulted later when we derive
        # additional froms from "whereclause" and the columns clause,
        # which may still reference the uncloned parent table.
        # as of 0.7.4 we also put the current version of _froms, which
        # gets cleared on each generation.  previously we were "baking"
        # _froms into self._from_obj.
        self._from_cloned = from_cloned = dict(
            (f, clone(f, **kw)) for f in self._from_obj.union(self._froms)
        )

        # 3. update persistent _from_obj with the cloned versions.
        self._from_obj = util.OrderedSet(
            from_cloned[f] for f in self._from_obj
        )

        # the _correlate collection is done separately, what can happen
        # here is the same item is _correlate as in _from_obj but the
        # _correlate version has an annotation on it - (specifically
        # RelationshipProperty.Comparator._criterion_exists() does
        # this). Also keep _correlate liberally open with its previous
        # contents, as this set is used for matching, not rendering.
        self._correlate = set(clone(f) for f in self._correlate).union(
            self._correlate
        )

        # do something similar for _correlate_except - this is a more
        # unusual case but same idea applies
        if self._correlate_except:
            self._correlate_except = set(
                clone(f) for f in self._correlate_except
            ).union(self._correlate_except)

        # 4. clone other things.   The difficulty here is that Column
        # objects are not actually cloned, and refer to their original
        # .table, resulting in the wrong "from" parent after a clone
        # operation.  Hence _from_cloned and _from_obj supersede what is
        # present here.
        self._raw_columns = [clone(c, **kw) for c in self._raw_columns]
        for attr in (
            "_whereclause",
            "_having",
            "_order_by_clause",
            "_group_by_clause",
            "_for_update_arg",
        ):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr), **kw))

        # erase exported column list, _froms collection,
        # etc.
        self._reset_exported()

    def get_children(self, column_collections=True, **kwargs):
        """return child elements as per the ClauseElement specification."""

        return (
            (column_collections and list(self.columns) or [])
            + self._raw_columns
            + list(self._froms)
            + [
                x
                for x in (
                    self._whereclause,
                    self._having,
                    self._order_by_clause,
                    self._group_by_clause,
                )
                if x is not None
            ]
        )

    @_generative
    def column(self, column):
        """return a new select() construct with the given column expression
            added to its columns clause.

            E.g.::

                my_select = my_select.column(table.c.new_column)

            See the documentation for :meth:`.Select.with_only_columns`
            for guidelines on adding /replacing the columns of a
            :class:`.Select` object.

        """
        self.append_column(column)

    @util.dependencies("sqlalchemy.sql.util")
    def reduce_columns(self, sqlutil, only_synonyms=True):
        """Return a new :func`.select` construct with redundantly
        named, equivalently-valued columns removed from the columns clause.

        "Redundant" here means two columns where one refers to the
        other either based on foreign key, or via a simple equality
        comparison in the WHERE clause of the statement.   The primary purpose
        of this method is to automatically construct a select statement
        with all uniquely-named columns, without the need to use
        table-qualified labels as :meth:`.apply_labels` does.

        When columns are omitted based on foreign key, the referred-to
        column is the one that's kept.  When columns are omitted based on
        WHERE equivalence, the first column in the columns clause is the
        one that's kept.

        :param only_synonyms: when True, limit the removal of columns
         to those which have the same name as the equivalent.   Otherwise,
         all columns that are equivalent to another are removed.

        """
        return self.with_only_columns(
            sqlutil.reduce_columns(
                self.inner_columns,
                only_synonyms=only_synonyms,
                *(self._whereclause,) + tuple(self._from_obj)
            )
        )

    @_generative
    def with_only_columns(self, columns):
        r"""Return a new :func:`.select` construct with its columns
        clause replaced with the given columns.

        This method is exactly equivalent to as if the original
        :func:`.select` had been called with the given columns
        clause.   I.e. a statement::

            s = select([table1.c.a, table1.c.b])
            s = s.with_only_columns([table1.c.b])

        should be exactly equivalent to::

            s = select([table1.c.b])

        This means that FROM clauses which are only derived
        from the column list will be discarded if the new column
        list no longer contains that FROM::

            >>> table1 = table('t1', column('a'), column('b'))
            >>> table2 = table('t2', column('a'), column('b'))
            >>> s1 = select([table1.c.a, table2.c.b])
            >>> print s1
            SELECT t1.a, t2.b FROM t1, t2
            >>> s2 = s1.with_only_columns([table2.c.b])
            >>> print s2
            SELECT t2.b FROM t1

        The preferred way to maintain a specific FROM clause
        in the construct, assuming it won't be represented anywhere
        else (i.e. not in the WHERE clause, etc.) is to set it using
        :meth:`.Select.select_from`::

            >>> s1 = select([table1.c.a, table2.c.b]).\
            ...         select_from(table1.join(table2,
            ...                 table1.c.a==table2.c.a))
            >>> s2 = s1.with_only_columns([table2.c.b])
            >>> print s2
            SELECT t2.b FROM t1 JOIN t2 ON t1.a=t2.a

        Care should also be taken to use the correct
        set of column objects passed to :meth:`.Select.with_only_columns`.
        Since the method is essentially equivalent to calling the
        :func:`.select` construct in the first place with the given
        columns, the columns passed to :meth:`.Select.with_only_columns`
        should usually be a subset of those which were passed
        to the :func:`.select` construct, not those which are available
        from the ``.c`` collection of that :func:`.select`.  That
        is::

            s = select([table1.c.a, table1.c.b]).select_from(table1)
            s = s.with_only_columns([table1.c.b])

        and **not**::

            # usually incorrect
            s = s.with_only_columns([s.c.b])

        The latter would produce the SQL::

            SELECT b
            FROM (SELECT t1.a AS a, t1.b AS b
            FROM t1), t1

        Since the :func:`.select` construct is essentially being
        asked to select both from ``table1`` as well as itself.

        """
        self._reset_exported()
        rc = []
        for c in columns:
            c = _interpret_as_column_or_from(c)
            if isinstance(c, ScalarSelect):
                c = c.self_group(against=operators.comma_op)
            rc.append(c)
        self._raw_columns = rc

    @_generative
    def where(self, whereclause):
        """return a new select() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """

        self.append_whereclause(whereclause)

    @_generative
    def having(self, having):
        """return a new select() construct with the given expression added to
        its HAVING clause, joined to the existing clause via AND, if any.

        """
        self.append_having(having)

    @_generative
    def distinct(self, *expr):
        r"""Return a new select() construct which will apply DISTINCT to its
        columns clause.

        :param \*expr: optional column expressions.  When present,
         the PostgreSQL dialect will render a ``DISTINCT ON (<expressions>>)``
         construct.

        """
        if expr:
            expr = [_literal_as_label_reference(e) for e in expr]
            if isinstance(self._distinct, list):
                self._distinct = self._distinct + expr
            else:
                self._distinct = expr
        else:
            self._distinct = True

    @_generative
    def select_from(self, fromclause):
        r"""return a new :func:`.select` construct with the
        given FROM expression
        merged into its list of FROM objects.

        E.g.::

            table1 = table('t1', column('a'))
            table2 = table('t2', column('b'))
            s = select([table1.c.a]).\
                select_from(
                    table1.join(table2, table1.c.a==table2.c.b)
                )

        The "from" list is a unique set on the identity of each element,
        so adding an already present :class:`.Table` or other selectable
        will have no effect.   Passing a :class:`.Join` that refers
        to an already present :class:`.Table` or other selectable will have
        the effect of concealing the presence of that selectable as
        an individual element in the rendered FROM list, instead
        rendering it into a JOIN clause.

        While the typical purpose of :meth:`.Select.select_from` is to
        replace the default, derived FROM clause with a join, it can
        also be called with individual table elements, multiple times
        if desired, in the case that the FROM clause cannot be fully
        derived from the columns clause::

            select([func.count('*')]).select_from(table1)

        """
        self.append_from(fromclause)

    @_generative
    def correlate(self, *fromclauses):
        r"""return a new :class:`.Select` which will correlate the given FROM
        clauses to that of an enclosing :class:`.Select`.

        Calling this method turns off the :class:`.Select` object's
        default behavior of "auto-correlation".  Normally, FROM elements
        which appear in a :class:`.Select` that encloses this one via
        its :term:`WHERE clause`, ORDER BY, HAVING or
        :term:`columns clause` will be omitted from this :class:`.Select`
        object's :term:`FROM clause`.
        Setting an explicit correlation collection using the
        :meth:`.Select.correlate` method provides a fixed list of FROM objects
        that can potentially take place in this process.

        When :meth:`.Select.correlate` is used to apply specific FROM clauses
        for correlation, the FROM elements become candidates for
        correlation regardless of how deeply nested this :class:`.Select`
        object is, relative to an enclosing :class:`.Select` which refers to
        the same FROM object.  This is in contrast to the behavior of
        "auto-correlation" which only correlates to an immediate enclosing
        :class:`.Select`.   Multi-level correlation ensures that the link
        between enclosed and enclosing :class:`.Select` is always via
        at least one WHERE/ORDER BY/HAVING/columns clause in order for
        correlation to take place.

        If ``None`` is passed, the :class:`.Select` object will correlate
        none of its FROM entries, and all will render unconditionally
        in the local FROM clause.

        :param \*fromclauses: a list of one or more :class:`.FromClause`
         constructs, or other compatible constructs (i.e. ORM-mapped
         classes) to become part of the correlate collection.

        .. seealso::

            :meth:`.Select.correlate_except`

            :ref:`correlated_subqueries`

        """
        self._auto_correlate = False
        if fromclauses and fromclauses[0] is None:
            self._correlate = ()
        else:
            self._correlate = set(self._correlate).union(
                _interpret_as_from(f) for f in fromclauses
            )

    @_generative
    def correlate_except(self, *fromclauses):
        r"""return a new :class:`.Select` which will omit the given FROM
        clauses from the auto-correlation process.

        Calling :meth:`.Select.correlate_except` turns off the
        :class:`.Select` object's default behavior of
        "auto-correlation" for the given FROM elements.  An element
        specified here will unconditionally appear in the FROM list, while
        all other FROM elements remain subject to normal auto-correlation
        behaviors.

        If ``None`` is passed, the :class:`.Select` object will correlate
        all of its FROM entries.

        :param \*fromclauses: a list of one or more :class:`.FromClause`
         constructs, or other compatible constructs (i.e. ORM-mapped
         classes) to become part of the correlate-exception collection.

        .. seealso::

            :meth:`.Select.correlate`

            :ref:`correlated_subqueries`

        """

        self._auto_correlate = False
        if fromclauses and fromclauses[0] is None:
            self._correlate_except = ()
        else:
            self._correlate_except = set(self._correlate_except or ()).union(
                _interpret_as_from(f) for f in fromclauses
            )

    def append_correlation(self, fromclause):
        """append the given correlation expression to this select()
        construct.

        This is an **in-place** mutation method; the
        :meth:`~.Select.correlate` method is preferred, as it provides
        standard :term:`method chaining`.

        """

        self._auto_correlate = False
        self._correlate = set(self._correlate).union(
            _interpret_as_from(f) for f in fromclause
        )

    def append_column(self, column):
        """append the given column expression to the columns clause of this
        select() construct.

        E.g.::

            my_select.append_column(some_table.c.new_column)

        This is an **in-place** mutation method; the
        :meth:`~.Select.column` method is preferred, as it provides standard
        :term:`method chaining`.

        See the documentation for :meth:`.Select.with_only_columns`
        for guidelines on adding /replacing the columns of a
        :class:`.Select` object.

        """
        self._reset_exported()
        column = _interpret_as_column_or_from(column)

        if isinstance(column, ScalarSelect):
            column = column.self_group(against=operators.comma_op)

        self._raw_columns = self._raw_columns + [column]

    def append_prefix(self, clause):
        """append the given columns clause prefix expression to this select()
        construct.

        This is an **in-place** mutation method; the
        :meth:`~.Select.prefix_with` method is preferred, as it provides
        standard :term:`method chaining`.

        """
        clause = _literal_as_text(clause)
        self._prefixes = self._prefixes + (clause,)

    def append_whereclause(self, whereclause):
        """append the given expression to this select() construct's WHERE
        criterion.

        The expression will be joined to existing WHERE criterion via AND.

        This is an **in-place** mutation method; the
        :meth:`~.Select.where` method is preferred, as it provides standard
        :term:`method chaining`.

        """

        self._reset_exported()
        self._whereclause = and_(True_._ifnone(self._whereclause), whereclause)

    def append_having(self, having):
        """append the given expression to this select() construct's HAVING
        criterion.

        The expression will be joined to existing HAVING criterion via AND.

        This is an **in-place** mutation method; the
        :meth:`~.Select.having` method is preferred, as it provides standard
        :term:`method chaining`.

        """
        self._reset_exported()
        self._having = and_(True_._ifnone(self._having), having)

    def append_from(self, fromclause):
        """append the given FromClause expression to this select() construct's
        FROM clause.

        This is an **in-place** mutation method; the
        :meth:`~.Select.select_from` method is preferred, as it provides
        standard :term:`method chaining`.

        """
        self._reset_exported()
        fromclause = _interpret_as_from(fromclause)
        self._from_obj = self._from_obj.union([fromclause])

    @_memoized_property
    def _columns_plus_names(self):
        if self.use_labels:
            names = set()

            def name_for_col(c):
                if c._label is None or not c._render_label_in_columns_clause:
                    return (None, c)

                name = c._label
                if name in names:
                    name = c.anon_label
                else:
                    names.add(name)
                return name, c

            return [
                name_for_col(c)
                for c in util.unique_list(_select_iterables(self._raw_columns))
            ]
        else:
            return [
                (None, c)
                for c in util.unique_list(_select_iterables(self._raw_columns))
            ]

    def _populate_column_collection(self):
        for name, c in self._columns_plus_names:
            if not hasattr(c, "_make_proxy"):
                continue
            if name is None:
                key = None
            elif self.use_labels:
                key = c._key_label
                if key is not None and key in self.c:
                    key = c.anon_label
            else:
                key = None

            c._make_proxy(self, key=key, name=name, name_is_truncatable=True)

    def _refresh_for_new_column(self, column):
        for fromclause in self._froms:
            col = fromclause._refresh_for_new_column(column)
            if col is not None:
                if col in self.inner_columns and self._cols_populated:
                    our_label = col._key_label if self.use_labels else col.key
                    if our_label not in self.c:
                        return col._make_proxy(
                            self,
                            name=col._label if self.use_labels else None,
                            key=col._key_label if self.use_labels else None,
                            name_is_truncatable=True,
                        )
                return None
        return None

    def _needs_parens_for_grouping(self):
        return (
            self._limit_clause is not None
            or self._offset_clause is not None
            or bool(self._order_by_clause.clauses)
        )

    def self_group(self, against=None):
        """return a 'grouping' construct as per the ClauseElement
        specification.

        This produces an element that can be embedded in an expression. Note
        that this method is called automatically as needed when constructing
        expressions and should not require explicit use.

        """
        if (
            isinstance(against, CompoundSelect)
            and not self._needs_parens_for_grouping()
        ):
            return self
        return FromGrouping(self)

    def union(self, other, **kwargs):
        """return a SQL UNION of this select() construct against the given
        selectable."""

        return CompoundSelect._create_union(self, other, **kwargs)

    def union_all(self, other, **kwargs):
        """return a SQL UNION ALL of this select() construct against the given
        selectable.

        """
        return CompoundSelect._create_union_all(self, other, **kwargs)

    def except_(self, other, **kwargs):
        """return a SQL EXCEPT of this select() construct against the given
        selectable."""

        return CompoundSelect._create_except(self, other, **kwargs)

    def except_all(self, other, **kwargs):
        """return a SQL EXCEPT ALL of this select() construct against the
        given selectable.

        """
        return CompoundSelect._create_except_all(self, other, **kwargs)

    def intersect(self, other, **kwargs):
        """return a SQL INTERSECT of this select() construct against the given
        selectable.

        """
        return CompoundSelect._create_intersect(self, other, **kwargs)

    def intersect_all(self, other, **kwargs):
        """return a SQL INTERSECT ALL of this select() construct against the
        given selectable.

        """
        return CompoundSelect._create_intersect_all(self, other, **kwargs)

    def bind(self):
        if self._bind:
            return self._bind
        froms = self._froms
        if not froms:
            for c in self._raw_columns:
                e = c.bind
                if e:
                    self._bind = e
                    return e
        else:
            e = list(froms)[0].bind
            if e:
                self._bind = e
                return e

        return None

    def _set_bind(self, bind):
        self._bind = bind

    bind = property(bind, _set_bind)


class ScalarSelect(Generative, Grouping):
    _from_objects = []
    _is_from_container = True
    _is_implicitly_boolean = False

    def __init__(self, element):
        self.element = element
        self.type = element._scalar_type()

    @property
    def columns(self):
        raise exc.InvalidRequestError(
            "Scalar Select expression has no "
            "columns; use this object directly "
            "within a column-level expression."
        )

    c = columns

    @_generative
    def where(self, crit):
        """Apply a WHERE clause to the SELECT statement referred to
        by this :class:`.ScalarSelect`.

        """
        self.element = self.element.where(crit)

    def self_group(self, **kwargs):
        return self


class Exists(UnaryExpression):
    """Represent an ``EXISTS`` clause.

    """

    __visit_name__ = UnaryExpression.__visit_name__
    _from_objects = []

    def __init__(self, *args, **kwargs):
        """Construct a new :class:`.Exists` against an existing
        :class:`.Select` object.

        Calling styles are of the following forms::

            # use on an existing select()
            s = select([table.c.col1]).where(table.c.col2==5)
            s = exists(s)

            # construct a select() at once
            exists(['*'], **select_arguments).where(criterion)

            # columns argument is optional, generates "EXISTS (SELECT *)"
            # by default.
            exists().where(table.c.col2==5)

        """
        if args and isinstance(args[0], (SelectBase, ScalarSelect)):
            s = args[0]
        else:
            if not args:
                args = ([literal_column("*")],)
            s = Select(*args, **kwargs).as_scalar().self_group()

        UnaryExpression.__init__(
            self,
            s,
            operator=operators.exists,
            type_=type_api.BOOLEANTYPE,
            wraps_column_expression=True,
        )

    def select(self, whereclause=None, **params):
        return Select([self], whereclause, **params)

    def correlate(self, *fromclause):
        e = self._clone()
        e.element = self.element.correlate(*fromclause).self_group()
        return e

    def correlate_except(self, *fromclause):
        e = self._clone()
        e.element = self.element.correlate_except(*fromclause).self_group()
        return e

    def select_from(self, clause):
        """return a new :class:`.Exists` construct, applying the given
        expression to the :meth:`.Select.select_from` method of the select
        statement contained.

        """
        e = self._clone()
        e.element = self.element.select_from(clause).self_group()
        return e

    def where(self, clause):
        """return a new exists() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """
        e = self._clone()
        e.element = self.element.where(clause).self_group()
        return e


class TextAsFrom(SelectBase):
    """Wrap a :class:`.TextClause` construct within a :class:`.SelectBase`
    interface.

    This allows the :class:`.TextClause` object to gain a ``.c`` collection
    and other FROM-like capabilities such as :meth:`.FromClause.alias`,
    :meth:`.SelectBase.cte`, etc.

    The :class:`.TextAsFrom` construct is produced via the
    :meth:`.TextClause.columns` method - see that method for details.

    .. versionadded:: 0.9.0

    .. seealso::

        :func:`.text`

        :meth:`.TextClause.columns`

    """

    __visit_name__ = "text_as_from"

    _textual = True

    def __init__(self, text, columns, positional=False):
        self.element = text
        self.column_args = columns
        self.positional = positional

    @property
    def _bind(self):
        return self.element._bind

    @_generative
    def bindparams(self, *binds, **bind_as_values):
        self.element = self.element.bindparams(*binds, **bind_as_values)

    def _populate_column_collection(self):
        for c in self.column_args:
            c._make_proxy(self)

    def _copy_internals(self, clone=_clone, **kw):
        self._reset_exported()
        self.element = clone(self.element, **kw)

    def _scalar_type(self):
        return self.column_args[0].type


class AnnotatedFromClause(Annotated):
    def __init__(self, element, values):
        # force FromClause to generate their internal
        # collections into __dict__
        element.c
        Annotated.__init__(self, element, values)
