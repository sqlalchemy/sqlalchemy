# sql/selectable.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""The :class:`.FromClause` class of SQL expression elements, representing
SQL tables and derived rowsets.

"""

import collections
import itertools
from operator import attrgetter

from . import coercions
from . import operators
from . import roles
from . import type_api
from .annotation import Annotated
from .annotation import SupportsCloneAnnotations
from .base import _clone
from .base import _cloned_difference
from .base import _cloned_intersection
from .base import _expand_cloned
from .base import _from_objects
from .base import _generative
from .base import ColumnCollection
from .base import ColumnSet
from .base import DedupeColumnCollection
from .base import Executable
from .base import Generative
from .base import HasMemoized
from .base import Immutable
from .coercions import _document_text_coercion
from .elements import _anonymous_label
from .elements import _select_iterables
from .elements import and_
from .elements import BindParameter
from .elements import ClauseElement
from .elements import ClauseList
from .elements import ColumnClause
from .elements import GroupedElement
from .elements import Grouping
from .elements import literal_column
from .elements import True_
from .elements import UnaryExpression
from .visitors import InternalTraversal
from .. import exc
from .. import util

if util.TYPE_CHECKING:
    from typing import Any
    from typing import Optional


class _OffsetLimitParam(BindParameter):
    @property
    def _limit_offset_value(self):
        return self.effective_value


@util.deprecated(
    "1.4",
    "The standalone :func:`.subquery` function is deprecated "
    "and will be removed in a future release.  Use select().subquery().",
)
def subquery(alias, *args, **kwargs):
    r"""Return an :class:`.Subquery` object derived
    from a :class:`.Select`.

    :param name: the alias name for the subquery

    :param \*args, \**kwargs:  all other arguments are passed through to the
     :func:`.select` function.

    """
    return Select(*args, **kwargs).subquery(alias)


class ReturnsRows(roles.ReturnsRowsRole, ClauseElement):
    """The basemost class for Core constructs that have some concept of
    columns that can represent rows.

    While the SELECT statement and TABLE are the primary things we think
    of in this category,  DML like INSERT, UPDATE and DELETE can also specify
    RETURNING which means they can be used in CTEs and other forms, and
    PostgreSQL has functions that return rows also.

    .. versionadded:: 1.4

    """

    _is_returns_rows = True

    # sub-elements of returns_rows
    _is_from_clause = False
    _is_select_statement = False
    _is_lateral = False

    @property
    def selectable(self):
        raise NotImplementedError()


class Selectable(ReturnsRows):
    """mark a class as being selectable.

    """

    __visit_name__ = "selectable"

    is_selectable = True

    @property
    def selectable(self):
        return self

    @property
    def exported_columns(self):
        """A :class:`.ColumnCollection` that represents the "exported"
        columns of this :class:`.Selectable`.

        The "exported" columns represent the collection of
        :class:`.ColumnElement` expressions that are rendered by this SQL
        construct.   There are two primary varieties which are the
        "FROM clause columns" of a FROM clause, such as a table, join,
        or subquery, and the "SELECTed columns", which are the columns in
        the "columns clause" of a SELECT statement.

        .. versionadded:: 1.4

        .. seealso:

            :attr:`.FromClause.exported_columns`

            :attr:`.SelectBase.exported_columns`
        """

        raise NotImplementedError()

    def _refresh_for_new_column(self, column):
        raise NotImplementedError()

    def lateral(self, name=None):
        """Return a LATERAL alias of this :class:`.Selectable`.

        The return value is the :class:`.Lateral` construct also
        provided by the top-level :func:`~.expression.lateral` function.

        .. versionadded:: 1.1

        .. seealso::

            :ref:`lateral_selects` -  overview of usage.

        """
        return Lateral._construct(self, name)

    @util.deprecated(
        "1.4",
        message="The :meth:`.Selectable.replace_selectable` method is "
        "deprecated, and will be removed in a future release.  Similar "
        "functionality is available via the sqlalchemy.sql.visitors module.",
    )
    @util.dependencies("sqlalchemy.sql.util")
    def replace_selectable(self, sqlutil, old, alias):
        """replace all occurrences of FromClause 'old' with the given Alias
        object, returning a copy of this :class:`.FromClause`.

        """

        return sqlutil.ClauseAdapter(alias).traverse(self)

    def corresponding_column(self, column, require_embedded=False):
        """Given a :class:`.ColumnElement`, return the exported
        :class:`.ColumnElement` object from the
        :attr:`.Selectable.exported_columns`
        collection of this :class:`.Selectable` which corresponds to that
        original :class:`.ColumnElement` via a common ancestor
        column.

        :param column: the target :class:`.ColumnElement` to be matched

        :param require_embedded: only return corresponding columns for
         the given :class:`.ColumnElement`, if the given
         :class:`.ColumnElement` is actually present within a sub-element
         of this :class:`.Selectable`.  Normally the column will match if
         it merely shares a common ancestor with one of the exported
         columns of this :class:`.Selectable`.

        .. seealso::

            :attr:`.Selectable.exported_columns` - the
            :class:`.ColumnCollection` that is used for the operation

            :meth:`.ColumnCollection.corresponding_column` - implementation
            method.

        """

        return self.exported_columns.corresponding_column(
            column, require_embedded
        )


class HasPrefixes(object):
    _prefixes = ()

    _has_prefixes_traverse_internals = [
        ("_prefixes", InternalTraversal.dp_prefix_sequence)
    ]

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
                (coercions.expect(roles.StatementOptionRole, p), dialect)
                for p in prefixes
            ]
        )


class HasSuffixes(object):
    _suffixes = ()

    _has_suffixes_traverse_internals = [
        ("_suffixes", InternalTraversal.dp_prefix_sequence)
    ]

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
                (coercions.expect(roles.StatementOptionRole, p), dialect)
                for p in suffixes
            ]
        )


class FromClause(HasMemoized, roles.AnonymizedFromClauseRole, Selectable):
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

    schema = None
    """Define the 'schema' attribute for this :class:`.FromClause`.

    This is typically ``None`` for most objects except that of
    :class:`.Table`, where it is taken as the value of the
    :paramref:`.Table.schema` argument.

    """

    is_selectable = True
    _is_from_clause = True
    _is_join = False

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
        return Select._create_select_from_fromclause(
            self,
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

        E.g.::

            a2 = some_table.alias('a2')

        The above code creates an :class:`.Alias` object which can be used
        as a FROM clause in any SELECT statement.

        .. seealso::

            :ref:`core_tutorial_aliases`

            :func:`~.expression.alias`

        """

        return Alias._construct(self, name)

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

    @property
    def description(self):
        """a brief description of this FromClause.

        Used primarily for error message formatting.

        """
        return getattr(self, "name", self.__class__.__name__ + " object")

    def _generate_fromclause_column_proxies(self, fromclause):
        fromclause._columns._populate_separate_keys(
            col._make_proxy(fromclause) for col in self.c
        )

    @property
    def exported_columns(self):
        """A :class:`.ColumnCollection` that represents the "exported"
        columns of this :class:`.Selectable`.

        The "exported" columns for a :class:`.FromClause` object are synonymous
        with the :attr:`.FromClause.columns` collection.

        .. versionadded:: 1.4

        .. seealso:

            :attr:`.Selectable.exported_columns`

            :attr:`.SelectBase.exported_columns`


        """
        return self.columns

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
        self._reset_exported()


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

    _traverse_internals = [
        ("left", InternalTraversal.dp_clauseelement),
        ("right", InternalTraversal.dp_clauseelement),
        ("onclause", InternalTraversal.dp_clauseelement),
        ("isouter", InternalTraversal.dp_boolean),
        ("full", InternalTraversal.dp_boolean),
    ]

    _is_join = True

    def __init__(self, left, right, onclause=None, isouter=False, full=False):
        """Construct a new :class:`.Join`.

        The usual entrypoint here is the :func:`~.expression.join`
        function or the :meth:`.FromClause.join` method of any
        :class:`.FromClause` object.

        """
        self.left = coercions.expect(roles.FromClauseRole, left)
        self.right = coercions.expect(roles.FromClauseRole, right).self_group()

        if onclause is None:
            self.onclause = self._match_primaries(self.left, self.right)
        else:
            self.onclause = onclause.self_group(against=operators._asbool)

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
        self._columns._populate_separate_keys(
            (col._key_label, col) for col in columns
        )
        self.foreign_keys.update(
            itertools.chain(*[col.foreign_keys for col in columns])
        )

    def _refresh_for_new_column(self, column):
        super(Join, self)._refresh_for_new_column(column)
        self.left._refresh_for_new_column(column)
        self.right._refresh_for_new_column(column)

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
        a = coercions.expect(roles.FromClauseRole, a)
        b = coercions.expect(roles.FromClauseRole, b)

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

        if whereclause is not None:
            kwargs["whereclause"] = whereclause
        return Select._create_select_from_fromclause(
            self, collist, **kwargs
        ).select_from(self)

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

            :ref:`core_tutorial_aliases`

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
            return self.select().apply_labels().correlate(None).alias(name)

    @property
    def _hide_froms(self):
        return itertools.chain(
            *[_from_objects(x.left, x.right) for x in self._cloned_set]
        )

    @property
    def _from_objects(self):
        return [self] + self.left._from_objects + self.right._from_objects


class NoInit(object):
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


# FromClause ->
#   AliasedReturnsRows
#        -> Alias   only for FromClause
#        -> Subquery  only for SelectBase
#        -> CTE only for HasCTE -> SelectBase, DML
#        -> Lateral -> FromClause, but we accept SelectBase
#           w/ non-deprecated coercion
#        -> TableSample -> only for FromClause
class AliasedReturnsRows(NoInit, FromClause):
    """Base class of aliases against tables, subqueries, and other
    selectables."""

    _is_from_container = True
    named_with_column = True

    _traverse_internals = [
        ("element", InternalTraversal.dp_clauseelement),
        ("name", InternalTraversal.dp_anon_name),
    ]

    @classmethod
    def _construct(cls, *arg, **kw):
        obj = cls.__new__(cls)
        obj._init(*arg, **kw)
        return obj

    @classmethod
    def _factory(cls, returnsrows, name=None):
        """Base factory method.  Subclasses need to provide this."""
        raise NotImplementedError()

    def _init(self, selectable, name=None):
        self.element = selectable
        self.supports_execution = selectable.supports_execution
        if self.supports_execution:
            self._execution_options = selectable._execution_options
        self.element = selectable
        self._orig_name = name
        if name is None:
            if (
                isinstance(selectable, FromClause)
                and selectable.named_with_column
            ):
                name = getattr(selectable, "name", None)
                if isinstance(name, _anonymous_label):
                    name = None
            name = _anonymous_label("%%(%d %s)s" % (id(self), name or "anon"))
        self.name = name

    def _refresh_for_new_column(self, column):
        super(AliasedReturnsRows, self)._refresh_for_new_column(column)
        self.element._refresh_for_new_column(column)

    @property
    def description(self):
        if util.py3k:
            return self.name
        else:
            return self.name.encode("ascii", "backslashreplace")

    @property
    def original(self):
        """legacy for dialects that are referring to Alias.original"""
        return self.element

    def is_derived_from(self, fromclause):
        if fromclause in self._cloned_set:
            return True
        return self.element.is_derived_from(fromclause)

    def _populate_column_collection(self):
        self.element._generate_fromclause_column_proxies(self)

    def _copy_internals(self, clone=_clone, **kw):
        element = clone(self.element, **kw)

        # the element clone is usually against a Table that returns the
        # same object.  don't reset exported .c. collections and other
        # memoized details if nothing changed
        if element is not self.element:
            self._reset_exported()
            self.element = element

    @property
    def _from_objects(self):
        return [self]

    @property
    def bind(self):
        return self.element.bind


class Alias(AliasedReturnsRows):
    """Represents an table or selectable alias (AS).

    Represents an alias, as typically applied to any table or
    sub-select within a SQL statement using the ``AS`` keyword (or
    without the keyword on certain databases such as Oracle).

    This object is constructed from the :func:`~.expression.alias` module
    level function as well as the :meth:`.FromClause.alias` method available
    on all :class:`.FromClause` subclasses.

    .. seealso::

        :meth:`.FromClause.alias`

    """

    __visit_name__ = "alias"

    @classmethod
    def _factory(cls, selectable, name=None, flat=False):
        """Return an :class:`.Alias` object.

        An :class:`.Alias` represents any :class:`.FromClause`
        with an alternate name assigned within SQL, typically using the ``AS``
        clause when generated, e.g. ``SELECT * FROM table AS aliasname``.

        Similar functionality is available via the
        :meth:`~.FromClause.alias` method
        available on all :class:`.FromClause` subclasses.   In terms of a
        SELECT object as generated from the :func:`.select` function, the
        :meth:`.SelectBase.alias` method returns an :class:`.Alias` or
        similar object which represents a named, parenthesized subquery.

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
        return coercions.expect(
            roles.FromClauseRole, selectable, allow_select=True
        ).alias(name=name, flat=flat)


class Lateral(AliasedReturnsRows):
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
        return coercions.expect(
            roles.FromClauseRole, selectable, explicit_subquery=True
        ).lateral(name=name)


class TableSample(AliasedReturnsRows):
    """Represent a TABLESAMPLE clause.

    This object is constructed from the :func:`~.expression.tablesample` module
    level function as well as the :meth:`.FromClause.tablesample` method
    available on all :class:`.FromClause` subclasses.

    .. versionadded:: 1.1

    .. seealso::

        :func:`~.expression.tablesample`

    """

    __visit_name__ = "tablesample"

    _traverse_internals = AliasedReturnsRows._traverse_internals + [
        ("sampling", InternalTraversal.dp_clauseelement),
        ("seed", InternalTraversal.dp_clauseelement),
    ]

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
        return coercions.expect(roles.FromClauseRole, selectable).tablesample(
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


class CTE(Generative, HasPrefixes, HasSuffixes, AliasedReturnsRows):
    """Represent a Common Table Expression.

    The :class:`.CTE` object is obtained using the
    :meth:`.SelectBase.cte` method from any selectable.
    See that method for complete examples.

    """

    __visit_name__ = "cte"

    _traverse_internals = (
        AliasedReturnsRows._traverse_internals
        + [
            ("_cte_alias", InternalTraversal.dp_clauseelement),
            ("_restates", InternalTraversal.dp_clauseelement_unordered_set),
            ("recursive", InternalTraversal.dp_boolean),
        ]
        + HasPrefixes._has_prefixes_traverse_internals
        + HasSuffixes._has_suffixes_traverse_internals
    )

    @classmethod
    def _factory(cls, selectable, name=None, recursive=False):
        r"""Return a new :class:`.CTE`, or Common Table Expression instance.

        Please see :meth:`.HasCte.cte` for detail on CTE usage.

        """
        return coercions.expect(roles.HasCTERole, selectable).cte(
            name=name, recursive=recursive
        )

    def _init(
        self,
        selectable,
        name=None,
        recursive=False,
        _cte_alias=None,
        _restates=frozenset(),
        _prefixes=None,
        _suffixes=None,
    ):
        self.recursive = recursive
        self._cte_alias = _cte_alias
        self._restates = _restates
        if _prefixes:
            self._prefixes = _prefixes
        if _suffixes:
            self._suffixes = _suffixes
        super(CTE, self)._init(selectable, name=name)

    def _copy_internals(self, clone=_clone, **kw):
        super(CTE, self)._copy_internals(clone, **kw)
        # TODO: I don't like that we can't use the traversal data here
        if self._cte_alias is not None:
            self._cte_alias = clone(self._cte_alias, **kw)
        self._restates = frozenset(
            [clone(elem, **kw) for elem in self._restates]
        )

    def alias(self, name=None, flat=False):
        """Return an :class:`.Alias` of this :class:`.CTE`.

        This method is a CTE-specific specialization of the
        :class:`.FromClause.alias` method.

        .. seealso::

            :ref:`core_tutorial_aliases`

            :func:`~.expression.alias`

        """
        return CTE._construct(
            self.element,
            name=name,
            recursive=self.recursive,
            _cte_alias=self,
            _prefixes=self._prefixes,
            _suffixes=self._suffixes,
        )

    def union(self, other):
        return CTE._construct(
            self.element.union(other),
            name=self.name,
            recursive=self.recursive,
            _restates=self._restates.union([self]),
            _prefixes=self._prefixes,
            _suffixes=self._suffixes,
        )

    def union_all(self, other):
        return CTE._construct(
            self.element.union_all(other),
            name=self.name,
            recursive=self.recursive,
            _restates=self._restates.union([self]),
            _prefixes=self._prefixes,
            _suffixes=self._suffixes,
        )


class HasCTE(roles.HasCTERole):
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

        .. versionchanged:: 1.1 Added support for UPDATE/INSERT/DELETE as
           CTE, CTEs added to UPDATE/INSERT/DELETE.

        SQLAlchemy detects :class:`.CTE` objects, which are treated
        similarly to :class:`.Alias` objects, as special elements
        to be delivered to the FROM clause of the statement as well
        as to a WITH clause at the top of the statement.

        For special prefixes such as PostgreSQL "MATERIALIZED" and
        "NOT MATERIALIZED", the :meth:`.CTE.prefix_with` method may be
        used to establish these.

        .. versionchanged:: 1.3.13 Added support for prefixes.
           In particular - MATERIALIZED and NOT MATERIALIZED.

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


class Subquery(AliasedReturnsRows):
    """Represent a subquery of a SELECT.

    A :class:`.Subquery` is created by invoking the
    :meth:`.SelectBase.subquery` method, or for convenience the
    :class:`.SelectBase.alias` method, on any :class:`.SelectBase` subclass
    which includes :class:`.Select`, :class:`.CompoundSelect`, and
    :class:`.TextualSelect`.  As rendered in a FROM clause, it represents the
    body of the SELECT statement inside of parenthesis, followed by the usual
    "AS <somename>" that defines all "alias" objects.

    The :class:`.Subquery` object is very similar to the :class:`.Alias`
    object and can be used in an equivalent way.    The difference between
    :class:`.Alias` and :class:`.Subquery` is that :class:`.Alias` always
    contains a :class:`.FromClause` object whereas :class:`.Subquery`
    always contains a :class:`.SelectBase` object.

    .. versionadded:: 1.4 The :class:`.Subquery` class was added which now
       serves the purpose of providing an aliased version of a SELECT
       statement.

    """

    __visit_name__ = "subquery"

    _is_subquery = True

    @classmethod
    def _factory(cls, selectable, name=None):
        """Return a :class:`.Subquery` object.

        """
        return coercions.expect(
            roles.SelectStatementRole, selectable
        ).subquery(name=name)

    @util.deprecated(
        "1.4",
        "The :meth:`.Subquery.as_scalar` method, which was previously "
        "``Alias.as_scalar()`` prior to version 1.4, is deprecated and "
        "will be removed in a future release; Please use the "
        ":meth:`.Select.scalar_subquery` method of the :func:`.select` "
        "construct before constructing a subquery object, or with the ORM "
        "use the :meth:`.Query.scalar_subquery` method.",
    )
    def as_scalar(self):
        return self.element.scalar_subquery()


class FromGrouping(GroupedElement, FromClause):
    """Represent a grouping of a FROM clause"""

    _traverse_internals = [("element", InternalTraversal.dp_clauseelement)]

    def __init__(self, element):
        self.element = coercions.expect(roles.FromClauseRole, element)

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

    @property
    def _from_objects(self):
        return self.element._from_objects

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

    _traverse_internals = [
        (
            "columns",
            InternalTraversal.dp_fromclause_canonical_column_collection,
        ),
        ("name", InternalTraversal.dp_string),
    ]

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
        self._columns = DedupeColumnCollection()
        self.primary_key = ColumnSet()
        self.foreign_keys = set()
        for c in columns:
            self.append_column(c)

    def _refresh_for_new_column(self, column):
        pass

    def _init_collections(self):
        pass

    @util.memoized_property
    def description(self):
        if util.py3k:
            return self.name
        else:
            return self.name.encode("ascii", "backslashreplace")

    def append_column(self, c):
        self._columns.add(c)
        c.table = self

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
    _traverse_internals = [
        ("of", InternalTraversal.dp_clauseelement_list),
        ("nowait", InternalTraversal.dp_boolean),
        ("read", InternalTraversal.dp_boolean),
        ("skip_locked", InternalTraversal.dp_boolean),
    ]

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

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return id(self)

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
                coercions.expect(roles.ColumnsClauseRole, elem)
                for elem in util.to_list(of)
            ]
        else:
            self.of = None


class SelectBase(
    roles.SelectStatementRole,
    roles.DMLSelectRole,
    roles.CompoundElementRole,
    roles.InElementRole,
    HasMemoized,
    HasCTE,
    Executable,
    SupportsCloneAnnotations,
    Selectable,
):
    """Base class for SELECT statements.


    This includes :class:`.Select`, :class:`.CompoundSelect` and
    :class:`.TextualSelect`.


    """

    _is_select_statement = True

    _memoized_property = util.group_expirable_memoized_property()

    def _generate_fromclause_column_proxies(self, fromclause):
        # type: (FromClause) -> None
        raise NotImplementedError()

    def _refresh_for_new_column(self, column):
        self._reset_memoizations()

    @property
    def selected_columns(self):
        """A :class:`.ColumnCollection` representing the columns that
        this SELECT statement or similar construct returns in its result set.

        This collection differs from the :attr:`.FromClause.columns` collection
        of a :class:`.FromClause` in that the columns within this collection
        cannot be directly nested inside another SELECT statement; a subquery
        must be applied first which provides for the necessary parenthesization
        required by SQL.

        .. versionadded:: 1.4

        """
        raise NotImplementedError()

    @property
    def exported_columns(self):
        """A :class:`.ColumnCollection` that represents the "exported"
        columns of this :class:`.Selectable`.

        The "exported" columns for a :class:`.SelectBase` object are synonymous
        with the :attr:`.SelectBase.selected_columns` collection.

        .. versionadded:: 1.4

        .. seealso:

            :attr:`.Selectable.exported_columns`

            :attr:`.FromClause.exported_columns`


        """
        return self.selected_columns

    @property
    @util.deprecated(
        "1.4",
        "The :attr:`.SelectBase.c` and :attr:`.SelectBase.columns` attributes "
        "are deprecated and will be removed in a future release; these "
        "attributes implicitly create a subquery that should be explicit.  "
        "Please call :meth:`.SelectBase.subquery` first in order to create "
        "a subquery, which then contains this attribute.  To access the "
        "columns that this SELECT object SELECTs "
        "from, use the :attr:`.SelectBase.selected_columns` attribute.",
    )
    def c(self):
        return self._implicit_subquery.columns

    @property
    def columns(self):
        return self.c

    @util.deprecated(
        "1.4",
        "The :meth:`.SelectBase.select` method is deprecated "
        "and will be removed in a future release; this method implicitly "
        "creates a subquery that should be explicit.  "
        "Please call :meth:`.SelectBase.subquery` first in order to create "
        "a subquery, which then can be seleted.",
    )
    def select(self, *arg, **kw):
        return self._implicit_subquery.select(*arg, **kw)

    @util.deprecated(
        "1.4",
        "The :meth:`.SelectBase.join` method is deprecated "
        "and will be removed in a future release; this method implicitly "
        "creates a subquery that should be explicit.  "
        "Please call :meth:`.SelectBase.subquery` first in order to create "
        "a subquery, which then can be selected.",
    )
    def join(self, *arg, **kw):
        return self._implicit_subquery.join(*arg, **kw)

    @util.deprecated(
        "1.4",
        "The :meth:`.SelectBase.outerjoin` method is deprecated "
        "and will be removed in a future release; this method implicitly "
        "creates a subquery that should be explicit.  "
        "Please call :meth:`.SelectBase.subquery` first in order to create "
        "a subquery, which then can be selected.",
    )
    def outerjoin(self, *arg, **kw):
        return self._implicit_subquery.outerjoin(*arg, **kw)

    @_memoized_property
    def _implicit_subquery(self):
        return self.subquery()

    @util.deprecated(
        "1.4",
        "The :meth:`.SelectBase.as_scalar` method is deprecated and will be "
        "removed in a future release.  Please refer to "
        ":meth:`.SelectBase.scalar_subquery`.",
    )
    def as_scalar(self):
        return self.scalar_subquery()

    def scalar_subquery(self):
        """return a 'scalar' representation of this selectable, which can be
        used as a column expression.

        Typically, a select statement which has only one column in its columns
        clause is eligible to be used as a scalar expression.  The scalar
        subquery can then be used in the WHERE clause or columns clause of
        an enclosing SELECT.

        Note that the scalar subquery differentiates from the FROM-level
        subquery that can be produced using the :meth:`.SelectBase.subquery`
        method.

        .. versionchanged: 1.4 - the ``.as_scalar()`` method was renamed to
           :meth:`.SelectBase.scalar_subquery`.

        """
        return ScalarSelect(self)

    def label(self, name):
        """return a 'scalar' representation of this selectable, embedded as a
        subquery with a label.

        .. seealso::

            :meth:`~.SelectBase.as_scalar`.

        """
        return self.scalar_subquery().label(name)

    def lateral(self, name=None):
        """Return a LATERAL alias of this :class:`.Selectable`.

        The return value is the :class:`.Lateral` construct also
        provided by the top-level :func:`~.expression.lateral` function.

        .. versionadded:: 1.1

        .. seealso::

            :ref:`lateral_selects` -  overview of usage.

        """
        return Lateral._factory(self, name)

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
        s._reset_memoizations()
        return s

    @property
    def _from_objects(self):
        return [self]

    def subquery(self, name=None):
        """Return a subquery of this :class:`.SelectBase`.

        A subquery is from a SQL perspective a parentheized, named construct
        that can be placed in the FROM clause of another SELECT statement.

        Given a SELECT statement such as::

            stmt = select([table.c.id, table.c.name])

        The above statement might look like::

            SELECT table.id, table.name FROM table

        The subquery form by itself renders the same way, however when
        embedded into the FROM clause of another SELECT statement, it becomes
        a named sub-element::

            subq = stmt.subquery()
            new_stmt = select([subq])

        The above renders as::

            SELECT anon_1.id, anon_1.name
            FROM (SELECT table.id, table.name FROM table) AS anon_1

        Historically, :meth:`.SelectBase.subquery` is equivalent to calling
        the :meth:`.FromClause.alias` method on a FROM object; however,
        as a :class:`.SelectBase` object is not directly  FROM object,
        the :meth:`.SelectBase.subquery` method provides clearer semantics.

        .. versionadded:: 1.4

        """
        return Subquery._construct(self, name)

    def alias(self, name=None, flat=False):
        """Return a named subquery against this :class:`.SelectBase`.

        For a :class:`.SelectBase` (as opposed to a :class:`.FromClause`),
        this returns a :class:`.Subquery` object which behaves mostly the
        same as the :class:`.Alias` object that is used with a
        :class:`.FromClause`.

        .. versionchanged:: 1.4 The :meth:`.SelectBase.alias` method is now
           a synonym for the :meth:`.SelectBase.subquery` method.

        """
        return self.subquery(name=name)


class SelectStatementGrouping(GroupedElement, SelectBase):
    """Represent a grouping of a :class:`.SelectBase`.

    This differs from :class:`.Subquery` in that we are still
    an "inner" SELECT statement, this is strictly for grouping inside of
    compound selects.

    """

    __visit_name__ = "grouping"
    _traverse_internals = [("element", InternalTraversal.dp_clauseelement)]

    _is_select_container = True

    def __init__(self, element):
        # type: (SelectBase) -> None
        self.element = coercions.expect(roles.SelectStatementRole, element)

    @property
    def select_statement(self):
        return self.element

    def self_group(self, against=None):
        # type: (Optional[Any]) -> FromClause
        return self

    def _generate_fromclause_column_proxies(self, subquery):
        self.element._generate_fromclause_column_proxies(subquery)

    def _generate_proxy_for_new_column(self, column, subquery):
        return self.element._generate_proxy_for_new_column(subquery)

    @property
    def selected_columns(self):
        """A :class:`.ColumnCollection` representing the columns that
        the embedded SELECT statement returns in its result set.

        .. versionadded:: 1.4

        .. seealso::

            :ref:`.SelectBase.selected_columns`

        """
        return self.element.selected_columns

    @property
    def _from_objects(self):
        return self.element._from_objects


class DeprecatedSelectBaseGenerations(object):
    @util.deprecated(
        "1.4",
        "The :meth:`.GenerativeSelect.append_order_by` method is deprecated "
        "and will be removed in a future release.  Use the generative method "
        ":meth:`.GenerativeSelect.order_by`.",
    )
    def append_order_by(self, *clauses):
        """Append the given ORDER BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing ORDER BY criterion.

        This is an **in-place** mutation method; the
        :meth:`~.GenerativeSelect.order_by` method is preferred, as it
        provides standard :term:`method chaining`.

        .. seealso::

            :meth:`.GenerativeSelect.order_by`

        """
        self.order_by.non_generative(self, *clauses)

    @util.deprecated(
        "1.4",
        "The :meth:`.GenerativeSelect.append_group_by` method is deprecated "
        "and will be removed in a future release.  Use the generative method "
        ":meth:`.GenerativeSelect.group_by`.",
    )
    def append_group_by(self, *clauses):
        """Append the given GROUP BY criterion applied to this selectable.

        The criterion will be appended to any pre-existing GROUP BY criterion.

        This is an **in-place** mutation method; the
        :meth:`~.GenerativeSelect.group_by` method is preferred, as it
        provides standard :term:`method chaining`.

        .. seealso::

            :meth:`.GenerativeSelect.group_by`

        """
        self.group_by.non_generative(self, *clauses)


class GenerativeSelect(DeprecatedSelectBaseGenerations, SelectBase):
    """Base class for SELECT statements where additional elements can be
    added.

    This serves as the base for :class:`.Select` and :class:`.CompoundSelect`
    where elements such as ORDER BY, GROUP BY can be added and column
    rendering can be controlled.  Compare to :class:`.TextualSelect`, which,
    while it subclasses :class:`.SelectBase` and is also a SELECT construct,
    represents a fixed textual string which cannot be altered at this level,
    only wrapped as a subquery.

    .. versionadded:: 0.9.0 :class:`.GenerativeSelect` was added to
       provide functionality specific to :class:`.Select` and
       :class:`.CompoundSelect` while allowing :class:`.SelectBase` to be
       used for other SELECT-like objects, e.g. :class:`.TextualSelect`.

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
            self._limit_clause = self._offset_or_limit_clause(limit)
        if offset is not None:
            self._offset_clause = self._offset_or_limit_clause(offset)
        self._bind = bind

        if order_by is not None:
            self._order_by_clause = ClauseList(
                *util.to_list(order_by),
                _literal_as_text_role=roles.OrderByRole
            )
        if group_by is not None:
            self._group_by_clause = ClauseList(
                *util.to_list(group_by), _literal_as_text_role=roles.ByOfRole
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

    def _offset_or_limit_clause(self, element, name=None, type_=None):
        """Convert the given value to an "offset or limit" clause.

        This handles incoming integers and converts to an expression; if
        an expression is already given, it is passed through.

        """
        return coercions.expect(
            roles.LimitOffsetRole, element, name=name, type_=type_
        )

    def _offset_or_limit_clause_asint(self, clause, attrname):
        """Convert the "offset or limit" clause of a select construct to an
        integer.

        This is only possible if the value is stored as a simple bound
        parameter. Otherwise, a compilation error is raised.

        """
        if clause is None:
            return None
        try:
            value = clause._limit_offset_value
        except AttributeError as err:
            util.raise_(
                exc.CompileError(
                    "This SELECT structure does not use a simple "
                    "integer value for %s" % attrname
                ),
                replace_context=err,
            )
        else:
            return util.asint(value)

    @property
    def _limit(self):
        """Get an integer value for the limit.  This should only be used
        by code that cannot support a limit as a BindParameter or
        other custom clause as it will throw an exception if the limit
        isn't currently set to an integer.

        """
        return self._offset_or_limit_clause_asint(self._limit_clause, "limit")

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
        return self._offset_or_limit_clause_asint(
            self._offset_clause, "offset"
        )

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

        self._limit_clause = self._offset_or_limit_clause(limit)

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

        self._offset_clause = self._offset_or_limit_clause(offset)

    @_generative
    def order_by(self, *clauses):
        r"""return a new selectable with the given list of ORDER BY
        criterion applied.

        e.g.::

            stmt = select([table]).order_by(table.c.id, table.c.name)

        :param \*order_by: a series of :class:`.ColumnElement` constructs
         which will be used to generate an ORDER BY clause.

        .. seealso::

            :ref:`core_tutorial_ordering`

        """

        if len(clauses) == 1 and clauses[0] is None:
            self._order_by_clause = ClauseList()
        else:
            if getattr(self, "_order_by_clause", None) is not None:
                clauses = list(self._order_by_clause) + list(clauses)
            self._order_by_clause = ClauseList(
                *clauses, _literal_as_text_role=roles.OrderByRole
            )

    @_generative
    def group_by(self, *clauses):
        r"""return a new selectable with the given list of GROUP BY
        criterion applied.

        e.g.::

            stmt = select([table.c.name, func.max(table.c.stat)]).\
            group_by(table.c.name)

        :param \*group_by: a series of :class:`.ColumnElement` constructs
         which will be used to generate an GROUP BY clause.

        .. seealso::

            :ref:`core_tutorial_ordering`

        """

        if len(clauses) == 1 and clauses[0] is None:
            self._group_by_clause = ClauseList()
        else:
            if getattr(self, "_group_by_clause", None) is not None:
                clauses = list(self._group_by_clause) + list(clauses)
            self._group_by_clause = ClauseList(
                *clauses, _literal_as_text_role=roles.ByOfRole
            )

    @property
    def _label_resolve_dict(self):
        raise NotImplementedError()


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

    _traverse_internals = [
        ("selects", InternalTraversal.dp_clauseelement_list),
        ("_limit_clause", InternalTraversal.dp_clauseelement),
        ("_offset_clause", InternalTraversal.dp_clauseelement),
        ("_order_by_clause", InternalTraversal.dp_clauseelement),
        ("_group_by_clause", InternalTraversal.dp_clauseelement),
        ("_for_update_arg", InternalTraversal.dp_clauseelement),
        ("keyword", InternalTraversal.dp_string),
    ] + SupportsCloneAnnotations._clone_annotations_traverse_internals

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
        self.selects = [
            coercions.expect(roles.CompoundElementRole, s).self_group(
                against=self
            )
            for s in selects
        ]

        GenerativeSelect.__init__(self, **kwargs)

    @SelectBase._memoized_property
    def _label_resolve_dict(self):
        # TODO: this is hacky and slow
        hacky_subquery = self.subquery()
        hacky_subquery.named_with_column = False
        d = dict((c.key, c) for c in hacky_subquery.c)
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
        # type: (Optional[Any]) -> FromClause
        return SelectStatementGrouping(self)

    def is_derived_from(self, fromclause):
        for s in self.selects:
            if s.is_derived_from(fromclause):
                return True
        return False

    def _generate_fromclause_column_proxies(self, subquery):

        # this is a slightly hacky thing - the union exports a
        # column that resembles just that of the *first* selectable.
        # to get at a "composite" column, particularly foreign keys,
        # you have to dig through the proxies collection which we
        # generate below.  We may want to improve upon this, such as
        # perhaps _make_proxy can accept a list of other columns
        # that are "shared" - schema.column can then copy all the
        # ForeignKeys in. this would allow the union() to have all
        # those fks too.
        select_0 = self.selects[0]
        if self.use_labels:
            select_0 = select_0.apply_labels()
        select_0._generate_fromclause_column_proxies(subquery)

        # hand-construct the "_proxies" collection to include all
        # derived columns place a 'weight' annotation corresponding
        # to how low in the list of select()s the column occurs, so
        # that the corresponding_column() operation can resolve
        # conflicts
        for subq_col, select_cols in zip(
            subquery.c._all_columns,
            zip(*[s.selected_columns for s in self.selects]),
        ):
            subq_col._proxies = [
                c._annotate({"weight": i + 1})
                for (i, c) in enumerate(select_cols)
            ]

    def _refresh_for_new_column(self, column):
        super(CompoundSelect, self)._refresh_for_new_column(column)
        for select in self.selects:
            select._refresh_for_new_column(column)

    @property
    def selected_columns(self):
        """A :class:`.ColumnCollection` representing the columns that
        this SELECT statement or similar construct returns in its result set.

        For a :class:`.CompoundSelect`, the
        :attr:`.CompoundSelect.selected_columns` attribute returns the selected
        columns of the first SELECT statement contained within the series of
        statements within the set operation.

        .. versionadded:: 1.4

        """
        return self.selects[0].selected_columns

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


class DeprecatedSelectGenerations(object):
    @util.deprecated(
        "1.4",
        "The :meth:`.Select.append_correlation` method is deprecated "
        "and will be removed in a future release.  Use the generative "
        "method :meth:`.Select.correlate`.",
    )
    def append_correlation(self, fromclause):
        """append the given correlation expression to this select()
        construct.

        This is an **in-place** mutation method; the
        :meth:`~.Select.correlate` method is preferred, as it provides
        standard :term:`method chaining`.

        """

        self.correlate.non_generative(self, fromclause)

    @util.deprecated(
        "1.4",
        "The :meth:`.Select.append_column` method is deprecated "
        "and will be removed in a future release.  Use the generative "
        "method :meth:`.Select.column`.",
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
        self.add_columns.non_generative(self, column)

    @util.deprecated(
        "1.4",
        "The :meth:`.Select.append_prefix` method is deprecated "
        "and will be removed in a future release.  Use the generative "
        "method :meth:`.Select.prefix_with`.",
    )
    def append_prefix(self, clause):
        """append the given columns clause prefix expression to this select()
        construct.

        This is an **in-place** mutation method; the
        :meth:`~.Select.prefix_with` method is preferred, as it provides
        standard :term:`method chaining`.

        """
        self.prefix_with.non_generative(self, clause)

    @util.deprecated(
        "1.4",
        "The :meth:`.Select.append_whereclause` method is deprecated "
        "and will be removed in a future release.  Use the generative "
        "method :meth:`.Select.where`.",
    )
    def append_whereclause(self, whereclause):
        """append the given expression to this select() construct's WHERE
        criterion.

        The expression will be joined to existing WHERE criterion via AND.

        This is an **in-place** mutation method; the
        :meth:`~.Select.where` method is preferred, as it provides standard
        :term:`method chaining`.

        """
        self.where.non_generative(self, whereclause)

    @util.deprecated(
        "1.4",
        "The :meth:`.Select.append_having` method is deprecated "
        "and will be removed in a future release.  Use the generative "
        "method :meth:`.Select.having`.",
    )
    def append_having(self, having):
        """append the given expression to this select() construct's HAVING
        criterion.

        The expression will be joined to existing HAVING criterion via AND.

        This is an **in-place** mutation method; the
        :meth:`~.Select.having` method is preferred, as it provides standard
        :term:`method chaining`.

        """

        self.having.non_generative(self, having)

    @util.deprecated(
        "1.4",
        "The :meth:`.Select.append_from` method is deprecated "
        "and will be removed in a future release.  Use the generative "
        "method :meth:`.Select.select_from`.",
    )
    def append_from(self, fromclause):
        """append the given FromClause expression to this select() construct's
        FROM clause.

        This is an **in-place** mutation method; the
        :meth:`~.Select.select_from` method is preferred, as it provides
        standard :term:`method chaining`.

        """
        self.select_from.non_generative(self, fromclause)


class Select(
    HasPrefixes, HasSuffixes, DeprecatedSelectGenerations, GenerativeSelect
):
    """Represents a ``SELECT`` statement.

    """

    __visit_name__ = "select"

    _prefixes = ()
    _suffixes = ()
    _hints = util.immutabledict()
    _statement_hints = ()
    _distinct = False
    _distinct_on = ()
    _correlate = ()
    _correlate_except = None
    _memoized_property = SelectBase._memoized_property

    _traverse_internals = (
        [
            ("_from_obj", InternalTraversal.dp_fromclause_ordered_set),
            ("_raw_columns", InternalTraversal.dp_clauseelement_list),
            ("_whereclause", InternalTraversal.dp_clauseelement),
            ("_having", InternalTraversal.dp_clauseelement),
            (
                "_order_by_clause.clauses",
                InternalTraversal.dp_clauseelement_list,
            ),
            (
                "_group_by_clause.clauses",
                InternalTraversal.dp_clauseelement_list,
            ),
            ("_correlate", InternalTraversal.dp_clauseelement_unordered_set),
            (
                "_correlate_except",
                InternalTraversal.dp_clauseelement_unordered_set,
            ),
            ("_for_update_arg", InternalTraversal.dp_clauseelement),
            ("_statement_hints", InternalTraversal.dp_statement_hint_list),
            ("_hints", InternalTraversal.dp_table_hint_list),
            ("_distinct", InternalTraversal.dp_boolean),
            ("_distinct_on", InternalTraversal.dp_clauseelement_list),
        ]
        + HasPrefixes._has_prefixes_traverse_internals
        + HasSuffixes._has_suffixes_traverse_internals
        + SupportsCloneAnnotations._clone_annotations_traverse_internals
    )

    @classmethod
    def _create_select(cls, *entities):
        r"""Construct a new :class:`.Select` using the 2.x style API.

        .. versionadded:: 2.0 - the :func:`.future.select` construct is
           the same construct as the one returned by
           :func:`.sql.expression.select`, except that the function only
           accepts the "columns clause" entities up front; the rest of the
           state of the SELECT should be built up using generative methods.

        Similar functionality is also available via the
        :meth:`.FromClause.select` method on any :class:`.FromClause`.

        .. seealso::

            :ref:`coretutorial_selecting` - Core Tutorial description of
            :func:`.select`.

        :param \*entities:
          Entities to SELECT from.  For Core usage, this is typically a series
          of :class:`.ColumnElement` and / or :class:`.FromClause`
          objects which will form the columns clause of the resulting
          statement.   For those objects that are instances of
          :class:`.FromClause` (typically :class:`.Table` or :class:`.Alias`
          objects), the :attr:`.FromClause.c` collection is extracted
          to form a collection of :class:`.ColumnElement` objects.

          This parameter will also accept :class:`.Text` constructs as
          given, as well as ORM-mapped classes.

        """

        self = cls.__new__(cls)
        self._raw_columns = [
            coercions.expect(roles.ColumnsClauseRole, ent)
            for ent in util.to_list(entities)
        ]

        # this should all go away once Select is converted to have
        # default state at the class level
        self._auto_correlate = True
        self._from_obj = util.OrderedSet()
        self._whereclause = None
        self._having = None

        GenerativeSelect.__init__(self)

        return self

    @classmethod
    def _create_select_from_fromclause(cls, target, entities, *arg, **kw):
        if arg or kw:
            util.warn_deprecated_20(
                "Passing arguments to %s.select() is deprecated and "
                "will be removed in SQLAlchemy 2.0.  Please use generative "
                "methods such as select().where(), etc."
                % (target.__class__.__name__,)
            )
            return Select(entities, *arg, **kw)
        else:
            return Select._create_select(*entities)

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
        """Construct a new :class:`.Select` using the 1.x style API.

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
        util.warn_deprecated_20(
            "The select() function in SQLAlchemy 2.0 will accept a "
            "series of columns / tables and other entities only, "
            "passed positionally. For forwards compatibility, use the "
            "sqlalchemy.future.select() construct.",
            stacklevel=4,
        )

        self._auto_correlate = correlate
        if distinct is not False:
            self._distinct = True
            if not isinstance(distinct, bool):
                self._distinct_on = tuple(
                    [
                        coercions.expect(roles.ByOfRole, e)
                        for e in util.to_list(distinct)
                    ]
                )

        if from_obj is not None:
            self._from_obj = util.OrderedSet(
                coercions.expect(roles.FromClauseRole, f)
                for f in util.to_list(from_obj)
            )
        else:
            self._from_obj = util.OrderedSet()

        try:
            cols_present = bool(columns)
        except TypeError as err:
            util.raise_(
                exc.ArgumentError(
                    "columns argument to select() must "
                    "be a Python list or other iterable"
                ),
                from_=err,
            )

        if cols_present:
            self._raw_columns = []
            for c in columns:
                c = coercions.expect(roles.ColumnsClauseRole, c)
                self._raw_columns.append(c)
        else:
            self._raw_columns = []

        if whereclause is not None:
            self._whereclause = coercions.expect(
                roles.WhereHavingRole, whereclause
            ).self_group(against=operators._asbool)
        else:
            self._whereclause = None

        if having is not None:
            self._having = coercions.expect(
                roles.WhereHavingRole, having
            ).self_group(against=operators._asbool)
        else:
            self._having = None

        if prefixes:
            self._setup_prefixes(prefixes)

        if suffixes:
            self._setup_suffixes(suffixes)

        GenerativeSelect.__init__(self, **kwargs)

    @property
    def _froms(self):
        # current roadblock to caching is two tests that test that the
        # SELECT can be compiled to a string, then a Table is created against
        # columns, then it can be compiled again and works.  this is somewhat
        # valid as people make select() against declarative class where
        # columns don't have their Table yet and perhaps some operations
        # call upon _froms and cache it too soon.
        froms = []
        seen = set()

        for item in itertools.chain(
            _from_objects(*self._raw_columns),
            _from_objects(self._whereclause)
            if self._whereclause is not None
            else (),
            self._from_obj,
        ):
            if item._is_subquery and item.element is self:
                raise exc.InvalidRequestError(
                    "select() construct refers to itself as a FROM"
                )
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

        all_the_froms = list(
            itertools.chain(
                _from_objects(*self._raw_columns),
                _from_objects(self._whereclause)
                if self._whereclause is not None
                else (),
            )
        )
        new_froms = {f: clone(f, **kw) for f in all_the_froms}
        # copy FROM collections

        self._from_obj = util.OrderedSet(
            clone(f, **kw) for f in self._from_obj
        ).union(f for f in new_froms.values() if isinstance(f, Join))

        self._correlate = set(clone(f, **kw) for f in self._correlate)
        if self._correlate_except:
            self._correlate_except = set(
                clone(f, **kw) for f in self._correlate_except
            )

        # 4. clone other things.   The difficulty here is that Column
        # objects are usually not altered by a straight clone because they
        # are dependent on the FROM cloning we just did above in order to
        # be targeted correctly, or a new FROM we have might be a JOIN
        # object which doesn't have its own columns.  so give the cloner a
        # hint.
        def replace(obj, **kw):
            if isinstance(obj, ColumnClause) and obj.table in new_froms:
                newelem = new_froms[obj.table].corresponding_column(obj)
                return newelem

        kw["replace"] = replace

        # TODO: I'd still like to try to leverage the traversal data
        self._raw_columns = [clone(c, **kw) for c in self._raw_columns]
        for attr in (
            "_limit_clause",
            "_offset_clause",
            "_whereclause",
            "_having",
            "_order_by_clause",
            "_group_by_clause",
            "_for_update_arg",
        ):
            if getattr(self, attr) is not None:
                setattr(self, attr, clone(getattr(self, attr), **kw))

        self._reset_memoizations()

    def get_children(self, **kwargs):
        # TODO: define "get_children" traversal items separately?
        return self._froms + super(Select, self).get_children(
            omit_attrs=["_from_obj", "_correlate", "_correlate_except"]
        )

    @_generative
    def add_columns(self, *columns):
        """return a new select() construct with the given column expressions
            added to its columns clause.

            E.g.::

                my_select = my_select.add_columns(table.c.new_column)

            See the documentation for :meth:`.Select.with_only_columns`
            for guidelines on adding /replacing the columns of a
            :class:`.Select` object.

        """
        self._reset_memoizations()

        self._raw_columns = self._raw_columns + [
            coercions.expect(roles.ColumnsClauseRole, column)
            for column in columns
        ]

    @util.deprecated(
        "1.4",
        "The :meth:`.Select.column` method is deprecated and will "
        "be removed in a future release.  Please use "
        ":meth:`.Select.add_columns",
    )
    def column(self, column):
        """return a new select() construct with the given column expression
            added to its columns clause.

            E.g.::

                my_select = my_select.column(table.c.new_column)

            See the documentation for :meth:`.Select.with_only_columns`
            for guidelines on adding /replacing the columns of a
            :class:`.Select` object.

        """
        return self.add_columns(column)

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
            >>> print(s1)
            SELECT t1.a, t2.b FROM t1, t2
            >>> s2 = s1.with_only_columns([table2.c.b])
            >>> print(s2)
            SELECT t2.b FROM t1

        The preferred way to maintain a specific FROM clause
        in the construct, assuming it won't be represented anywhere
        else (i.e. not in the WHERE clause, etc.) is to set it using
        :meth:`.Select.select_from`::

            >>> s1 = select([table1.c.a, table2.c.b]).\
            ...         select_from(table1.join(table2,
            ...                 table1.c.a==table2.c.a))
            >>> s2 = s1.with_only_columns([table2.c.b])
            >>> print(s2)
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
        self._reset_memoizations()
        rc = []
        for c in columns:
            c = coercions.expect(roles.ColumnsClauseRole, c)
            if isinstance(c, ScalarSelect):
                c = c.self_group(against=operators.comma_op)
            rc.append(c)
        self._raw_columns = rc

    @_generative
    def where(self, whereclause):
        """return a new select() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """

        self._reset_memoizations()
        self._whereclause = and_(True_._ifnone(self._whereclause), whereclause)

    @_generative
    def having(self, having):
        """return a new select() construct with the given expression added to
        its HAVING clause, joined to the existing clause via AND, if any.

        """
        self._reset_memoizations()
        self._having = and_(True_._ifnone(self._having), having)

    @_generative
    def distinct(self, *expr):
        r"""Return a new select() construct which will apply DISTINCT to its
        columns clause.

        :param \*expr: optional column expressions.  When present,
         the PostgreSQL dialect will render a ``DISTINCT ON (<expressions>>)``
         construct.

        """
        if expr:
            expr = [coercions.expect(roles.ByOfRole, e) for e in expr]
            self._distinct = True
            self._distinct_on = self._distinct_on + tuple(expr)
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
        self._reset_memoizations()
        fromclause = coercions.expect(roles.FromClauseRole, fromclause)
        self._from_obj = self._from_obj.union([fromclause])

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
                coercions.expect(roles.FromClauseRole, f) for f in fromclauses
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
                coercions.expect(roles.FromClauseRole, f) for f in fromclauses
            )

    @_memoized_property
    def selected_columns(self):
        """A :class:`.ColumnCollection` representing the columns that
        this SELECT statement or similar construct returns in its result set.

        This collection differs from the :attr:`.FromClause.columns` collection
        of a :class:`.FromClause` in that the columns within this collection
        cannot be directly nested inside another SELECT statement; a subquery
        must be applied first which provides for the necessary parenthesization
        required by SQL.

        For a :func:`.select` construct, the collection here is exactly what
        would be rendered inside the  "SELECT" statement, and the
        :class:`.ColumnElement` objects are  directly present as they were
        given, e.g.::

            col1 = column('q', Integer)
            col2 = column('p', Integer)
            stmt = select([col1, col2])

        Above, ``stmt.selected_columns`` would be a collection that contains
        the ``col1`` and ``col2`` objects directly.    For a statement that is
        against a :class:`.Table` or other :class:`.FromClause`, the collection
        will use the :class:`.ColumnElement` objects that are in the
        :attr:`.FromClause.c` collection of the from element.

        .. versionadded:: 1.4

        """
        names = set()

        cols = _select_iterables(self._raw_columns)

        def name_for_col(c):
            # we use key_label since this name is intended for targeting
            # within the ColumnCollection only, it's not related to SQL
            # rendering which always uses column name for SQL label names
            if self.use_labels:
                name = c._key_label
            else:
                name = c._proxy_key
            if name in names:
                if self.use_labels:
                    name = c._label_anon_label
                else:
                    name = c.anon_label
            else:
                names.add(name)
            return name

        return ColumnCollection(
            (name_for_col(c), c) for c in cols
        ).as_immutable()

    def _generate_columns_plus_names(self, anon_for_dupe_key):
        cols = _select_iterables(self._raw_columns)

        # when use_labels is on:
        # in all cases == if we see the same label name, use _label_anon_label
        # for subsequent occurences of that label
        #
        # anon_for_dupe_key == if we see the same column object multiple
        # times under a particular name, whether it's the _label name or the
        # anon label, apply _dedupe_label_anon_label to the subsequent
        # occurrences of it.

        if self.use_labels:
            names = {}

            def name_for_col(c):
                if c._label is None or not c._render_label_in_columns_clause:
                    return (None, c, False)

                repeated = False
                name = c._label

                if name in names:
                    # when looking to see if names[name] is the same column as
                    # c, use hash(), so that an annotated version of the column
                    # is seen as the same as the non-annotated
                    if hash(names[name]) != hash(c):

                        # different column under the same name.  apply
                        # disambiguating label
                        name = c._label_anon_label

                        if anon_for_dupe_key and name in names:
                            # here, c._label_anon_label is definitely unique to
                            # that column identity (or annotated version), so
                            # this should always be true.
                            # this is also an infrequent codepath because
                            # you need two levels of duplication to be here
                            assert hash(names[name]) == hash(c)

                            # the column under the disambiguating label is
                            # already present.  apply the "dedupe" label to
                            # subsequent occurrences of the column so that the
                            # original stays non-ambiguous
                            name = c._dedupe_label_anon_label
                            repeated = True
                        else:
                            names[name] = c
                    elif anon_for_dupe_key:
                        # same column under the same name. apply the "dedupe"
                        # label so that the original stays non-ambiguous
                        name = c._dedupe_label_anon_label
                        repeated = True
                else:
                    names[name] = c
                return name, c, repeated

            return [name_for_col(c) for c in cols]
        else:
            # repeated name logic only for use labels at the moment
            return [(None, c, False) for c in cols]

    @_memoized_property
    def _columns_plus_names(self):
        """generate label names plus columns to render in a SELECT."""

        return self._generate_columns_plus_names(True)

    def _generate_fromclause_column_proxies(self, subquery):
        """generate column proxies to place in the exported .c collection
        of a subquery."""

        keys_seen = set()
        prox = []

        for name, c, repeated in self._generate_columns_plus_names(False):
            if not hasattr(c, "_make_proxy"):
                continue
            if name is None:
                key = None
            elif self.use_labels:
                key = c._key_label
                if key is not None and key in keys_seen:
                    key = c._label_anon_label
                keys_seen.add(key)
            else:
                key = None
            prox.append(
                c._make_proxy(
                    subquery, key=key, name=name, name_is_truncatable=True
                )
            )
        subquery._columns._populate_separate_keys(prox)

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
        else:
            return SelectStatementGrouping(self)

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


class ScalarSelect(roles.InElementRole, Generative, Grouping):
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
            s = Select(*args, **kwargs).scalar_subquery()

        UnaryExpression.__init__(
            self,
            s,
            operator=operators.exists,
            type_=type_api.BOOLEANTYPE,
            wraps_column_expression=True,
        )

    def _regroup(self, fn):
        element = self.element._ungroup()
        element = fn(element)
        return element.self_group(against=operators.exists)

    def select(self, whereclause=None, **params):
        if whereclause is not None:
            params["whereclause"] = whereclause
        return Select._create_select_from_fromclause(self, [self], **params)

    def correlate(self, *fromclause):
        e = self._clone()
        e.element = self._regroup(
            lambda element: element.correlate(*fromclause)
        )
        return e

    def correlate_except(self, *fromclause):
        e = self._clone()
        e.element = self._regroup(
            lambda element: element.correlate_except(*fromclause)
        )
        return e

    def select_from(self, clause):
        """return a new :class:`.Exists` construct, applying the given
        expression to the :meth:`.Select.select_from` method of the select
        statement contained.

        """
        e = self._clone()
        e.element = self._regroup(lambda element: element.select_from(clause))
        return e

    def where(self, clause):
        """return a new exists() construct with the given expression added to
        its WHERE clause, joined to the existing clause via AND, if any.

        """
        e = self._clone()
        e.element = self._regroup(lambda element: element.where(clause))
        return e


class TextualSelect(SelectBase):
    """Wrap a :class:`.TextClause` construct within a :class:`.SelectBase`
    interface.

    This allows the :class:`.TextClause` object to gain a ``.c`` collection
    and other FROM-like capabilities such as :meth:`.FromClause.alias`,
    :meth:`.SelectBase.cte`, etc.

    The :class:`.TextualSelect` construct is produced via the
    :meth:`.TextClause.columns` method - see that method for details.

    .. versionchanged:: 1.4 the :class:`.TextualSelect` class was renamed
       from ``TextAsFrom``, to more correctly suit its role as a
       SELECT-oriented object and not a FROM clause.

    .. seealso::

        :func:`.text`

        :meth:`.TextClause.columns` - primary creation interface.

    """

    __visit_name__ = "textual_select"

    _traverse_internals = [
        ("element", InternalTraversal.dp_clauseelement),
        ("column_args", InternalTraversal.dp_clauseelement_list),
    ] + SupportsCloneAnnotations._clone_annotations_traverse_internals

    _is_textual = True

    def __init__(self, text, columns, positional=False):
        self.element = text
        # convert for ORM attributes->columns, etc
        self.column_args = [
            coercions.expect(roles.ColumnsClauseRole, c) for c in columns
        ]
        self.positional = positional

    @SelectBase._memoized_property
    def selected_columns(self):
        """A :class:`.ColumnCollection` representing the columns that
        this SELECT statement or similar construct returns in its result set.

        This collection differs from the :attr:`.FromClause.columns` collection
        of a :class:`.FromClause` in that the columns within this collection
        cannot be directly nested inside another SELECT statement; a subquery
        must be applied first which provides for the necessary parenthesization
        required by SQL.

        For a :class:`.TextualSelect` construct, the collection contains the
        :class:`.ColumnElement` objects that were passed to the constructor,
        typically via the :meth:`.TextClause.columns` method.

        .. versionadded:: 1.4

        """
        return ColumnCollection(
            (c.key, c) for c in self.column_args
        ).as_immutable()

    @property
    def _bind(self):
        return self.element._bind

    @_generative
    def bindparams(self, *binds, **bind_as_values):
        self.element = self.element.bindparams(*binds, **bind_as_values)

    def _generate_fromclause_column_proxies(self, fromclause):
        fromclause._columns._populate_separate_keys(
            c._make_proxy(fromclause) for c in self.column_args
        )

    def _scalar_type(self):
        return self.column_args[0].type


TextAsFrom = TextualSelect
"""Backwards compatibility with the previous name"""


class AnnotatedFromClause(Annotated):
    def __init__(self, element, values):
        # force FromClause to generate their internal
        # collections into __dict__
        element.c
        Annotated.__init__(self, element, values)
