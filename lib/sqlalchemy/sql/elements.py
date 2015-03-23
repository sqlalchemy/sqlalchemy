# sql/elements.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Core SQL expression elements, including :class:`.ClauseElement`,
:class:`.ColumnElement`, and derived classes.

"""

from __future__ import unicode_literals

from .. import util, exc, inspection
from . import type_api
from . import operators
from .visitors import Visitable, cloned_traverse, traverse
from .annotation import Annotated
import itertools
from .base import Executable, PARSE_AUTOCOMMIT, Immutable, NO_ARG
from .base import _generative
import numbers

import re
import operator


def _clone(element, **kw):
    return element._clone()


def collate(expression, collation):
    """Return the clause ``expression COLLATE collation``.

    e.g.::

        collate(mycolumn, 'utf8_bin')

    produces::

        mycolumn COLLATE utf8_bin

    """

    expr = _literal_as_binds(expression)
    return BinaryExpression(
        expr,
        _literal_as_text(collation),
        operators.collate, type_=expr.type)


def between(expr, lower_bound, upper_bound, symmetric=False):
    """Produce a ``BETWEEN`` predicate clause.

    E.g.::

        from sqlalchemy import between
        stmt = select([users_table]).where(between(users_table.c.id, 5, 7))

    Would produce SQL resembling::

        SELECT id, name FROM user WHERE id BETWEEN :id_1 AND :id_2

    The :func:`.between` function is a standalone version of the
    :meth:`.ColumnElement.between` method available on all
    SQL expressions, as in::

        stmt = select([users_table]).where(users_table.c.id.between(5, 7))

    All arguments passed to :func:`.between`, including the left side
    column expression, are coerced from Python scalar values if a
    the value is not a :class:`.ColumnElement` subclass.   For example,
    three fixed values can be compared as in::

        print(between(5, 3, 7))

    Which would produce::

        :param_1 BETWEEN :param_2 AND :param_3

    :param expr: a column expression, typically a :class:`.ColumnElement`
     instance or alternatively a Python scalar expression to be coerced
     into a column expression, serving as the left side of the ``BETWEEN``
     expression.

    :param lower_bound: a column or Python scalar expression serving as the
     lower bound of the right side of the ``BETWEEN`` expression.

    :param upper_bound: a column or Python scalar expression serving as the
     upper bound of the right side of the ``BETWEEN`` expression.

    :param symmetric: if True, will render " BETWEEN SYMMETRIC ". Note
     that not all databases support this syntax.

     .. versionadded:: 0.9.5

    .. seealso::

        :meth:`.ColumnElement.between`

    """
    expr = _literal_as_binds(expr)
    return expr.between(lower_bound, upper_bound, symmetric=symmetric)


def literal(value, type_=None):
    """Return a literal clause, bound to a bind parameter.

    Literal clauses are created automatically when non-
    :class:`.ClauseElement` objects (such as strings, ints, dates, etc.) are
    used in a comparison operation with a :class:`.ColumnElement` subclass,
    such as a :class:`~sqlalchemy.schema.Column` object.  Use this function
    to force the generation of a literal clause, which will be created as a
    :class:`BindParameter` with a bound value.

    :param value: the value to be bound. Can be any Python object supported by
        the underlying DB-API, or is translatable via the given type argument.

    :param type\_: an optional :class:`~sqlalchemy.types.TypeEngine` which
        will provide bind-parameter translation for this literal.

    """
    return BindParameter(None, value, type_=type_, unique=True)


def type_coerce(expression, type_):
    """Associate a SQL expression with a particular type, without rendering
    ``CAST``.

    E.g.::

        from sqlalchemy import type_coerce

        stmt = select([type_coerce(log_table.date_string, StringDateTime())])

    The above construct will produce SQL that is usually otherwise unaffected
    by the :func:`.type_coerce` call::

        SELECT date_string FROM log

    However, when result rows are fetched, the ``StringDateTime`` type
    will be applied to result rows on behalf of the ``date_string`` column.

    A type that features bound-value handling will also have that behavior
    take effect when literal values or :func:`.bindparam` constructs are
    passed to :func:`.type_coerce` as targets.
    For example, if a type implements the :meth:`.TypeEngine.bind_expression`
    method or :meth:`.TypeEngine.bind_processor` method or equivalent,
    these functions will take effect at statement compilation/execution time
    when a literal value is passed, as in::

        # bound-value handling of MyStringType will be applied to the
        # literal value "some string"
        stmt = select([type_coerce("some string", MyStringType)])

    :func:`.type_coerce` is similar to the :func:`.cast` function,
    except that it does not render the ``CAST`` expression in the resulting
    statement.

    :param expression: A SQL expression, such as a :class:`.ColumnElement`
     expression or a Python string which will be coerced into a bound literal
     value.

    :param type_: A :class:`.TypeEngine` class or instance indicating
     the type to which the expression is coerced.

    .. seealso::

        :func:`.cast`

    """
    type_ = type_api.to_instance(type_)

    if hasattr(expression, '__clause_element__'):
        return type_coerce(expression.__clause_element__(), type_)
    elif isinstance(expression, BindParameter):
        bp = expression._clone()
        bp.type = type_
        return bp
    elif not isinstance(expression, Visitable):
        if expression is None:
            return Null()
        else:
            return literal(expression, type_=type_)
    else:
        return Label(None, expression, type_=type_)


def outparam(key, type_=None):
    """Create an 'OUT' parameter for usage in functions (stored procedures),
    for databases which support them.

    The ``outparam`` can be used like a regular function parameter.
    The "output" value will be available from the
    :class:`~sqlalchemy.engine.ResultProxy` object via its ``out_parameters``
    attribute, which returns a dictionary containing the values.

    """
    return BindParameter(
        key, None, type_=type_, unique=False, isoutparam=True)


def not_(clause):
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The ``~`` operator is also overloaded on all
    :class:`.ColumnElement` subclasses to produce the
    same result.

    """
    return operators.inv(_literal_as_binds(clause))


@inspection._self_inspects
class ClauseElement(Visitable):
    """Base class for elements of a programmatically constructed SQL
    expression.

    """
    __visit_name__ = 'clause'

    _annotations = {}
    supports_execution = False
    _from_objects = []
    bind = None
    _is_clone_of = None
    is_selectable = False
    is_clause_element = True

    description = None
    _order_by_label_element = None
    _is_from_container = False

    def _clone(self):
        """Create a shallow copy of this ClauseElement.

        This method may be used by a generative API.  Its also used as
        part of the "deep" copy afforded by a traversal that combines
        the _copy_internals() method.

        """
        c = self.__class__.__new__(self.__class__)
        c.__dict__ = self.__dict__.copy()
        ClauseElement._cloned_set._reset(c)
        ColumnElement.comparator._reset(c)

        # this is a marker that helps to "equate" clauses to each other
        # when a Select returns its list of FROM clauses.  the cloning
        # process leaves around a lot of remnants of the previous clause
        # typically in the form of column expressions still attached to the
        # old table.
        c._is_clone_of = self

        return c

    @property
    def _constructor(self):
        """return the 'constructor' for this ClauseElement.

        This is for the purposes for creating a new object of
        this type.   Usually, its just the element's __class__.
        However, the "Annotated" version of the object overrides
        to return the class of its proxied element.

        """
        return self.__class__

    @util.memoized_property
    def _cloned_set(self):
        """Return the set consisting all cloned ancestors of this
        ClauseElement.

        Includes this ClauseElement.  This accessor tends to be used for
        FromClause objects to identify 'equivalent' FROM clauses, regardless
        of transformative operations.

        """
        s = util.column_set()
        f = self
        while f is not None:
            s.add(f)
            f = f._is_clone_of
        return s

    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop('_is_clone_of', None)
        return d

    def _annotate(self, values):
        """return a copy of this ClauseElement with annotations
        updated by the given dictionary.

        """
        return Annotated(self, values)

    def _with_annotations(self, values):
        """return a copy of this ClauseElement with annotations
        replaced by the given dictionary.

        """
        return Annotated(self, values)

    def _deannotate(self, values=None, clone=False):
        """return a copy of this :class:`.ClauseElement` with annotations
        removed.

        :param values: optional tuple of individual values
         to remove.

        """
        if clone:
            # clone is used when we are also copying
            # the expression for a deep deannotation
            return self._clone()
        else:
            # if no clone, since we have no annotations we return
            # self
            return self

    def _execute_on_connection(self, connection, multiparams, params):
        return connection._execute_clauseelement(self, multiparams, params)

    def unique_params(self, *optionaldict, **kwargs):
        """Return a copy with :func:`bindparam()` elements replaced.

        Same functionality as ``params()``, except adds `unique=True`
        to affected bind parameters so that multiple statements can be
        used.

        """
        return self._params(True, optionaldict, kwargs)

    def params(self, *optionaldict, **kwargs):
        """Return a copy with :func:`bindparam()` elements replaced.

        Returns a copy of this ClauseElement with :func:`bindparam()`
        elements replaced with values taken from the given dictionary::

          >>> clause = column('x') + bindparam('foo')
          >>> print clause.compile().params
          {'foo':None}
          >>> print clause.params({'foo':7}).compile().params
          {'foo':7}

        """
        return self._params(False, optionaldict, kwargs)

    def _params(self, unique, optionaldict, kwargs):
        if len(optionaldict) == 1:
            kwargs.update(optionaldict[0])
        elif len(optionaldict) > 1:
            raise exc.ArgumentError(
                "params() takes zero or one positional dictionary argument")

        def visit_bindparam(bind):
            if bind.key in kwargs:
                bind.value = kwargs[bind.key]
                bind.required = False
            if unique:
                bind._convert_to_unique()
        return cloned_traverse(self, {}, {'bindparam': visit_bindparam})

    def compare(self, other, **kw):
        """Compare this ClauseElement to the given ClauseElement.

        Subclasses should override the default behavior, which is a
        straight identity comparison.

        \**kw are arguments consumed by subclass compare() methods and
        may be used to modify the criteria for comparison.
        (see :class:`.ColumnElement`)

        """
        return self is other

    def _copy_internals(self, clone=_clone, **kw):
        """Reassign internal elements to be clones of themselves.

        Called during a copy-and-traverse operation on newly
        shallow-copied elements to create a deep copy.

        The given clone function should be used, which may be applying
        additional transformations to the element (i.e. replacement
        traversal, cloned traversal, annotations).

        """
        pass

    def get_children(self, **kwargs):
        """Return immediate child elements of this :class:`.ClauseElement`.

        This is used for visit traversal.

        \**kwargs may contain flags that change the collection that is
        returned, for example to return a subset of items in order to
        cut down on larger traversals, or to return child items from a
        different context (such as schema-level collections instead of
        clause-level).

        """
        return []

    def self_group(self, against=None):
        """Apply a 'grouping' to this :class:`.ClauseElement`.

        This method is overridden by subclasses to return a
        "grouping" construct, i.e. parenthesis.   In particular
        it's used by "binary" expressions to provide a grouping
        around themselves when placed into a larger expression,
        as well as by :func:`.select` constructs when placed into
        the FROM clause of another :func:`.select`.  (Note that
        subqueries should be normally created using the
        :meth:`.Select.alias` method, as many platforms require
        nested SELECT statements to be named).

        As expressions are composed together, the application of
        :meth:`self_group` is automatic - end-user code should never
        need to use this method directly.  Note that SQLAlchemy's
        clause constructs take operator precedence into account -
        so parenthesis might not be needed, for example, in
        an expression like ``x OR (y AND z)`` - AND takes precedence
        over OR.

        The base :meth:`self_group` method of :class:`.ClauseElement`
        just returns self.
        """
        return self

    @util.dependencies("sqlalchemy.engine.default")
    def compile(self, default, bind=None, dialect=None, **kw):
        """Compile this SQL expression.

        The return value is a :class:`~.Compiled` object.
        Calling ``str()`` or ``unicode()`` on the returned value will yield a
        string representation of the result. The
        :class:`~.Compiled` object also can return a
        dictionary of bind parameter names and values
        using the ``params`` accessor.

        :param bind: An ``Engine`` or ``Connection`` from which a
            ``Compiled`` will be acquired. This argument takes precedence over
            this :class:`.ClauseElement`'s bound engine, if any.

        :param column_keys: Used for INSERT and UPDATE statements, a list of
            column names which should be present in the VALUES clause of the
            compiled statement. If ``None``, all columns from the target table
            object are rendered.

        :param dialect: A ``Dialect`` instance from which a ``Compiled``
            will be acquired. This argument takes precedence over the `bind`
            argument as well as this :class:`.ClauseElement`'s bound engine,
            if any.

        :param inline: Used for INSERT statements, for a dialect which does
            not support inline retrieval of newly generated primary key
            columns, will force the expression used to create the new primary
            key value to be rendered inline within the INSERT statement's
            VALUES clause. This typically refers to Sequence execution but may
            also refer to any server-side default generation function
            associated with a primary key `Column`.

        :param compile_kwargs: optional dictionary of additional parameters
            that will be passed through to the compiler within all "visit"
            methods.  This allows any custom flag to be passed through to
            a custom compilation construct, for example.  It is also used
            for the case of passing the ``literal_binds`` flag through::

                from sqlalchemy.sql import table, column, select

                t = table('t', column('x'))

                s = select([t]).where(t.c.x == 5)

                print s.compile(compile_kwargs={"literal_binds": True})

            .. versionadded:: 0.9.0

        .. seealso::

            :ref:`faq_sql_expression_string`

        """

        if not dialect:
            if bind:
                dialect = bind.dialect
            elif self.bind:
                dialect = self.bind.dialect
                bind = self.bind
            else:
                dialect = default.DefaultDialect()
        return self._compiler(dialect, bind=bind, **kw)

    def _compiler(self, dialect, **kw):
        """Return a compiler appropriate for this ClauseElement, given a
        Dialect."""

        return dialect.statement_compiler(dialect, self, **kw)

    def __str__(self):
        if util.py3k:
            return str(self.compile())
        else:
            return unicode(self.compile()).encode('ascii', 'backslashreplace')

    def __and__(self, other):
        """'and' at the ClauseElement level.

        .. deprecated:: 0.9.5 - conjunctions are intended to be
           at the :class:`.ColumnElement`. level

        """
        return and_(self, other)

    def __or__(self, other):
        """'or' at the ClauseElement level.

        .. deprecated:: 0.9.5 - conjunctions are intended to be
           at the :class:`.ColumnElement`. level

        """
        return or_(self, other)

    def __invert__(self):
        if hasattr(self, 'negation_clause'):
            return self.negation_clause
        else:
            return self._negate()

    def _negate(self):
        return UnaryExpression(
            self.self_group(against=operators.inv),
            operator=operators.inv,
            negate=None)

    def __bool__(self):
        raise TypeError("Boolean value of this clause is not defined")

    __nonzero__ = __bool__

    def __repr__(self):
        friendly = self.description
        if friendly is None:
            return object.__repr__(self)
        else:
            return '<%s.%s at 0x%x; %s>' % (
                self.__module__, self.__class__.__name__, id(self), friendly)


class ColumnElement(operators.ColumnOperators, ClauseElement):
    """Represent a column-oriented SQL expression suitable for usage in the
    "columns" clause, WHERE clause etc. of a statement.

    While the most familiar kind of :class:`.ColumnElement` is the
    :class:`.Column` object, :class:`.ColumnElement` serves as the basis
    for any unit that may be present in a SQL expression, including
    the expressions themselves, SQL functions, bound parameters,
    literal expressions, keywords such as ``NULL``, etc.
    :class:`.ColumnElement` is the ultimate base class for all such elements.

    A wide variety of SQLAlchemy Core functions work at the SQL expression
    level, and are intended to accept instances of :class:`.ColumnElement` as
    arguments.  These functions will typically document that they accept a
    "SQL expression" as an argument.  What this means in terms of SQLAlchemy
    usually refers to an input which is either already in the form of a
    :class:`.ColumnElement` object, or a value which can be **coerced** into
    one.  The coercion rules followed by most, but not all, SQLAlchemy Core
    functions with regards to SQL expressions are as follows:

        * a literal Python value, such as a string, integer or floating
          point value, boolean, datetime, ``Decimal`` object, or virtually
          any other Python object, will be coerced into a "literal bound
          value".  This generally means that a :func:`.bindparam` will be
          produced featuring the given value embedded into the construct; the
          resulting :class:`.BindParameter` object is an instance of
          :class:`.ColumnElement`.  The Python value will ultimately be sent
          to the DBAPI at execution time as a paramterized argument to the
          ``execute()`` or ``executemany()`` methods, after SQLAlchemy
          type-specific converters (e.g. those provided by any associated
          :class:`.TypeEngine` objects) are applied to the value.

        * any special object value, typically ORM-level constructs, which
          feature a method called ``__clause_element__()``.  The Core
          expression system looks for this method when an object of otherwise
          unknown type is passed to a function that is looking to coerce the
          argument into a :class:`.ColumnElement` expression.  The
          ``__clause_element__()`` method, if present, should return a
          :class:`.ColumnElement` instance.  The primary use of
          ``__clause_element__()`` within SQLAlchemy is that of class-bound
          attributes on ORM-mapped classes; a ``User`` class which contains a
          mapped attribute named ``.name`` will have a method
          ``User.name.__clause_element__()`` which when invoked returns the
          :class:`.Column` called ``name`` associated with the mapped table.

        * The Python ``None`` value is typically interpreted as ``NULL``,
          which in SQLAlchemy Core produces an instance of :func:`.null`.

    A :class:`.ColumnElement` provides the ability to generate new
    :class:`.ColumnElement`
    objects using Python expressions.  This means that Python operators
    such as ``==``, ``!=`` and ``<`` are overloaded to mimic SQL operations,
    and allow the instantiation of further :class:`.ColumnElement` instances
    which are composed from other, more fundamental :class:`.ColumnElement`
    objects.  For example, two :class:`.ColumnClause` objects can be added
    together with the addition operator ``+`` to produce
    a :class:`.BinaryExpression`.
    Both :class:`.ColumnClause` and :class:`.BinaryExpression` are subclasses
    of :class:`.ColumnElement`::

        >>> from sqlalchemy.sql import column
        >>> column('a') + column('b')
        <sqlalchemy.sql.expression.BinaryExpression object at 0x101029dd0>
        >>> print column('a') + column('b')
        a + b

    .. seealso::

        :class:`.Column`

        :func:`.expression.column`

    """

    __visit_name__ = 'column'
    primary_key = False
    foreign_keys = []

    _label = None
    """The named label that can be used to target
    this column in a result set.

    This label is almost always the label used when
    rendering <expr> AS <label> in a SELECT statement.  It also
    refers to a name that this column expression can be located from
    in a result set.

    For a regular Column bound to a Table, this is typically the label
    <tablename>_<columnname>.  For other constructs, different rules
    may apply, such as anonymized labels and others.

    """

    key = None
    """the 'key' that in some circumstances refers to this object in a
    Python namespace.

    This typically refers to the "key" of the column as present in the
    ``.c`` collection of a selectable, e.g. sometable.c["somekey"] would
    return a Column with a .key of "somekey".

    """

    _key_label = None
    """A label-based version of 'key' that in some circumstances refers
    to this object in a Python namespace.


    _key_label comes into play when a select() statement is constructed with
    apply_labels(); in this case, all Column objects in the ``.c`` collection
    are rendered as <tablename>_<columnname> in SQL; this is essentially the
    value of ._label.  But to locate those columns in the ``.c`` collection,
    the name is along the lines of <tablename>_<key>; that's the typical
    value of .key_label.

    """

    _render_label_in_columns_clause = True
    """A flag used by select._columns_plus_names that helps to determine
    we are actually going to render in terms of "SELECT <col> AS <label>".
    This flag can be returned as False for some Column objects that want
    to be rendered as simple "SELECT <col>"; typically columns that don't have
    any parent table and are named the same as what the label would be
    in any case.

    """

    _resolve_label = None
    """The name that should be used to identify this ColumnElement in a
    select() object when "label resolution" logic is used; this refers
    to using a string name in an expression like order_by() or group_by()
    that wishes to target a labeled expression in the columns clause.

    The name is distinct from that of .name or ._label to account for the case
    where anonymizing logic may be used to change the name that's actually
    rendered at compile time; this attribute should hold onto the original
    name that was user-assigned when producing a .label() construct.

    """

    _allow_label_resolve = True
    """A flag that can be flipped to prevent a column from being resolvable
    by string label name."""

    _alt_names = ()

    def self_group(self, against=None):
        if (against in (operators.and_, operators.or_, operators._asbool) and
                self.type._type_affinity
                is type_api.BOOLEANTYPE._type_affinity):
            return AsBoolean(self, operators.istrue, operators.isfalse)
        else:
            return self

    def _negate(self):
        if self.type._type_affinity is type_api.BOOLEANTYPE._type_affinity:
            return AsBoolean(self, operators.isfalse, operators.istrue)
        else:
            return super(ColumnElement, self)._negate()

    @util.memoized_property
    def type(self):
        return type_api.NULLTYPE

    @util.memoized_property
    def comparator(self):
        return self.type.comparator_factory(self)

    def __getattr__(self, key):
        try:
            return getattr(self.comparator, key)
        except AttributeError:
            raise AttributeError(
                'Neither %r object nor %r object has an attribute %r' % (
                    type(self).__name__,
                    type(self.comparator).__name__,
                    key)
            )

    def operate(self, op, *other, **kwargs):
        return op(self.comparator, *other, **kwargs)

    def reverse_operate(self, op, other, **kwargs):
        return op(other, self.comparator, **kwargs)

    def _bind_param(self, operator, obj):
        return BindParameter(None, obj,
                             _compared_to_operator=operator,
                             _compared_to_type=self.type, unique=True)

    @property
    def expression(self):
        """Return a column expression.

        Part of the inspection interface; returns self.

        """
        return self

    @property
    def _select_iterable(self):
        return (self, )

    @util.memoized_property
    def base_columns(self):
        return util.column_set(c for c in self.proxy_set
                               if not hasattr(c, '_proxies'))

    @util.memoized_property
    def proxy_set(self):
        s = util.column_set([self])
        if hasattr(self, '_proxies'):
            for c in self._proxies:
                s.update(c.proxy_set)
        return s

    def shares_lineage(self, othercolumn):
        """Return True if the given :class:`.ColumnElement`
        has a common ancestor to this :class:`.ColumnElement`."""

        return bool(self.proxy_set.intersection(othercolumn.proxy_set))

    def _compare_name_for_result(self, other):
        """Return True if the given column element compares to this one
        when targeting within a result row."""

        return hasattr(other, 'name') and hasattr(self, 'name') and \
            other.name == self.name

    def _make_proxy(
            self, selectable, name=None, name_is_truncatable=False, **kw):
        """Create a new :class:`.ColumnElement` representing this
        :class:`.ColumnElement` as it appears in the select list of a
        descending selectable.

        """
        if name is None:
            name = self.anon_label
            if self.key:
                key = self.key
            else:
                try:
                    key = str(self)
                except exc.UnsupportedCompilationError:
                    key = self.anon_label

        else:
            key = name
        co = ColumnClause(
            _as_truncated(name) if name_is_truncatable else name,
            type_=getattr(self, 'type', None),
            _selectable=selectable
        )
        co._proxies = [self]
        if selectable._is_clone_of is not None:
            co._is_clone_of = \
                selectable._is_clone_of.columns.get(key)
        selectable._columns[key] = co
        return co

    def compare(self, other, use_proxies=False, equivalents=None, **kw):
        """Compare this ColumnElement to another.

        Special arguments understood:

        :param use_proxies: when True, consider two columns that
          share a common base column as equivalent (i.e. shares_lineage())

        :param equivalents: a dictionary of columns as keys mapped to sets
          of columns. If the given "other" column is present in this
          dictionary, if any of the columns in the corresponding set() pass
          the comparison test, the result is True. This is used to expand the
          comparison to other columns that may be known to be equivalent to
          this one via foreign key or other criterion.

        """
        to_compare = (other, )
        if equivalents and other in equivalents:
            to_compare = equivalents[other].union(to_compare)

        for oth in to_compare:
            if use_proxies and self.shares_lineage(oth):
                return True
            elif hash(oth) == hash(self):
                return True
        else:
            return False

    def label(self, name):
        """Produce a column label, i.e. ``<columnname> AS <name>``.

        This is a shortcut to the :func:`~.expression.label` function.

        if 'name' is None, an anonymous label name will be generated.

        """
        return Label(name, self, self.type)

    @util.memoized_property
    def anon_label(self):
        """provides a constant 'anonymous label' for this ColumnElement.

        This is a label() expression which will be named at compile time.
        The same label() is returned each time anon_label is called so
        that expressions can reference anon_label multiple times, producing
        the same label name at compile time.

        the compiler uses this function automatically at compile time
        for expressions that are known to be 'unnamed' like binary
        expressions and function calls.

        """
        while self._is_clone_of is not None:
            self = self._is_clone_of

        return _anonymous_label(
            '%%(%d %s)s' % (id(self), getattr(self, 'name', 'anon'))
        )


class BindParameter(ColumnElement):
    """Represent a "bound expression".

    :class:`.BindParameter` is invoked explicitly using the
    :func:`.bindparam` function, as in::

        from sqlalchemy import bindparam

        stmt = select([users_table]).\\
                    where(users_table.c.name == bindparam('username'))

    Detailed discussion of how :class:`.BindParameter` is used is
    at :func:`.bindparam`.

    .. seealso::

        :func:`.bindparam`

    """

    __visit_name__ = 'bindparam'

    _is_crud = False

    def __init__(self, key, value=NO_ARG, type_=None,
                 unique=False, required=NO_ARG,
                 quote=None, callable_=None,
                 isoutparam=False,
                 _compared_to_operator=None,
                 _compared_to_type=None):
        """Produce a "bound expression".

        The return value is an instance of :class:`.BindParameter`; this
        is a :class:`.ColumnElement` subclass which represents a so-called
        "placeholder" value in a SQL expression, the value of which is
        supplied at the point at which the statement in executed against a
        database connection.

        In SQLAlchemy, the :func:`.bindparam` construct has
        the ability to carry along the actual value that will be ultimately
        used at expression time.  In this way, it serves not just as
        a "placeholder" for eventual population, but also as a means of
        representing so-called "unsafe" values which should not be rendered
        directly in a SQL statement, but rather should be passed along
        to the :term:`DBAPI` as values which need to be correctly escaped
        and potentially handled for type-safety.

        When using :func:`.bindparam` explicitly, the use case is typically
        one of traditional deferment of parameters; the :func:`.bindparam`
        construct accepts a name which can then be referred to at execution
        time::

            from sqlalchemy import bindparam

            stmt = select([users_table]).\\
                        where(users_table.c.name == bindparam('username'))

        The above statement, when rendered, will produce SQL similar to::

            SELECT id, name FROM user WHERE name = :username

        In order to populate the value of ``:username`` above, the value
        would typically be applied at execution time to a method
        like :meth:`.Connection.execute`::

            result = connection.execute(stmt, username='wendy')

        Explicit use of :func:`.bindparam` is also common when producing
        UPDATE or DELETE statements that are to be invoked multiple times,
        where the WHERE criterion of the statement is to change on each
        invocation, such as::

            stmt = (users_table.update().
                    where(user_table.c.name == bindparam('username')).
                    values(fullname=bindparam('fullname'))
                    )

            connection.execute(
                stmt, [{"username": "wendy", "fullname": "Wendy Smith"},
                       {"username": "jack", "fullname": "Jack Jones"},
                       ]
            )

        SQLAlchemy's Core expression system makes wide use of
        :func:`.bindparam` in an implicit sense.   It is typical that Python
        literal values passed to virtually all SQL expression functions are
        coerced into fixed :func:`.bindparam` constructs.  For example, given
        a comparison operation such as::

            expr = users_table.c.name == 'Wendy'

        The above expression will produce a :class:`.BinaryExpression`
        construct, where the left side is the :class:`.Column` object
        representing the ``name`` column, and the right side is a
        :class:`.BindParameter` representing the literal value::

            print(repr(expr.right))
            BindParameter('%(4327771088 name)s', 'Wendy', type_=String())

        The expression above will render SQL such as::

            user.name = :name_1

        Where the ``:name_1`` parameter name is an anonymous name.  The
        actual string ``Wendy`` is not in the rendered string, but is carried
        along where it is later used within statement execution.  If we
        invoke a statement like the following::

            stmt = select([users_table]).where(users_table.c.name == 'Wendy')
            result = connection.execute(stmt)

        We would see SQL logging output as::

            SELECT "user".id, "user".name
            FROM "user"
            WHERE "user".name = %(name_1)s
            {'name_1': 'Wendy'}

        Above, we see that ``Wendy`` is passed as a parameter to the database,
        while the placeholder ``:name_1`` is rendered in the appropriate form
        for the target database, in this case the Postgresql database.

        Similarly, :func:`.bindparam` is invoked automatically
        when working with :term:`CRUD` statements as far as the "VALUES"
        portion is concerned.   The :func:`.insert` construct produces an
        ``INSERT`` expression which will, at statement execution time,
        generate bound placeholders based on the arguments passed, as in::

            stmt = users_table.insert()
            result = connection.execute(stmt, name='Wendy')

        The above will produce SQL output as::

            INSERT INTO "user" (name) VALUES (%(name)s)
            {'name': 'Wendy'}

        The :class:`.Insert` construct, at compilation/execution time,
        rendered a single :func:`.bindparam` mirroring the column
        name ``name`` as a result of the single ``name`` parameter
        we passed to the :meth:`.Connection.execute` method.

        :param key:
          the key (e.g. the name) for this bind param.
          Will be used in the generated
          SQL statement for dialects that use named parameters.  This
          value may be modified when part of a compilation operation,
          if other :class:`BindParameter` objects exist with the same
          key, or if its length is too long and truncation is
          required.

        :param value:
          Initial value for this bind param.  Will be used at statement
          execution time as the value for this parameter passed to the
          DBAPI, if no other value is indicated to the statement execution
          method for this particular parameter name.  Defaults to ``None``.

        :param callable\_:
          A callable function that takes the place of "value".  The function
          will be called at statement execution time to determine the
          ultimate value.   Used for scenarios where the actual bind
          value cannot be determined at the point at which the clause
          construct is created, but embedded bind values are still desirable.

        :param type\_:
          A :class:`.TypeEngine` class or instance representing an optional
          datatype for this :func:`.bindparam`.  If not passed, a type
          may be determined automatically for the bind, based on the given
          value; for example, trivial Python types such as ``str``,
          ``int``, ``bool``
          may result in the :class:`.String`, :class:`.Integer` or
          :class:`.Boolean` types being autoamtically selected.

          The type of a :func:`.bindparam` is significant especially in that
          the type will apply pre-processing to the value before it is
          passed to the database.  For example, a :func:`.bindparam` which
          refers to a datetime value, and is specified as holding the
          :class:`.DateTime` type, may apply conversion needed to the
          value (such as stringification on SQLite) before passing the value
          to the database.

        :param unique:
          if True, the key name of this :class:`.BindParameter` will be
          modified if another :class:`.BindParameter` of the same name
          already has been located within the containing
          expression.  This flag is used generally by the internals
          when producing so-called "anonymous" bound expressions, it
          isn't generally applicable to explicitly-named :func:`.bindparam`
          constructs.

        :param required:
          If ``True``, a value is required at execution time.  If not passed,
          it defaults to ``True`` if neither :paramref:`.bindparam.value`
          or :paramref:`.bindparam.callable` were passed.  If either of these
          parameters are present, then :paramref:`.bindparam.required`
          defaults to ``False``.

          .. versionchanged:: 0.8 If the ``required`` flag is not specified,
             it will be set automatically to ``True`` or ``False`` depending
             on whether or not the ``value`` or ``callable`` parameters
             were specified.

        :param quote:
          True if this parameter name requires quoting and is not
          currently known as a SQLAlchemy reserved word; this currently
          only applies to the Oracle backend, where bound names must
          sometimes be quoted.

        :param isoutparam:
          if True, the parameter should be treated like a stored procedure
          "OUT" parameter.  This applies to backends such as Oracle which
          support OUT parameters.

        .. seealso::

            :ref:`coretutorial_bind_param`

            :ref:`coretutorial_insert_expressions`

            :func:`.outparam`

        """
        if isinstance(key, ColumnClause):
            type_ = key.type
            key = key.key
        if required is NO_ARG:
            required = (value is NO_ARG and callable_ is None)
        if value is NO_ARG:
            value = None

        if quote is not None:
            key = quoted_name(key, quote)

        if unique:
            self.key = _anonymous_label('%%(%d %s)s' % (id(self), key
                                                        or 'param'))
        else:
            self.key = key or _anonymous_label('%%(%d param)s'
                                               % id(self))

        # identifying key that won't change across
        # clones, used to identify the bind's logical
        # identity
        self._identifying_key = self.key

        # key that was passed in the first place, used to
        # generate new keys
        self._orig_key = key or 'param'

        self.unique = unique
        self.value = value
        self.callable = callable_
        self.isoutparam = isoutparam
        self.required = required
        if type_ is None:
            if _compared_to_type is not None:
                self.type = \
                    _compared_to_type.coerce_compared_value(
                        _compared_to_operator, value)
            else:
                self.type = type_api._type_map.get(type(value),
                                                   type_api.NULLTYPE)
        elif isinstance(type_, type):
            self.type = type_()
        else:
            self.type = type_

    def _with_value(self, value):
        """Return a copy of this :class:`.BindParameter` with the given value
        set.
        """
        cloned = self._clone()
        cloned.value = value
        cloned.callable = None
        cloned.required = False
        if cloned.type is type_api.NULLTYPE:
            cloned.type = type_api._type_map.get(type(value),
                                                 type_api.NULLTYPE)
        return cloned

    @property
    def effective_value(self):
        """Return the value of this bound parameter,
        taking into account if the ``callable`` parameter
        was set.

        The ``callable`` value will be evaluated
        and returned if present, else ``value``.

        """
        if self.callable:
            return self.callable()
        else:
            return self.value

    def _clone(self):
        c = ClauseElement._clone(self)
        if self.unique:
            c.key = _anonymous_label('%%(%d %s)s' % (id(c), c._orig_key
                                                     or 'param'))
        return c

    def _convert_to_unique(self):
        if not self.unique:
            self.unique = True
            self.key = _anonymous_label(
                '%%(%d %s)s' % (id(self), self._orig_key or 'param'))

    def compare(self, other, **kw):
        """Compare this :class:`BindParameter` to the given
        clause."""

        return isinstance(other, BindParameter) \
            and self.type._compare_type_affinity(other.type) \
            and self.value == other.value

    def __getstate__(self):
        """execute a deferred value for serialization purposes."""

        d = self.__dict__.copy()
        v = self.value
        if self.callable:
            v = self.callable()
            d['callable'] = None
        d['value'] = v
        return d

    def __repr__(self):
        return 'BindParameter(%r, %r, type_=%r)' % (self.key,
                                                    self.value, self.type)


class TypeClause(ClauseElement):
    """Handle a type keyword in a SQL statement.

    Used by the ``Case`` statement.

    """

    __visit_name__ = 'typeclause'

    def __init__(self, type):
        self.type = type


class TextClause(Executable, ClauseElement):
    """Represent a literal SQL text fragment.

    E.g.::

        from sqlalchemy import text

        t = text("SELECT * FROM users")
        result = connection.execute(t)


    The :class:`.Text` construct is produced using the :func:`.text`
    function; see that function for full documentation.

    .. seealso::

        :func:`.text`

    """

    __visit_name__ = 'textclause'

    _bind_params_regex = re.compile(r'(?<![:\w\x5c]):(\w+)(?!:)', re.UNICODE)
    _execution_options = \
        Executable._execution_options.union(
            {'autocommit': PARSE_AUTOCOMMIT})

    @property
    def _select_iterable(self):
        return (self,)

    @property
    def selectable(self):
        return self

    _hide_froms = []

    # help in those cases where text() is
    # interpreted in a column expression situation
    key = _label = _resolve_label = None

    _allow_label_resolve = False

    def __init__(
            self,
            text,
            bind=None):
        self._bind = bind
        self._bindparams = {}

        def repl(m):
            self._bindparams[m.group(1)] = BindParameter(m.group(1))
            return ':%s' % m.group(1)

        # scan the string and search for bind parameter names, add them
        # to the list of bindparams
        self.text = self._bind_params_regex.sub(repl, text)

    @classmethod
    def _create_text(self, text, bind=None, bindparams=None,
                     typemap=None, autocommit=None):
        """Construct a new :class:`.TextClause` clause, representing
        a textual SQL string directly.

        E.g.::

            from sqlalchemy import text

            t = text("SELECT * FROM users")
            result = connection.execute(t)

        The advantages :func:`.text` provides over a plain string are
        backend-neutral support for bind parameters, per-statement
        execution options, as well as
        bind parameter and result-column typing behavior, allowing
        SQLAlchemy type constructs to play a role when executing
        a statement that is specified literally.  The construct can also
        be provided with a ``.c`` collection of column elements, allowing
        it to be embedded in other SQL expression constructs as a subquery.

        Bind parameters are specified by name, using the format ``:name``.
        E.g.::

            t = text("SELECT * FROM users WHERE id=:user_id")
            result = connection.execute(t, user_id=12)

        For SQL statements where a colon is required verbatim, as within
        an inline string, use a backslash to escape::

            t = text("SELECT * FROM users WHERE name='\\:username'")

        The :class:`.TextClause` construct includes methods which can
        provide information about the bound parameters as well as the column
        values which would be returned from the textual statement, assuming
        it's an executable SELECT type of statement.  The
        :meth:`.TextClause.bindparams` method is used to provide bound
        parameter detail, and :meth:`.TextClause.columns` method allows
        specification of return columns including names and types::

            t = text("SELECT * FROM users WHERE id=:user_id").\\
                    bindparams(user_id=7).\\
                    columns(id=Integer, name=String)

            for id, name in connection.execute(t):
                print(id, name)

        The :func:`.text` construct is used internally in cases when
        a literal string is specified for part of a larger query, such as
        when a string is specified to the :meth:`.Select.where` method of
        :class:`.Select`.  In those cases, the same
        bind parameter syntax is applied::

            s = select([users.c.id, users.c.name]).where("id=:user_id")
            result = connection.execute(s, user_id=12)

        Using :func:`.text` explicitly usually implies the construction
        of a full, standalone statement.   As such, SQLAlchemy refers
        to it as an :class:`.Executable` object, and it supports
        the :meth:`Executable.execution_options` method.  For example,
        a :func:`.text` construct that should be subject to "autocommit"
        can be set explicitly so using the
        :paramref:`.Connection.execution_options.autocommit` option::

            t = text("EXEC my_procedural_thing()").\\
                    execution_options(autocommit=True)

        Note that SQLAlchemy's usual "autocommit" behavior applies to
        :func:`.text` constructs implicitly - that is, statements which begin
        with a phrase such as ``INSERT``, ``UPDATE``, ``DELETE``,
        or a variety of other phrases specific to certain backends, will
        be eligible for autocommit if no transaction is in progress.

        :param text:
          the text of the SQL statement to be created.  use ``:<param>``
          to specify bind parameters; they will be compiled to their
          engine-specific format.

        :param autocommit:
          Deprecated.  Use .execution_options(autocommit=<True|False>)
          to set the autocommit option.

        :param bind:
          an optional connection or engine to be used for this text query.

        :param bindparams:
          Deprecated.  A list of :func:`.bindparam` instances used to
          provide information about parameters embedded in the statement.
          This argument now invokes the :meth:`.TextClause.bindparams`
          method on the construct before returning it.  E.g.::

              stmt = text("SELECT * FROM table WHERE id=:id",
                        bindparams=[bindparam('id', value=5, type_=Integer)])

          Is equivalent to::

              stmt = text("SELECT * FROM table WHERE id=:id").\\
                        bindparams(bindparam('id', value=5, type_=Integer))

          .. deprecated:: 0.9.0 the :meth:`.TextClause.bindparams` method
             supersedes the ``bindparams`` argument to :func:`.text`.

        :param typemap:
          Deprecated.  A dictionary mapping the names of columns
          represented in the columns clause of a ``SELECT`` statement
          to type objects,
          which will be used to perform post-processing on columns within
          the result set.  This parameter now invokes the
          :meth:`.TextClause.columns` method, which returns a
          :class:`.TextAsFrom` construct that gains a ``.c`` collection and
          can be embedded in other expressions.  E.g.::

              stmt = text("SELECT * FROM table",
                            typemap={'id': Integer, 'name': String},
                        )

          Is equivalent to::

              stmt = text("SELECT * FROM table").columns(id=Integer,
                                                         name=String)

          Or alternatively::

              from sqlalchemy.sql import column
              stmt = text("SELECT * FROM table").columns(
                                    column('id', Integer),
                                    column('name', String)
                                )

          .. deprecated:: 0.9.0 the :meth:`.TextClause.columns` method
             supersedes the ``typemap`` argument to :func:`.text`.

        """
        stmt = TextClause(text, bind=bind)
        if bindparams:
            stmt = stmt.bindparams(*bindparams)
        if typemap:
            stmt = stmt.columns(**typemap)
        if autocommit is not None:
            util.warn_deprecated('autocommit on text() is deprecated.  '
                                 'Use .execution_options(autocommit=True)')
            stmt = stmt.execution_options(autocommit=autocommit)

        return stmt

    @_generative
    def bindparams(self, *binds, **names_to_values):
        """Establish the values and/or types of bound parameters within
        this :class:`.TextClause` construct.

        Given a text construct such as::

            from sqlalchemy import text
            stmt = text("SELECT id, name FROM user WHERE name=:name "
                        "AND timestamp=:timestamp")

        the :meth:`.TextClause.bindparams` method can be used to establish
        the initial value of ``:name`` and ``:timestamp``,
        using simple keyword arguments::

            stmt = stmt.bindparams(name='jack',
                        timestamp=datetime.datetime(2012, 10, 8, 15, 12, 5))

        Where above, new :class:`.BindParameter` objects
        will be generated with the names ``name`` and ``timestamp``, and
        values of ``jack`` and ``datetime.datetime(2012, 10, 8, 15, 12, 5)``,
        respectively.  The types will be
        inferred from the values given, in this case :class:`.String` and
        :class:`.DateTime`.

        When specific typing behavior is needed, the positional ``*binds``
        argument can be used in which to specify :func:`.bindparam` constructs
        directly.  These constructs must include at least the ``key``
        argument, then an optional value and type::

            from sqlalchemy import bindparam
            stmt = stmt.bindparams(
                            bindparam('name', value='jack', type_=String),
                            bindparam('timestamp', type_=DateTime)
                        )

        Above, we specified the type of :class:`.DateTime` for the
        ``timestamp`` bind, and the type of :class:`.String` for the ``name``
        bind.  In the case of ``name`` we also set the default value of
        ``"jack"``.

        Additional bound parameters can be supplied at statement execution
        time, e.g.::

            result = connection.execute(stmt,
                        timestamp=datetime.datetime(2012, 10, 8, 15, 12, 5))

        The :meth:`.TextClause.bindparams` method can be called repeatedly,
        where it will re-use existing :class:`.BindParameter` objects to add
        new information.  For example, we can call
        :meth:`.TextClause.bindparams` first with typing information, and a
        second time with value information, and it will be combined::

            stmt = text("SELECT id, name FROM user WHERE name=:name "
                        "AND timestamp=:timestamp")
            stmt = stmt.bindparams(
                bindparam('name', type_=String),
                bindparam('timestamp', type_=DateTime)
            )
            stmt = stmt.bindparams(
                name='jack',
                timestamp=datetime.datetime(2012, 10, 8, 15, 12, 5)
            )


        .. versionadded:: 0.9.0 The :meth:`.TextClause.bindparams` method
           supersedes the argument ``bindparams`` passed to
           :func:`~.expression.text`.


        """
        self._bindparams = new_params = self._bindparams.copy()

        for bind in binds:
            try:
                existing = new_params[bind.key]
            except KeyError:
                raise exc.ArgumentError(
                    "This text() construct doesn't define a "
                    "bound parameter named %r" % bind.key)
            else:
                new_params[existing.key] = bind

        for key, value in names_to_values.items():
            try:
                existing = new_params[key]
            except KeyError:
                raise exc.ArgumentError(
                    "This text() construct doesn't define a "
                    "bound parameter named %r" % key)
            else:
                new_params[key] = existing._with_value(value)

    @util.dependencies('sqlalchemy.sql.selectable')
    def columns(self, selectable, *cols, **types):
        """Turn this :class:`.TextClause` object into a :class:`.TextAsFrom`
        object that can be embedded into another statement.

        This function essentially bridges the gap between an entirely
        textual SELECT statement and the SQL expression language concept
        of a "selectable"::

            from sqlalchemy.sql import column, text

            stmt = text("SELECT id, name FROM some_table")
            stmt = stmt.columns(column('id'), column('name')).alias('st')

            stmt = select([mytable]).\\
                    select_from(
                        mytable.join(stmt, mytable.c.name == stmt.c.name)
                    ).where(stmt.c.id > 5)

        Above, we used untyped :func:`.column` elements.  These can also have
        types specified, which will impact how the column behaves in
        expressions as well as determining result set behavior::

            stmt = text("SELECT id, name, timestamp FROM some_table")
            stmt = stmt.columns(
                        column('id', Integer),
                        column('name', Unicode),
                        column('timestamp', DateTime)
                    )

            for id, name, timestamp in connection.execute(stmt):
                print(id, name, timestamp)

        Keyword arguments allow just the names and types of columns to be
        specified, where the :func:`.column` elements will be generated
        automatically::

            stmt = text("SELECT id, name, timestamp FROM some_table")
            stmt = stmt.columns(
                        id=Integer,
                        name=Unicode,
                        timestamp=DateTime
                    )

            for id, name, timestamp in connection.execute(stmt):
                print(id, name, timestamp)

        The :meth:`.TextClause.columns` method provides a direct
        route to calling :meth:`.FromClause.alias` as well as
        :meth:`.SelectBase.cte` against a textual SELECT statement::

            stmt = stmt.columns(id=Integer, name=String).cte('st')

            stmt = select([sometable]).where(sometable.c.id == stmt.c.id)

        .. versionadded:: 0.9.0 :func:`.text` can now be converted into a
           fully featured "selectable" construct using the
           :meth:`.TextClause.columns` method.  This method supersedes the
           ``typemap`` argument to :func:`.text`.

        """

        input_cols = [
            ColumnClause(col.key, types.pop(col.key))
            if col.key in types
            else col
            for col in cols
        ] + [ColumnClause(key, type_) for key, type_ in types.items()]
        return selectable.TextAsFrom(self, input_cols)

    @property
    def type(self):
        return type_api.NULLTYPE

    @property
    def comparator(self):
        return self.type.comparator_factory(self)

    def self_group(self, against=None):
        if against is operators.in_op:
            return Grouping(self)
        else:
            return self

    def _copy_internals(self, clone=_clone, **kw):
        self._bindparams = dict((b.key, clone(b, **kw))
                                for b in self._bindparams.values())

    def get_children(self, **kwargs):
        return list(self._bindparams.values())

    def compare(self, other):
        return isinstance(other, TextClause) and other.text == self.text


class Null(ColumnElement):
    """Represent the NULL keyword in a SQL statement.

    :class:`.Null` is accessed as a constant via the
    :func:`.null` function.

    """

    __visit_name__ = 'null'

    @util.memoized_property
    def type(self):
        return type_api.NULLTYPE

    @classmethod
    def _instance(cls):
        """Return a constant :class:`.Null` construct."""

        return Null()

    def compare(self, other):
        return isinstance(other, Null)


class False_(ColumnElement):
    """Represent the ``false`` keyword, or equivalent, in a SQL statement.

    :class:`.False_` is accessed as a constant via the
    :func:`.false` function.

    """

    __visit_name__ = 'false'

    @util.memoized_property
    def type(self):
        return type_api.BOOLEANTYPE

    def _negate(self):
        return True_()

    @classmethod
    def _instance(cls):
        """Return a :class:`.False_` construct.

        E.g.::

            >>> from sqlalchemy import false
            >>> print select([t.c.x]).where(false())
            SELECT x FROM t WHERE false

        A backend which does not support true/false constants will render as
        an expression against 1 or 0::

            >>> print select([t.c.x]).where(false())
            SELECT x FROM t WHERE 0 = 1

        The :func:`.true` and :func:`.false` constants also feature
        "short circuit" operation within an :func:`.and_` or :func:`.or_`
        conjunction::

            >>> print select([t.c.x]).where(or_(t.c.x > 5, true()))
            SELECT x FROM t WHERE true

            >>> print select([t.c.x]).where(and_(t.c.x > 5, false()))
            SELECT x FROM t WHERE false

        .. versionchanged:: 0.9 :func:`.true` and :func:`.false` feature
           better integrated behavior within conjunctions and on dialects
           that don't support true/false constants.

        .. seealso::

            :func:`.true`

        """

        return False_()

    def compare(self, other):
        return isinstance(other, False_)


class True_(ColumnElement):
    """Represent the ``true`` keyword, or equivalent, in a SQL statement.

    :class:`.True_` is accessed as a constant via the
    :func:`.true` function.

    """

    __visit_name__ = 'true'

    @util.memoized_property
    def type(self):
        return type_api.BOOLEANTYPE

    def _negate(self):
        return False_()

    @classmethod
    def _ifnone(cls, other):
        if other is None:
            return cls._instance()
        else:
            return other

    @classmethod
    def _instance(cls):
        """Return a constant :class:`.True_` construct.

        E.g.::

            >>> from sqlalchemy import true
            >>> print select([t.c.x]).where(true())
            SELECT x FROM t WHERE true

        A backend which does not support true/false constants will render as
        an expression against 1 or 0::

            >>> print select([t.c.x]).where(true())
            SELECT x FROM t WHERE 1 = 1

        The :func:`.true` and :func:`.false` constants also feature
        "short circuit" operation within an :func:`.and_` or :func:`.or_`
        conjunction::

            >>> print select([t.c.x]).where(or_(t.c.x > 5, true()))
            SELECT x FROM t WHERE true

            >>> print select([t.c.x]).where(and_(t.c.x > 5, false()))
            SELECT x FROM t WHERE false

        .. versionchanged:: 0.9 :func:`.true` and :func:`.false` feature
           better integrated behavior within conjunctions and on dialects
           that don't support true/false constants.

        .. seealso::

            :func:`.false`

        """

        return True_()

    def compare(self, other):
        return isinstance(other, True_)


class ClauseList(ClauseElement):
    """Describe a list of clauses, separated by an operator.

    By default, is comma-separated, such as a column listing.

    """
    __visit_name__ = 'clauselist'

    def __init__(self, *clauses, **kwargs):
        self.operator = kwargs.pop('operator', operators.comma_op)
        self.group = kwargs.pop('group', True)
        self.group_contents = kwargs.pop('group_contents', True)
        text_converter = kwargs.pop(
            '_literal_as_text',
            _expression_literal_as_text)
        if self.group_contents:
            self.clauses = [
                text_converter(clause).self_group(against=self.operator)
                for clause in clauses]
        else:
            self.clauses = [
                text_converter(clause)
                for clause in clauses]

    def __iter__(self):
        return iter(self.clauses)

    def __len__(self):
        return len(self.clauses)

    @property
    def _select_iterable(self):
        return iter(self)

    def append(self, clause):
        if self.group_contents:
            self.clauses.append(_literal_as_text(clause).
                                self_group(against=self.operator))
        else:
            self.clauses.append(_literal_as_text(clause))

    def _copy_internals(self, clone=_clone, **kw):
        self.clauses = [clone(clause, **kw) for clause in self.clauses]

    def get_children(self, **kwargs):
        return self.clauses

    @property
    def _from_objects(self):
        return list(itertools.chain(*[c._from_objects for c in self.clauses]))

    def self_group(self, against=None):
        if self.group and operators.is_precedent(self.operator, against):
            return Grouping(self)
        else:
            return self

    def compare(self, other, **kw):
        """Compare this :class:`.ClauseList` to the given :class:`.ClauseList`,
        including a comparison of all the clause items.

        """
        if not isinstance(other, ClauseList) and len(self.clauses) == 1:
            return self.clauses[0].compare(other, **kw)
        elif isinstance(other, ClauseList) and \
                len(self.clauses) == len(other.clauses):
            for i in range(0, len(self.clauses)):
                if not self.clauses[i].compare(other.clauses[i], **kw):
                    return False
            else:
                return self.operator == other.operator
        else:
            return False


class BooleanClauseList(ClauseList, ColumnElement):
    __visit_name__ = 'clauselist'

    def __init__(self, *arg, **kw):
        raise NotImplementedError(
            "BooleanClauseList has a private constructor")

    @classmethod
    def _construct(cls, operator, continue_on, skip_on, *clauses, **kw):
        convert_clauses = []

        clauses = util.coerce_generator_arg(clauses)
        for clause in clauses:
            clause = _expression_literal_as_text(clause)

            if isinstance(clause, continue_on):
                continue
            elif isinstance(clause, skip_on):
                return clause.self_group(against=operators._asbool)

            convert_clauses.append(clause)

        if len(convert_clauses) == 1:
            return convert_clauses[0].self_group(against=operators._asbool)
        elif not convert_clauses and clauses:
            return clauses[0].self_group(against=operators._asbool)

        convert_clauses = [c.self_group(against=operator)
                           for c in convert_clauses]

        self = cls.__new__(cls)
        self.clauses = convert_clauses
        self.group = True
        self.operator = operator
        self.group_contents = True
        self.type = type_api.BOOLEANTYPE
        return self

    @classmethod
    def and_(cls, *clauses):
        """Produce a conjunction of expressions joined by ``AND``.

        E.g.::

            from sqlalchemy import and_

            stmt = select([users_table]).where(
                            and_(
                                users_table.c.name == 'wendy',
                                users_table.c.enrolled == True
                            )
                        )

        The :func:`.and_` conjunction is also available using the
        Python ``&`` operator (though note that compound expressions
        need to be parenthesized in order to function with Python
        operator precedence behavior)::

            stmt = select([users_table]).where(
                            (users_table.c.name == 'wendy') &
                            (users_table.c.enrolled == True)
                        )

        The :func:`.and_` operation is also implicit in some cases;
        the :meth:`.Select.where` method for example can be invoked multiple
        times against a statement, which will have the effect of each
        clause being combined using :func:`.and_`::

            stmt = select([users_table]).\\
                        where(users_table.c.name == 'wendy').\\
                        where(users_table.c.enrolled == True)

        .. seealso::

            :func:`.or_`

        """
        return cls._construct(operators.and_, True_, False_, *clauses)

    @classmethod
    def or_(cls, *clauses):
        """Produce a conjunction of expressions joined by ``OR``.

        E.g.::

            from sqlalchemy import or_

            stmt = select([users_table]).where(
                            or_(
                                users_table.c.name == 'wendy',
                                users_table.c.name == 'jack'
                            )
                        )

        The :func:`.or_` conjunction is also available using the
        Python ``|`` operator (though note that compound expressions
        need to be parenthesized in order to function with Python
        operator precedence behavior)::

            stmt = select([users_table]).where(
                            (users_table.c.name == 'wendy') |
                            (users_table.c.name == 'jack')
                        )

        .. seealso::

            :func:`.and_`

        """
        return cls._construct(operators.or_, False_, True_, *clauses)

    @property
    def _select_iterable(self):
        return (self, )

    def self_group(self, against=None):
        if not self.clauses:
            return self
        else:
            return super(BooleanClauseList, self).self_group(against=against)

    def _negate(self):
        return ClauseList._negate(self)


and_ = BooleanClauseList.and_
or_ = BooleanClauseList.or_


class Tuple(ClauseList, ColumnElement):
    """Represent a SQL tuple."""

    def __init__(self, *clauses, **kw):
        """Return a :class:`.Tuple`.

        Main usage is to produce a composite IN construct::

            from sqlalchemy import tuple_

            tuple_(table.c.col1, table.c.col2).in_(
                [(1, 2), (5, 12), (10, 19)]
            )

        .. warning::

            The composite IN construct is not supported by all backends,
            and is currently known to work on Postgresql and MySQL,
            but not SQLite.   Unsupported backends will raise
            a subclass of :class:`~sqlalchemy.exc.DBAPIError` when such
            an expression is invoked.

        """

        clauses = [_literal_as_binds(c) for c in clauses]
        self._type_tuple = [arg.type for arg in clauses]
        self.type = kw.pop('type_', self._type_tuple[0]
                           if self._type_tuple else type_api.NULLTYPE)

        super(Tuple, self).__init__(*clauses, **kw)

    @property
    def _select_iterable(self):
        return (self, )

    def _bind_param(self, operator, obj):
        return Tuple(*[
            BindParameter(None, o, _compared_to_operator=operator,
                          _compared_to_type=type_, unique=True)
            for o, type_ in zip(obj, self._type_tuple)
        ]).self_group()


class Case(ColumnElement):
    """Represent a ``CASE`` expression.

    :class:`.Case` is produced using the :func:`.case` factory function,
    as in::

        from sqlalchemy import case

        stmt = select([users_table]).\\
                    where(
                        case(
                            [
                                (users_table.c.name == 'wendy', 'W'),
                                (users_table.c.name == 'jack', 'J')
                            ],
                            else_='E'
                        )
                    )

    Details on :class:`.Case` usage is at :func:`.case`.

    .. seealso::

        :func:`.case`

    """

    __visit_name__ = 'case'

    def __init__(self, whens, value=None, else_=None):
        """Produce a ``CASE`` expression.

        The ``CASE`` construct in SQL is a conditional object that
        acts somewhat analogously to an "if/then" construct in other
        languages.  It returns an instance of :class:`.Case`.

        :func:`.case` in its usual form is passed a list of "when"
        constructs, that is, a list of conditions and results as tuples::

            from sqlalchemy import case

            stmt = select([users_table]).\\
                        where(
                            case(
                                [
                                    (users_table.c.name == 'wendy', 'W'),
                                    (users_table.c.name == 'jack', 'J')
                                ],
                                else_='E'
                            )
                        )

        The above statement will produce SQL resembling::

            SELECT id, name FROM user
            WHERE CASE
                WHEN (name = :name_1) THEN :param_1
                WHEN (name = :name_2) THEN :param_2
                ELSE :param_3
            END

        When simple equality expressions of several values against a single
        parent column are needed, :func:`.case` also has a "shorthand" format
        used via the
        :paramref:`.case.value` parameter, which is passed a column
        expression to be compared.  In this form, the :paramref:`.case.whens`
        parameter is passed as a dictionary containing expressions to be
        compared against keyed to result expressions.  The statement below is
        equivalent to the preceding statement::

            stmt = select([users_table]).\\
                        where(
                            case(
                                {"wendy": "W", "jack": "J"},
                                value=users_table.c.name,
                                else_='E'
                            )
                        )

        The values which are accepted as result values in
        :paramref:`.case.whens` as well as with :paramref:`.case.else_` are
        coerced from Python literals into :func:`.bindparam` constructs.
        SQL expressions, e.g. :class:`.ColumnElement` constructs, are accepted
        as well.  To coerce a literal string expression into a constant
        expression rendered inline, use the :func:`.literal_column` construct,
        as in::

            from sqlalchemy import case, literal_column

            case(
                [
                    (
                        orderline.c.qty > 100,
                        literal_column("'greaterthan100'")
                    ),
                    (
                        orderline.c.qty > 10,
                        literal_column("'greaterthan10'")
                    )
                ],
                else_=literal_column("'lessthan10'")
            )

        The above will render the given constants without using bound
        parameters for the result values (but still for the comparison
        values), as in::

            CASE
                WHEN (orderline.qty > :qty_1) THEN 'greaterthan100'
                WHEN (orderline.qty > :qty_2) THEN 'greaterthan10'
                ELSE 'lessthan10'
            END

        :param whens: The criteria to be compared against,
         :paramref:`.case.whens` accepts two different forms, based on
         whether or not :paramref:`.case.value` is used.

         In the first form, it accepts a list of 2-tuples; each 2-tuple
         consists of ``(<sql expression>, <value>)``, where the SQL
         expression is a boolean expression and "value" is a resulting value,
         e.g.::

            case([
                (users_table.c.name == 'wendy', 'W'),
                (users_table.c.name == 'jack', 'J')
            ])

         In the second form, it accepts a Python dictionary of comparison
         values mapped to a resulting value; this form requires
         :paramref:`.case.value` to be present, and values will be compared
         using the ``==`` operator, e.g.::

            case(
                {"wendy": "W", "jack": "J"},
                value=users_table.c.name
            )

        :param value: An optional SQL expression which will be used as a
          fixed "comparison point" for candidate values within a dictionary
          passed to :paramref:`.case.whens`.

        :param else\_: An optional SQL expression which will be the evaluated
          result of the ``CASE`` construct if all expressions within
          :paramref:`.case.whens` evaluate to false.  When omitted, most
          databases will produce a result of NULL if none of the "when"
          expressions evaluate to true.


        """

        try:
            whens = util.dictlike_iteritems(whens)
        except TypeError:
            pass

        if value is not None:
            whenlist = [
                (_literal_as_binds(c).self_group(),
                 _literal_as_binds(r)) for (c, r) in whens
            ]
        else:
            whenlist = [
                (_no_literals(c).self_group(),
                 _literal_as_binds(r)) for (c, r) in whens
            ]

        if whenlist:
            type_ = list(whenlist[-1])[-1].type
        else:
            type_ = None

        if value is None:
            self.value = None
        else:
            self.value = _literal_as_binds(value)

        self.type = type_
        self.whens = whenlist
        if else_ is not None:
            self.else_ = _literal_as_binds(else_)
        else:
            self.else_ = None

    def _copy_internals(self, clone=_clone, **kw):
        if self.value is not None:
            self.value = clone(self.value, **kw)
        self.whens = [(clone(x, **kw), clone(y, **kw))
                      for x, y in self.whens]
        if self.else_ is not None:
            self.else_ = clone(self.else_, **kw)

    def get_children(self, **kwargs):
        if self.value is not None:
            yield self.value
        for x, y in self.whens:
            yield x
            yield y
        if self.else_ is not None:
            yield self.else_

    @property
    def _from_objects(self):
        return list(itertools.chain(*[x._from_objects for x in
                                      self.get_children()]))


def literal_column(text, type_=None):
    """Produce a :class:`.ColumnClause` object that has the
    :paramref:`.column.is_literal` flag set to True.

    :func:`.literal_column` is similar to :func:`.column`, except that
    it is more often used as a "standalone" column expression that renders
    exactly as stated; while :func:`.column` stores a string name that
    will be assumed to be part of a table and may be quoted as such,
    :func:`.literal_column` can be that, or any other arbitrary column-oriented
    expression.

    :param text: the text of the expression; can be any SQL expression.
      Quoting rules will not be applied. To specify a column-name expression
      which should be subject to quoting rules, use the :func:`column`
      function.

    :param type\_: an optional :class:`~sqlalchemy.types.TypeEngine`
      object which will
      provide result-set translation and additional expression semantics for
      this column. If left as None the type will be NullType.

    .. seealso::

        :func:`.column`

        :func:`.text`

        :ref:`sqlexpression_literal_column`

    """
    return ColumnClause(text, type_=type_, is_literal=True)


class Cast(ColumnElement):
    """Represent a ``CAST`` expression.

    :class:`.Cast` is produced using the :func:`.cast` factory function,
    as in::

        from sqlalchemy import cast, Numeric

        stmt = select([
                    cast(product_table.c.unit_price, Numeric(10, 4))
                ])

    Details on :class:`.Cast` usage is at :func:`.cast`.

    .. seealso::

        :func:`.cast`

    """

    __visit_name__ = 'cast'

    def __init__(self, expression, type_):
        """Produce a ``CAST`` expression.

        :func:`.cast` returns an instance of :class:`.Cast`.

        E.g.::

            from sqlalchemy import cast, Numeric

            stmt = select([
                        cast(product_table.c.unit_price, Numeric(10, 4))
                    ])

        The above statement will produce SQL resembling::

            SELECT CAST(unit_price AS NUMERIC(10, 4)) FROM product

        The :func:`.cast` function performs two distinct functions when
        used.  The first is that it renders the ``CAST`` expression within
        the resulting SQL string.  The second is that it associates the given
        type (e.g. :class:`.TypeEngine` class or instance) with the column
        expression on the Python side, which means the expression will take
        on the expression operator behavior associated with that type,
        as well as the bound-value handling and result-row-handling behavior
        of the type.

        .. versionchanged:: 0.9.0 :func:`.cast` now applies the given type
           to the expression such that it takes effect on the bound-value,
           e.g. the Python-to-database direction, in addition to the
           result handling, e.g. database-to-Python, direction.

        An alternative to :func:`.cast` is the :func:`.type_coerce` function.
        This function performs the second task of associating an expression
        with a specific type, but does not render the ``CAST`` expression
        in SQL.

        :param expression: A SQL expression, such as a :class:`.ColumnElement`
         expression or a Python string which will be coerced into a bound
         literal value.

        :param type_: A :class:`.TypeEngine` class or instance indicating
         the type to which the ``CAST`` should apply.

        .. seealso::

            :func:`.type_coerce` - Python-side type coercion without emitting
            CAST.

        """
        self.type = type_api.to_instance(type_)
        self.clause = _literal_as_binds(expression, type_=self.type)
        self.typeclause = TypeClause(self.type)

    def _copy_internals(self, clone=_clone, **kw):
        self.clause = clone(self.clause, **kw)
        self.typeclause = clone(self.typeclause, **kw)

    def get_children(self, **kwargs):
        return self.clause, self.typeclause

    @property
    def _from_objects(self):
        return self.clause._from_objects


class Extract(ColumnElement):
    """Represent a SQL EXTRACT clause, ``extract(field FROM expr)``."""

    __visit_name__ = 'extract'

    def __init__(self, field, expr, **kwargs):
        """Return a :class:`.Extract` construct.

        This is typically available as :func:`.extract`
        as well as ``func.extract`` from the
        :data:`.func` namespace.

        """
        self.type = type_api.INTEGERTYPE
        self.field = field
        self.expr = _literal_as_binds(expr, None)

    def _copy_internals(self, clone=_clone, **kw):
        self.expr = clone(self.expr, **kw)

    def get_children(self, **kwargs):
        return self.expr,

    @property
    def _from_objects(self):
        return self.expr._from_objects


class _label_reference(ColumnElement):
    """Wrap a column expression as it appears in a 'reference' context.

    This expression is any that inclues an _order_by_label_element,
    which is a Label, or a DESC / ASC construct wrapping a Label.

    The production of _label_reference() should occur when an expression
    is added to this context; this includes the ORDER BY or GROUP BY of a
    SELECT statement, as well as a few other places, such as the ORDER BY
    within an OVER clause.

    """
    __visit_name__ = 'label_reference'

    def __init__(self, element):
        self.element = element

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    @property
    def _from_objects(self):
        return ()


class _textual_label_reference(ColumnElement):
    __visit_name__ = 'textual_label_reference'

    def __init__(self, element):
        self.element = element

    @util.memoized_property
    def _text_clause(self):
        return TextClause._create_text(self.element)


class UnaryExpression(ColumnElement):
    """Define a 'unary' expression.

    A unary expression has a single column expression
    and an operator.  The operator can be placed on the left
    (where it is called the 'operator') or right (where it is called the
    'modifier') of the column expression.

    :class:`.UnaryExpression` is the basis for several unary operators
    including those used by :func:`.desc`, :func:`.asc`, :func:`.distinct`,
    :func:`.nullsfirst` and :func:`.nullslast`.

    """
    __visit_name__ = 'unary'

    def __init__(self, element, operator=None, modifier=None,
                 type_=None, negate=None):
        self.operator = operator
        self.modifier = modifier
        self.element = element.self_group(
            against=self.operator or self.modifier)
        self.type = type_api.to_instance(type_)
        self.negate = negate

    @classmethod
    def _create_nullsfirst(cls, column):
        """Produce the ``NULLS FIRST`` modifier for an ``ORDER BY`` expression.

        :func:`.nullsfirst` is intended to modify the expression produced
        by :func:`.asc` or :func:`.desc`, and indicates how NULL values
        should be handled when they are encountered during ordering::


            from sqlalchemy import desc, nullsfirst

            stmt = select([users_table]).\\
                        order_by(nullsfirst(desc(users_table.c.name)))

        The SQL expression from the above would resemble::

            SELECT id, name FROM user ORDER BY name DESC NULLS FIRST

        Like :func:`.asc` and :func:`.desc`, :func:`.nullsfirst` is typically
        invoked from the column expression itself using
        :meth:`.ColumnElement.nullsfirst`, rather than as its standalone
        function version, as in::

            stmt = (select([users_table]).
                    order_by(users_table.c.name.desc().nullsfirst())
                    )

        .. seealso::

            :func:`.asc`

            :func:`.desc`

            :func:`.nullslast`

            :meth:`.Select.order_by`

        """
        return UnaryExpression(
            _literal_as_label_reference(column),
            modifier=operators.nullsfirst_op)

    @classmethod
    def _create_nullslast(cls, column):
        """Produce the ``NULLS LAST`` modifier for an ``ORDER BY`` expression.

        :func:`.nullslast` is intended to modify the expression produced
        by :func:`.asc` or :func:`.desc`, and indicates how NULL values
        should be handled when they are encountered during ordering::


            from sqlalchemy import desc, nullslast

            stmt = select([users_table]).\\
                        order_by(nullslast(desc(users_table.c.name)))

        The SQL expression from the above would resemble::

            SELECT id, name FROM user ORDER BY name DESC NULLS LAST

        Like :func:`.asc` and :func:`.desc`, :func:`.nullslast` is typically
        invoked from the column expression itself using
        :meth:`.ColumnElement.nullslast`, rather than as its standalone
        function version, as in::

            stmt = select([users_table]).\\
                        order_by(users_table.c.name.desc().nullslast())

        .. seealso::

            :func:`.asc`

            :func:`.desc`

            :func:`.nullsfirst`

            :meth:`.Select.order_by`

        """
        return UnaryExpression(
            _literal_as_label_reference(column),
            modifier=operators.nullslast_op)

    @classmethod
    def _create_desc(cls, column):
        """Produce a descending ``ORDER BY`` clause element.

        e.g.::

            from sqlalchemy import desc

            stmt = select([users_table]).order_by(desc(users_table.c.name))

        will produce SQL as::

            SELECT id, name FROM user ORDER BY name DESC

        The :func:`.desc` function is a standalone version of the
        :meth:`.ColumnElement.desc` method available on all SQL expressions,
        e.g.::


            stmt = select([users_table]).order_by(users_table.c.name.desc())

        :param column: A :class:`.ColumnElement` (e.g. scalar SQL expression)
         with which to apply the :func:`.desc` operation.

        .. seealso::

            :func:`.asc`

            :func:`.nullsfirst`

            :func:`.nullslast`

            :meth:`.Select.order_by`

        """
        return UnaryExpression(
            _literal_as_label_reference(column), modifier=operators.desc_op)

    @classmethod
    def _create_asc(cls, column):
        """Produce an ascending ``ORDER BY`` clause element.

        e.g.::

            from sqlalchemy import asc
            stmt = select([users_table]).order_by(asc(users_table.c.name))

        will produce SQL as::

            SELECT id, name FROM user ORDER BY name ASC

        The :func:`.asc` function is a standalone version of the
        :meth:`.ColumnElement.asc` method available on all SQL expressions,
        e.g.::


            stmt = select([users_table]).order_by(users_table.c.name.asc())

        :param column: A :class:`.ColumnElement` (e.g. scalar SQL expression)
         with which to apply the :func:`.asc` operation.

        .. seealso::

            :func:`.desc`

            :func:`.nullsfirst`

            :func:`.nullslast`

            :meth:`.Select.order_by`

        """
        return UnaryExpression(
            _literal_as_label_reference(column), modifier=operators.asc_op)

    @classmethod
    def _create_distinct(cls, expr):
        """Produce an column-expression-level unary ``DISTINCT`` clause.

        This applies the ``DISTINCT`` keyword to an individual column
        expression, and is typically contained within an aggregate function,
        as in::

            from sqlalchemy import distinct, func
            stmt = select([func.count(distinct(users_table.c.name))])

        The above would produce an expression resembling::

            SELECT COUNT(DISTINCT name) FROM user

        The :func:`.distinct` function is also available as a column-level
        method, e.g. :meth:`.ColumnElement.distinct`, as in::

            stmt = select([func.count(users_table.c.name.distinct())])

        The :func:`.distinct` operator is different from the
        :meth:`.Select.distinct` method of :class:`.Select`,
        which produces a ``SELECT`` statement
        with ``DISTINCT`` applied to the result set as a whole,
        e.g. a ``SELECT DISTINCT`` expression.  See that method for further
        information.

        .. seealso::

            :meth:`.ColumnElement.distinct`

            :meth:`.Select.distinct`

            :data:`.func`

        """
        expr = _literal_as_binds(expr)
        return UnaryExpression(
            expr, operator=operators.distinct_op, type_=expr.type)

    @property
    def _order_by_label_element(self):
        if self.modifier in (operators.desc_op, operators.asc_op):
            return self.element._order_by_label_element
        else:
            return None

    @property
    def _from_objects(self):
        return self.element._from_objects

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    def get_children(self, **kwargs):
        return self.element,

    def compare(self, other, **kw):
        """Compare this :class:`UnaryExpression` against the given
        :class:`.ClauseElement`."""

        return (
            isinstance(other, UnaryExpression) and
            self.operator == other.operator and
            self.modifier == other.modifier and
            self.element.compare(other.element, **kw)
        )

    def _negate(self):
        if self.negate is not None:
            return UnaryExpression(
                self.element,
                operator=self.negate,
                negate=self.operator,
                modifier=self.modifier,
                type_=self.type)
        else:
            return ClauseElement._negate(self)

    def self_group(self, against=None):
        if self.operator and operators.is_precedent(self.operator, against):
            return Grouping(self)
        else:
            return self


class AsBoolean(UnaryExpression):

    def __init__(self, element, operator, negate):
        self.element = element
        self.type = type_api.BOOLEANTYPE
        self.operator = operator
        self.negate = negate
        self.modifier = None

    def self_group(self, against=None):
        return self

    def _negate(self):
        return self.element._negate()


class BinaryExpression(ColumnElement):
    """Represent an expression that is ``LEFT <operator> RIGHT``.

    A :class:`.BinaryExpression` is generated automatically
    whenever two column expressions are used in a Python binary expression::

        >>> from sqlalchemy.sql import column
        >>> column('a') + column('b')
        <sqlalchemy.sql.expression.BinaryExpression object at 0x101029dd0>
        >>> print column('a') + column('b')
        a + b

    """

    __visit_name__ = 'binary'

    def __init__(self, left, right, operator, type_=None,
                 negate=None, modifiers=None):
        # allow compatibility with libraries that
        # refer to BinaryExpression directly and pass strings
        if isinstance(operator, util.string_types):
            operator = operators.custom_op(operator)
        self._orig = (left, right)
        self.left = left.self_group(against=operator)
        self.right = right.self_group(against=operator)
        self.operator = operator
        self.type = type_api.to_instance(type_)
        self.negate = negate

        if modifiers is None:
            self.modifiers = {}
        else:
            self.modifiers = modifiers

    def __bool__(self):
        if self.operator in (operator.eq, operator.ne):
            return self.operator(hash(self._orig[0]), hash(self._orig[1]))
        else:
            raise TypeError("Boolean value of this clause is not defined")

    __nonzero__ = __bool__

    @property
    def is_comparison(self):
        return operators.is_comparison(self.operator)

    @property
    def _from_objects(self):
        return self.left._from_objects + self.right._from_objects

    def _copy_internals(self, clone=_clone, **kw):
        self.left = clone(self.left, **kw)
        self.right = clone(self.right, **kw)

    def get_children(self, **kwargs):
        return self.left, self.right

    def compare(self, other, **kw):
        """Compare this :class:`BinaryExpression` against the
        given :class:`BinaryExpression`."""

        return (
            isinstance(other, BinaryExpression) and
            self.operator == other.operator and
            (
                self.left.compare(other.left, **kw) and
                self.right.compare(other.right, **kw) or
                (
                    operators.is_commutative(self.operator) and
                    self.left.compare(other.right, **kw) and
                    self.right.compare(other.left, **kw)
                )
            )
        )

    def self_group(self, against=None):
        if operators.is_precedent(self.operator, against):
            return Grouping(self)
        else:
            return self

    def _negate(self):
        if self.negate is not None:
            return BinaryExpression(
                self.left,
                self.right,
                self.negate,
                negate=self.operator,
                type_=self.type,
                modifiers=self.modifiers)
        else:
            return super(BinaryExpression, self)._negate()


class Grouping(ColumnElement):
    """Represent a grouping within a column expression"""

    __visit_name__ = 'grouping'

    def __init__(self, element):
        self.element = element
        self.type = getattr(element, 'type', type_api.NULLTYPE)

    def self_group(self, against=None):
        return self

    @property
    def _key_label(self):
        return self._label

    @property
    def _label(self):
        return getattr(self.element, '_label', None) or self.anon_label

    def _copy_internals(self, clone=_clone, **kw):
        self.element = clone(self.element, **kw)

    def get_children(self, **kwargs):
        return self.element,

    @property
    def _from_objects(self):
        return self.element._from_objects

    def __getattr__(self, attr):
        return getattr(self.element, attr)

    def __getstate__(self):
        return {'element': self.element, 'type': self.type}

    def __setstate__(self, state):
        self.element = state['element']
        self.type = state['type']

    def compare(self, other, **kw):
        return isinstance(other, Grouping) and \
            self.element.compare(other.element)


class Over(ColumnElement):
    """Represent an OVER clause.

    This is a special operator against a so-called
    "window" function, as well as any aggregate function,
    which produces results relative to the result set
    itself.  It's supported only by certain database
    backends.

    """
    __visit_name__ = 'over'

    order_by = None
    partition_by = None

    def __init__(self, func, partition_by=None, order_by=None):
        """Produce an :class:`.Over` object against a function.

        Used against aggregate or so-called "window" functions,
        for database backends that support window functions.

        E.g.::

            from sqlalchemy import over
            over(func.row_number(), order_by='x')

        Would produce "ROW_NUMBER() OVER(ORDER BY x)".

        :param func: a :class:`.FunctionElement` construct, typically
         generated by :data:`~.expression.func`.
        :param partition_by: a column element or string, or a list
         of such, that will be used as the PARTITION BY clause
         of the OVER construct.
        :param order_by: a column element or string, or a list
         of such, that will be used as the ORDER BY clause
         of the OVER construct.

        This function is also available from the :data:`~.expression.func`
        construct itself via the :meth:`.FunctionElement.over` method.

        .. versionadded:: 0.7

        """
        self.func = func
        if order_by is not None:
            self.order_by = ClauseList(
                *util.to_list(order_by),
                _literal_as_text=_literal_as_label_reference)
        if partition_by is not None:
            self.partition_by = ClauseList(
                *util.to_list(partition_by),
                _literal_as_text=_literal_as_label_reference)

    @util.memoized_property
    def type(self):
        return self.func.type

    def get_children(self, **kwargs):
        return [c for c in
                (self.func, self.partition_by, self.order_by)
                if c is not None]

    def _copy_internals(self, clone=_clone, **kw):
        self.func = clone(self.func, **kw)
        if self.partition_by is not None:
            self.partition_by = clone(self.partition_by, **kw)
        if self.order_by is not None:
            self.order_by = clone(self.order_by, **kw)

    @property
    def _from_objects(self):
        return list(itertools.chain(
            *[c._from_objects for c in
                (self.func, self.partition_by, self.order_by)
              if c is not None]
        ))


class FunctionFilter(ColumnElement):
    """Represent a function FILTER clause.

    This is a special operator against aggregate and window functions,
    which controls which rows are passed to it.
    It's supported only by certain database backends.

    Invocation of :class:`.FunctionFilter` is via
    :meth:`.FunctionElement.filter`::

        func.count(1).filter(True)

    .. versionadded:: 1.0.0

    .. seealso::

        :meth:`.FunctionElement.filter`

    """
    __visit_name__ = 'funcfilter'

    criterion = None

    def __init__(self, func, *criterion):
        """Produce a :class:`.FunctionFilter` object against a function.

        Used against aggregate and window functions,
        for database backends that support the "FILTER" clause.

        E.g.::

            from sqlalchemy import funcfilter
            funcfilter(func.count(1), MyClass.name == 'some name')

        Would produce "COUNT(1) FILTER (WHERE myclass.name = 'some name')".

        This function is also available from the :data:`~.expression.func`
        construct itself via the :meth:`.FunctionElement.filter` method.

        .. versionadded:: 1.0.0

        .. seealso::

            :meth:`.FunctionElement.filter`


        """
        self.func = func
        self.filter(*criterion)

    def filter(self, *criterion):
        """Produce an additional FILTER against the function.

        This method adds additional criteria to the initial criteria
        set up by :meth:`.FunctionElement.filter`.

        Multiple criteria are joined together at SQL render time
        via ``AND``.


        """

        for criterion in list(criterion):
            criterion = _expression_literal_as_text(criterion)

            if self.criterion is not None:
                self.criterion = self.criterion & criterion
            else:
                self.criterion = criterion

        return self

    def over(self, partition_by=None, order_by=None):
        """Produce an OVER clause against this filtered function.

        Used against aggregate or so-called "window" functions,
        for database backends that support window functions.

        The expression::

            func.rank().filter(MyClass.y > 5).over(order_by='x')

        is shorthand for::

            from sqlalchemy import over, funcfilter
            over(funcfilter(func.rank(), MyClass.y > 5), order_by='x')

        See :func:`~.expression.over` for a full description.

        """
        return Over(self, partition_by=partition_by, order_by=order_by)

    @util.memoized_property
    def type(self):
        return self.func.type

    def get_children(self, **kwargs):
        return [c for c in
                (self.func, self.criterion)
                if c is not None]

    def _copy_internals(self, clone=_clone, **kw):
        self.func = clone(self.func, **kw)
        if self.criterion is not None:
            self.criterion = clone(self.criterion, **kw)

    @property
    def _from_objects(self):
        return list(itertools.chain(
            *[c._from_objects for c in (self.func, self.criterion)
              if c is not None]
        ))


class Label(ColumnElement):
    """Represents a column label (AS).

    Represent a label, as typically applied to any column-level
    element using the ``AS`` sql keyword.

    """

    __visit_name__ = 'label'

    def __init__(self, name, element, type_=None):
        """Return a :class:`Label` object for the
        given :class:`.ColumnElement`.

        A label changes the name of an element in the columns clause of a
        ``SELECT`` statement, typically via the ``AS`` SQL keyword.

        This functionality is more conveniently available via the
        :meth:`.ColumnElement.label` method on :class:`.ColumnElement`.

        :param name: label name

        :param obj: a :class:`.ColumnElement`.

        """

        if isinstance(element, Label):
            self._resolve_label = element._label

        while isinstance(element, Label):
            element = element.element

        if name:
            self.name = name
            self._resolve_label = self.name
        else:
            self.name = _anonymous_label(
                '%%(%d %s)s' % (id(self), getattr(element, 'name', 'anon'))
            )

        self.key = self._label = self._key_label = self.name
        self._element = element
        self._type = type_
        self._proxies = [element]

    def __reduce__(self):
        return self.__class__, (self.name, self._element, self._type)

    @util.memoized_property
    def _allow_label_resolve(self):
        return self.element._allow_label_resolve

    @property
    def _order_by_label_element(self):
        return self

    @util.memoized_property
    def type(self):
        return type_api.to_instance(
            self._type or getattr(self._element, 'type', None)
        )

    @util.memoized_property
    def element(self):
        return self._element.self_group(against=operators.as_)

    def self_group(self, against=None):
        sub_element = self._element.self_group(against=against)
        if sub_element is not self._element:
            return Label(self.name,
                         sub_element,
                         type_=self._type)
        else:
            return self

    @property
    def primary_key(self):
        return self.element.primary_key

    @property
    def foreign_keys(self):
        return self.element.foreign_keys

    def get_children(self, **kwargs):
        return self.element,

    def _copy_internals(self, clone=_clone, anonymize_labels=False, **kw):
        self.element = clone(self.element, **kw)
        self.__dict__.pop('_allow_label_resolve', None)
        if anonymize_labels:
            self.name = self._resolve_label = _anonymous_label(
                '%%(%d %s)s' % (
                    id(self), getattr(self.element, 'name', 'anon'))
            )
            self.key = self._label = self._key_label = self.name

    @property
    def _from_objects(self):
        return self.element._from_objects

    def _make_proxy(self, selectable, name=None, **kw):
        e = self.element._make_proxy(selectable,
                                     name=name if name else self.name)
        e._proxies.append(self)
        if self._type is not None:
            e.type = self._type
        return e


class ColumnClause(Immutable, ColumnElement):
    """Represents a column expression from any textual string.

    The :class:`.ColumnClause`, a lightweight analogue to the
    :class:`.Column` class, is typically invoked using the
    :func:`.column` function, as in::

        from sqlalchemy import column

        id, name = column("id"), column("name")
        stmt = select([id, name]).select_from("user")

    The above statement would produce SQL like::

        SELECT id, name FROM user

    :class:`.ColumnClause` is the immediate superclass of the schema-specific
    :class:`.Column` object.  While the :class:`.Column` class has all the
    same capabilities as :class:`.ColumnClause`, the :class:`.ColumnClause`
    class is usable by itself in those cases where behavioral requirements
    are limited to simple SQL expression generation.  The object has none of
    the associations with schema-level metadata or with execution-time
    behavior that :class:`.Column` does, so in that sense is a "lightweight"
    version of :class:`.Column`.

    Full details on :class:`.ColumnClause` usage is at :func:`.column`.

    .. seealso::

        :func:`.column`

        :class:`.Column`

    """
    __visit_name__ = 'column'

    onupdate = default = server_default = server_onupdate = None

    _memoized_property = util.group_expirable_memoized_property()

    def __init__(self, text, type_=None, is_literal=False, _selectable=None):
        """Produce a :class:`.ColumnClause` object.

        The :class:`.ColumnClause` is a lightweight analogue to the
        :class:`.Column` class.  The :func:`.column` function can
        be invoked with just a name alone, as in::

            from sqlalchemy import column

            id, name = column("id"), column("name")
            stmt = select([id, name]).select_from("user")

        The above statement would produce SQL like::

            SELECT id, name FROM user

        Once constructed, :func:`.column` may be used like any other SQL
        expression element such as within :func:`.select` constructs::

            from sqlalchemy.sql import column

            id, name = column("id"), column("name")
            stmt = select([id, name]).select_from("user")

        The text handled by :func:`.column` is assumed to be handled
        like the name of a database column; if the string contains mixed case,
        special characters, or matches a known reserved word on the target
        backend, the column expression will render using the quoting
        behavior determined by the backend.  To produce a textual SQL
        expression that is rendered exactly without any quoting,
        use :func:`.literal_column` instead, or pass ``True`` as the
        value of :paramref:`.column.is_literal`.   Additionally, full SQL
        statements are best handled using the :func:`.text` construct.

        :func:`.column` can be used in a table-like
        fashion by combining it with the :func:`.table` function
        (which is the lightweight analogue to :class:`.Table`) to produce
        a working table construct with minimal boilerplate::

            from sqlalchemy import table, column, select

            user = table("user",
                    column("id"),
                    column("name"),
                    column("description"),
            )

            stmt = select([user.c.description]).where(user.c.name == 'wendy')

        A :func:`.column` / :func:`.table` construct like that illustrated
        above can be created in an
        ad-hoc fashion and is not associated with any
        :class:`.schema.MetaData`, DDL, or events, unlike its
        :class:`.Table` counterpart.

        .. versionchanged:: 1.0.0 :func:`.expression.column` can now
           be imported from the plain ``sqlalchemy`` namespace like any
           other SQL element.

        :param text: the text of the element.

        :param type: :class:`.types.TypeEngine` object which can associate
          this :class:`.ColumnClause` with a type.

        :param is_literal: if True, the :class:`.ColumnClause` is assumed to
          be an exact expression that will be delivered to the output with no
          quoting rules applied regardless of case sensitive settings. the
          :func:`.literal_column()` function essentially invokes
          :func:`.column` while passing ``is_literal=True``.

        .. seealso::

            :class:`.Column`

            :func:`.literal_column`

            :func:`.table`

            :func:`.text`

            :ref:`sqlexpression_literal_column`

        """

        self.key = self.name = text
        self.table = _selectable
        self.type = type_api.to_instance(type_)
        self.is_literal = is_literal

    def _compare_name_for_result(self, other):
        if self.is_literal or \
                self.table is None or self.table._textual or \
                not hasattr(other, 'proxy_set') or (
                    isinstance(other, ColumnClause) and
                    (other.is_literal or
                     other.table is None or
                     other.table._textual)
                ):
            return (hasattr(other, 'name') and self.name == other.name) or \
                (hasattr(other, '_label') and self._label == other._label)
        else:
            return other.proxy_set.intersection(self.proxy_set)

    def _get_table(self):
        return self.__dict__['table']

    def _set_table(self, table):
        self._memoized_property.expire_instance(self)
        self.__dict__['table'] = table
    table = property(_get_table, _set_table)

    @_memoized_property
    def _from_objects(self):
        t = self.table
        if t is not None:
            return [t]
        else:
            return []

    @util.memoized_property
    def description(self):
        if util.py3k:
            return self.name
        else:
            return self.name.encode('ascii', 'backslashreplace')

    @_memoized_property
    def _key_label(self):
        if self.key != self.name:
            return self._gen_label(self.key)
        else:
            return self._label

    @_memoized_property
    def _label(self):
        return self._gen_label(self.name)

    @_memoized_property
    def _render_label_in_columns_clause(self):
        return self.table is not None

    def _gen_label(self, name):
        t = self.table

        if self.is_literal:
            return None

        elif t is not None and t.named_with_column:
            if getattr(t, 'schema', None):
                label = t.schema.replace('.', '_') + "_" + \
                    t.name + "_" + name
            else:
                label = t.name + "_" + name

            # propagate name quoting rules for labels.
            if getattr(name, "quote", None) is not None:
                if isinstance(label, quoted_name):
                    label.quote = name.quote
                else:
                    label = quoted_name(label, name.quote)
            elif getattr(t.name, "quote", None) is not None:
                # can't get this situation to occur, so let's
                # assert false on it for now
                assert not isinstance(label, quoted_name)
                label = quoted_name(label, t.name.quote)

            # ensure the label name doesn't conflict with that
            # of an existing column
            if label in t.c:
                _label = label
                counter = 1
                while _label in t.c:
                    _label = label + "_" + str(counter)
                    counter += 1
                label = _label

            return _as_truncated(label)

        else:
            return name

    def _bind_param(self, operator, obj):
        return BindParameter(self.key, obj,
                             _compared_to_operator=operator,
                             _compared_to_type=self.type,
                             unique=True)

    def _make_proxy(self, selectable, name=None, attach=True,
                    name_is_truncatable=False, **kw):
        # propagate the "is_literal" flag only if we are keeping our name,
        # otherwise its considered to be a label
        is_literal = self.is_literal and (name is None or name == self.name)
        c = self._constructor(
            _as_truncated(name or self.name) if
            name_is_truncatable else
            (name or self.name),
            type_=self.type,
            _selectable=selectable,
            is_literal=is_literal
        )
        if name is None:
            c.key = self.key
        c._proxies = [self]
        if selectable._is_clone_of is not None:
            c._is_clone_of = \
                selectable._is_clone_of.columns.get(c.key)

        if attach:
            selectable._columns[c.key] = c
        return c


class _IdentifiedClause(Executable, ClauseElement):

    __visit_name__ = 'identified'
    _execution_options = \
        Executable._execution_options.union({'autocommit': False})

    def __init__(self, ident):
        self.ident = ident


class SavepointClause(_IdentifiedClause):
    __visit_name__ = 'savepoint'


class RollbackToSavepointClause(_IdentifiedClause):
    __visit_name__ = 'rollback_to_savepoint'


class ReleaseSavepointClause(_IdentifiedClause):
    __visit_name__ = 'release_savepoint'


class quoted_name(util.MemoizedSlots, util.text_type):
    """Represent a SQL identifier combined with quoting preferences.

    :class:`.quoted_name` is a Python unicode/str subclass which
    represents a particular identifier name along with a
    ``quote`` flag.  This ``quote`` flag, when set to
    ``True`` or ``False``, overrides automatic quoting behavior
    for this identifier in order to either unconditionally quote
    or to not quote the name.  If left at its default of ``None``,
    quoting behavior is applied to the identifier on a per-backend basis
    based on an examination of the token itself.

    A :class:`.quoted_name` object with ``quote=True`` is also
    prevented from being modified in the case of a so-called
    "name normalize" option.  Certain database backends, such as
    Oracle, Firebird, and DB2 "normalize" case-insensitive names
    as uppercase.  The SQLAlchemy dialects for these backends
    convert from SQLAlchemy's lower-case-means-insensitive convention
    to the upper-case-means-insensitive conventions of those backends.
    The ``quote=True`` flag here will prevent this conversion from occurring
    to support an identifier that's quoted as all lower case against
    such a backend.

    The :class:`.quoted_name` object is normally created automatically
    when specifying the name for key schema constructs such as
    :class:`.Table`, :class:`.Column`, and others.  The class can also be
    passed explicitly as the name to any function that receives a name which
    can be quoted.  Such as to use the :meth:`.Engine.has_table` method with
    an unconditionally quoted name::

        from sqlaclchemy import create_engine
        from sqlalchemy.sql.elements import quoted_name

        engine = create_engine("oracle+cx_oracle://some_dsn")
        engine.has_table(quoted_name("some_table", True))

    The above logic will run the "has table" logic against the Oracle backend,
    passing the name exactly as ``"some_table"`` without converting to
    upper case.

    .. versionadded:: 0.9.0

    """

    __slots__ = 'quote', 'lower', 'upper'

    def __new__(cls, value, quote):
        if value is None:
            return None
        # experimental - don't bother with quoted_name
        # if quote flag is None.  doesn't seem to make any dent
        # in performance however
        # elif not sprcls and quote is None:
        #   return value
        elif isinstance(value, cls) and (
            quote is None or value.quote == quote
        ):
            return value
        self = super(quoted_name, cls).__new__(cls, value)
        self.quote = quote
        return self

    def __reduce__(self):
        return quoted_name, (util.text_type(self), self.quote)

    def _memoized_method_lower(self):
        if self.quote:
            return self
        else:
            return util.text_type(self).lower()

    def _memoized_method_upper(self):
        if self.quote:
            return self
        else:
            return util.text_type(self).upper()

    def __repr__(self):
        backslashed = self.encode('ascii', 'backslashreplace')
        if not util.py2k:
            backslashed = backslashed.decode('ascii')
        return "'%s'" % backslashed


class _truncated_label(quoted_name):
    """A unicode subclass used to identify symbolic "
    "names that may require truncation."""

    __slots__ = ()

    def __new__(cls, value, quote=None):
        quote = getattr(value, "quote", quote)
        # return super(_truncated_label, cls).__new__(cls, value, quote, True)
        return super(_truncated_label, cls).__new__(cls, value, quote)

    def __reduce__(self):
        return self.__class__, (util.text_type(self), self.quote)

    def apply_map(self, map_):
        return self


class conv(_truncated_label):
    """Mark a string indicating that a name has already been converted
    by a naming convention.

    This is a string subclass that indicates a name that should not be
    subject to any further naming conventions.

    E.g. when we create a :class:`.Constraint` using a naming convention
    as follows::

        m = MetaData(naming_convention={
            "ck": "ck_%(table_name)s_%(constraint_name)s"
        })
        t = Table('t', m, Column('x', Integer),
                        CheckConstraint('x > 5', name='x5'))

    The name of the above constraint will be rendered as ``"ck_t_x5"``.
    That is, the existing name ``x5`` is used in the naming convention as the
    ``constraint_name`` token.

    In some situations, such as in migration scripts, we may be rendering
    the above :class:`.CheckConstraint` with a name that's already been
    converted.  In order to make sure the name isn't double-modified, the
    new name is applied using the :func:`.schema.conv` marker.  We can
    use this explicitly as follows::


        m = MetaData(naming_convention={
            "ck": "ck_%(table_name)s_%(constraint_name)s"
        })
        t = Table('t', m, Column('x', Integer),
                        CheckConstraint('x > 5', name=conv('ck_t_x5')))

    Where above, the :func:`.schema.conv` marker indicates that the constraint
    name here is final, and the name will render as ``"ck_t_x5"`` and not
    ``"ck_t_ck_t_x5"``

    .. versionadded:: 0.9.4

    .. seealso::

        :ref:`constraint_naming_conventions`

    """
    __slots__ = ()


class _defer_name(_truncated_label):
    """mark a name as 'deferred' for the purposes of automated name
    generation.

    """
    __slots__ = ()

    def __new__(cls, value):
        if value is None:
            return _NONE_NAME
        elif isinstance(value, conv):
            return value
        else:
            return super(_defer_name, cls).__new__(cls, value)

    def __reduce__(self):
        return self.__class__, (util.text_type(self), )


class _defer_none_name(_defer_name):
    """indicate a 'deferred' name that was ultimately the value None."""
    __slots__ = ()

_NONE_NAME = _defer_none_name("_unnamed_")

# for backwards compatibility in case
# someone is re-implementing the
# _truncated_identifier() sequence in a custom
# compiler
_generated_label = _truncated_label


class _anonymous_label(_truncated_label):
    """A unicode subclass used to identify anonymously
    generated names."""

    __slots__ = ()

    def __add__(self, other):
        return _anonymous_label(
            quoted_name(
                util.text_type.__add__(self, util.text_type(other)),
                self.quote)
        )

    def __radd__(self, other):
        return _anonymous_label(
            quoted_name(
                util.text_type.__add__(util.text_type(other), self),
                self.quote)
        )

    def apply_map(self, map_):
        if self.quote is not None:
            # preserve quoting only if necessary
            return quoted_name(self % map_, self.quote)
        else:
            # else skip the constructor call
            return self % map_


def _as_truncated(value):
    """coerce the given value to :class:`._truncated_label`.

    Existing :class:`._truncated_label` and
    :class:`._anonymous_label` objects are passed
    unchanged.
    """

    if isinstance(value, _truncated_label):
        return value
    else:
        return _truncated_label(value)


def _string_or_unprintable(element):
    if isinstance(element, util.string_types):
        return element
    else:
        try:
            return str(element)
        except Exception:
            return "unprintable element %r" % element


def _expand_cloned(elements):
    """expand the given set of ClauseElements to be the set of all 'cloned'
    predecessors.

    """
    return itertools.chain(*[x._cloned_set for x in elements])


def _select_iterables(elements):
    """expand tables into individual columns in the
    given list of column expressions.

    """
    return itertools.chain(*[c._select_iterable for c in elements])


def _cloned_intersection(a, b):
    """return the intersection of sets a and b, counting
    any overlap between 'cloned' predecessors.

    The returned set is in terms of the entities present within 'a'.

    """
    all_overlap = set(_expand_cloned(a)).intersection(_expand_cloned(b))
    return set(elem for elem in a
               if all_overlap.intersection(elem._cloned_set))


def _cloned_difference(a, b):
    all_overlap = set(_expand_cloned(a)).intersection(_expand_cloned(b))
    return set(elem for elem in a
               if not all_overlap.intersection(elem._cloned_set))


def _labeled(element):
    if not hasattr(element, 'name'):
        return element.label(None)
    else:
        return element


def _is_column(col):
    """True if ``col`` is an instance of :class:`.ColumnElement`."""

    return isinstance(col, ColumnElement)


def _find_columns(clause):
    """locate Column objects within the given expression."""

    cols = util.column_set()
    traverse(clause, {}, {'column': cols.add})
    return cols


# there is some inconsistency here between the usage of
# inspect() vs. checking for Visitable and __clause_element__.
# Ideally all functions here would derive from inspect(),
# however the inspect() versions add significant callcount
# overhead for critical functions like _interpret_as_column_or_from().
# Generally, the column-based functions are more performance critical
# and are fine just checking for __clause_element__().  It is only
# _interpret_as_from() where we'd like to be able to receive ORM entities
# that have no defined namespace, hence inspect() is needed there.


def _column_as_key(element):
    if isinstance(element, util.string_types):
        return element
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    try:
        return element.key
    except AttributeError:
        return None


def _clause_element_as_expr(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    else:
        return element


def _literal_as_label_reference(element):
    if isinstance(element, util.string_types):
        return _textual_label_reference(element)

    elif hasattr(element, '__clause_element__'):
        element = element.__clause_element__()

    if isinstance(element, ColumnElement) and \
            element._order_by_label_element is not None:
        return _label_reference(element)
    else:
        return _literal_as_text(element)


def _expression_literal_as_text(element):
    return _literal_as_text(element, warn=True)


def _literal_as_text(element, warn=False):
    if isinstance(element, Visitable):
        return element
    elif hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif isinstance(element, util.string_types):
        if warn:
            util.warn_limited(
                "Textual SQL expression %(expr)r should be "
                "explicitly declared as text(%(expr)r)",
                {"expr": util.ellipses_string(element)})

        return TextClause(util.text_type(element))
    elif isinstance(element, (util.NoneType, bool)):
        return _const_expr(element)
    else:
        raise exc.ArgumentError(
            "SQL expression object or string expected, got object of type %r "
            "instead" % type(element)
        )


def _no_literals(element):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        raise exc.ArgumentError("Ambiguous literal: %r.  Use the 'text()' "
                                "function to indicate a SQL expression "
                                "literal, or 'literal()' to indicate a "
                                "bound value." % element)
    else:
        return element


def _is_literal(element):
    return not isinstance(element, Visitable) and \
        not hasattr(element, '__clause_element__')


def _only_column_elements_or_none(element, name):
    if element is None:
        return None
    else:
        return _only_column_elements(element, name)


def _only_column_elements(element, name):
    if hasattr(element, '__clause_element__'):
        element = element.__clause_element__()
    if not isinstance(element, ColumnElement):
        raise exc.ArgumentError(
            "Column-based expression object expected for argument "
            "'%s'; got: '%s', type %s" % (name, element, type(element)))
    return element


def _literal_as_binds(element, name=None, type_=None):
    if hasattr(element, '__clause_element__'):
        return element.__clause_element__()
    elif not isinstance(element, Visitable):
        if element is None:
            return Null()
        else:
            return BindParameter(name, element, type_=type_, unique=True)
    else:
        return element

_guess_straight_column = re.compile(r'^\w\S*$', re.I)


def _interpret_as_column_or_from(element):
    if isinstance(element, Visitable):
        return element
    elif hasattr(element, '__clause_element__'):
        return element.__clause_element__()

    insp = inspection.inspect(element, raiseerr=False)
    if insp is None:
        if isinstance(element, (util.NoneType, bool)):
            return _const_expr(element)
    elif hasattr(insp, "selectable"):
        return insp.selectable

    # be forgiving as this is an extremely common
    # and known expression
    if element == "*":
        guess_is_literal = True
    elif isinstance(element, (numbers.Number)):
        return ColumnClause(str(element), is_literal=True)
    else:
        element = str(element)
        # give into temptation, as this fact we are guessing about
        # is not one we've previously ever needed our users tell us;
        # but let them know we are not happy about it
        guess_is_literal = not _guess_straight_column.match(element)
        util.warn_limited(
            "Textual column expression %(column)r should be "
            "explicitly declared with text(%(column)r), "
            "or use %(literal_column)s(%(column)r) "
            "for more specificity",
            {
                "column": util.ellipses_string(element),
                "literal_column": "literal_column"
                if guess_is_literal else "column"
            })
    return ColumnClause(
        element,
        is_literal=guess_is_literal)


def _const_expr(element):
    if isinstance(element, (Null, False_, True_)):
        return element
    elif element is None:
        return Null()
    elif element is False:
        return False_()
    elif element is True:
        return True_()
    else:
        raise exc.ArgumentError(
            "Expected None, False, or True"
        )


def _type_from_args(args):
    for a in args:
        if not a.type._isnull:
            return a.type
    else:
        return type_api.NULLTYPE


def _corresponding_column_or_error(fromclause, column,
                                   require_embedded=False):
    c = fromclause.corresponding_column(column,
                                        require_embedded=require_embedded)
    if c is None:
        raise exc.InvalidRequestError(
            "Given column '%s', attached to table '%s', "
            "failed to locate a corresponding column from table '%s'"
            %
            (column,
             getattr(column, 'table', None),
             fromclause.description)
        )
    return c


class AnnotatedColumnElement(Annotated):
    def __init__(self, element, values):
        Annotated.__init__(self, element, values)
        ColumnElement.comparator._reset(self)
        for attr in ('name', 'key', 'table'):
            if self.__dict__.get(attr, False) is None:
                self.__dict__.pop(attr)

    def _with_annotations(self, values):
        clone = super(AnnotatedColumnElement, self)._with_annotations(values)
        ColumnElement.comparator._reset(clone)
        return clone

    @util.memoized_property
    def name(self):
        """pull 'name' from parent, if not present"""
        return self._Annotated__element.name

    @util.memoized_property
    def table(self):
        """pull 'table' from parent, if not present"""
        return self._Annotated__element.table

    @util.memoized_property
    def key(self):
        """pull 'key' from parent, if not present"""
        return self._Annotated__element.key

    @util.memoized_property
    def info(self):
        return self._Annotated__element.info

    @util.memoized_property
    def anon_label(self):
        return self._Annotated__element.anon_label
