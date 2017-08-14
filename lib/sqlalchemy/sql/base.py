# sql/base.py
# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Foundational utilities common to many sql modules.

"""


from .. import util, exc
import itertools
from .visitors import ClauseVisitor
import re
import collections

PARSE_AUTOCOMMIT = util.symbol('PARSE_AUTOCOMMIT')
NO_ARG = util.symbol('NO_ARG')


class Immutable(object):
    """mark a ClauseElement as 'immutable' when expressions are cloned."""

    def unique_params(self, *optionaldict, **kwargs):
        raise NotImplementedError("Immutable objects do not support copying")

    def params(self, *optionaldict, **kwargs):
        raise NotImplementedError("Immutable objects do not support copying")

    def _clone(self):
        return self


def _from_objects(*elements):
    return itertools.chain(*[element._from_objects for element in elements])


@util.decorator
def _generative(fn, *args, **kw):
    """Mark a method as generative."""

    self = args[0]._generate()
    fn(self, *args[1:], **kw)
    return self


class _DialectArgView(collections.MutableMapping):
    """A dictionary view of dialect-level arguments in the form
    <dialectname>_<argument_name>.

    """

    def __init__(self, obj):
        self.obj = obj

    def _key(self, key):
        try:
            dialect, value_key = key.split("_", 1)
        except ValueError:
            raise KeyError(key)
        else:
            return dialect, value_key

    def __getitem__(self, key):
        dialect, value_key = self._key(key)

        try:
            opt = self.obj.dialect_options[dialect]
        except exc.NoSuchModuleError:
            raise KeyError(key)
        else:
            return opt[value_key]

    def __setitem__(self, key, value):
        try:
            dialect, value_key = self._key(key)
        except KeyError:
            raise exc.ArgumentError(
                "Keys must be of the form <dialectname>_<argname>")
        else:
            self.obj.dialect_options[dialect][value_key] = value

    def __delitem__(self, key):
        dialect, value_key = self._key(key)
        del self.obj.dialect_options[dialect][value_key]

    def __len__(self):
        return sum(len(args._non_defaults) for args in
                   self.obj.dialect_options.values())

    def __iter__(self):
        return (
            util.safe_kwarg("%s_%s" % (dialect_name, value_name))
            for dialect_name in self.obj.dialect_options
            for value_name in
            self.obj.dialect_options[dialect_name]._non_defaults
        )


class _DialectArgDict(collections.MutableMapping):
    """A dictionary view of dialect-level arguments for a specific
    dialect.

    Maintains a separate collection of user-specified arguments
    and dialect-specified default arguments.

    """

    def __init__(self):
        self._non_defaults = {}
        self._defaults = {}

    def __len__(self):
        return len(set(self._non_defaults).union(self._defaults))

    def __iter__(self):
        return iter(set(self._non_defaults).union(self._defaults))

    def __getitem__(self, key):
        if key in self._non_defaults:
            return self._non_defaults[key]
        else:
            return self._defaults[key]

    def __setitem__(self, key, value):
        self._non_defaults[key] = value

    def __delitem__(self, key):
        del self._non_defaults[key]


class DialectKWArgs(object):
    """Establish the ability for a class to have dialect-specific arguments
    with defaults and constructor validation.

    The :class:`.DialectKWArgs` interacts with the
    :attr:`.DefaultDialect.construct_arguments` present on a dialect.

    .. seealso::

        :attr:`.DefaultDialect.construct_arguments`

    """

    @classmethod
    def argument_for(cls, dialect_name, argument_name, default):
        """Add a new kind of dialect-specific keyword argument for this class.

        E.g.::

            Index.argument_for("mydialect", "length", None)

            some_index = Index('a', 'b', mydialect_length=5)

        The :meth:`.DialectKWArgs.argument_for` method is a per-argument
        way adding extra arguments to the
        :attr:`.DefaultDialect.construct_arguments` dictionary. This
        dictionary provides a list of argument names accepted by various
        schema-level constructs on behalf of a dialect.

        New dialects should typically specify this dictionary all at once as a
        data member of the dialect class.  The use case for ad-hoc addition of
        argument names is typically for end-user code that is also using
        a custom compilation scheme which consumes the additional arguments.

        :param dialect_name: name of a dialect.  The dialect must be
         locatable, else a :class:`.NoSuchModuleError` is raised.   The
         dialect must also include an existing
         :attr:`.DefaultDialect.construct_arguments` collection, indicating
         that it participates in the keyword-argument validation and default
         system, else :class:`.ArgumentError` is raised.  If the dialect does
         not include this collection, then any keyword argument can be
         specified on behalf of this dialect already.  All dialects packaged
         within SQLAlchemy include this collection, however for third party
         dialects, support may vary.

        :param argument_name: name of the parameter.

        :param default: default value of the parameter.

        .. versionadded:: 0.9.4

        """

        construct_arg_dictionary = DialectKWArgs._kw_registry[dialect_name]
        if construct_arg_dictionary is None:
            raise exc.ArgumentError(
                "Dialect '%s' does have keyword-argument "
                "validation and defaults enabled configured" %
                dialect_name)
        if cls not in construct_arg_dictionary:
            construct_arg_dictionary[cls] = {}
        construct_arg_dictionary[cls][argument_name] = default

    @util.memoized_property
    def dialect_kwargs(self):
        """A collection of keyword arguments specified as dialect-specific
        options to this construct.

        The arguments are present here in their original ``<dialect>_<kwarg>``
        format.  Only arguments that were actually passed are included;
        unlike the :attr:`.DialectKWArgs.dialect_options` collection, which
        contains all options known by this dialect including defaults.

        The collection is also writable; keys are accepted of the
        form ``<dialect>_<kwarg>`` where the value will be assembled
        into the list of options.

        .. versionadded:: 0.9.2

        .. versionchanged:: 0.9.4 The :attr:`.DialectKWArgs.dialect_kwargs`
           collection is now writable.

        .. seealso::

            :attr:`.DialectKWArgs.dialect_options` - nested dictionary form

        """
        return _DialectArgView(self)

    @property
    def kwargs(self):
        """A synonym for :attr:`.DialectKWArgs.dialect_kwargs`."""
        return self.dialect_kwargs

    @util.dependencies("sqlalchemy.dialects")
    def _kw_reg_for_dialect(dialects, dialect_name):
        dialect_cls = dialects.registry.load(dialect_name)
        if dialect_cls.construct_arguments is None:
            return None
        return dict(dialect_cls.construct_arguments)
    _kw_registry = util.PopulateDict(_kw_reg_for_dialect)

    def _kw_reg_for_dialect_cls(self, dialect_name):
        construct_arg_dictionary = DialectKWArgs._kw_registry[dialect_name]
        d = _DialectArgDict()

        if construct_arg_dictionary is None:
            d._defaults.update({"*": None})
        else:
            for cls in reversed(self.__class__.__mro__):
                if cls in construct_arg_dictionary:
                    d._defaults.update(construct_arg_dictionary[cls])
        return d

    @util.memoized_property
    def dialect_options(self):
        """A collection of keyword arguments specified as dialect-specific
        options to this construct.

        This is a two-level nested registry, keyed to ``<dialect_name>``
        and ``<argument_name>``.  For example, the ``postgresql_where``
        argument would be locatable as::

            arg = my_object.dialect_options['postgresql']['where']

        .. versionadded:: 0.9.2

        .. seealso::

            :attr:`.DialectKWArgs.dialect_kwargs` - flat dictionary form

        """

        return util.PopulateDict(
            util.portable_instancemethod(self._kw_reg_for_dialect_cls)
        )

    def _validate_dialect_kwargs(self, kwargs):
        # validate remaining kwargs that they all specify DB prefixes

        if not kwargs:
            return

        for k in kwargs:
            m = re.match('^(.+?)_(.+)$', k)
            if not m:
                raise TypeError(
                    "Additional arguments should be "
                    "named <dialectname>_<argument>, got '%s'" % k)
            dialect_name, arg_name = m.group(1, 2)

            try:
                construct_arg_dictionary = self.dialect_options[dialect_name]
            except exc.NoSuchModuleError:
                util.warn(
                    "Can't validate argument %r; can't "
                    "locate any SQLAlchemy dialect named %r" %
                    (k, dialect_name))
                self.dialect_options[dialect_name] = d = _DialectArgDict()
                d._defaults.update({"*": None})
                d._non_defaults[arg_name] = kwargs[k]
            else:
                if "*" not in construct_arg_dictionary and \
                        arg_name not in construct_arg_dictionary:
                    raise exc.ArgumentError(
                        "Argument %r is not accepted by "
                        "dialect %r on behalf of %r" % (
                            k,
                            dialect_name, self.__class__
                        ))
                else:
                    construct_arg_dictionary[arg_name] = kwargs[k]


class Generative(object):
    """Allow a ClauseElement to generate itself via the
    @_generative decorator.

    """

    def _generate(self):
        s = self.__class__.__new__(self.__class__)
        s.__dict__ = self.__dict__.copy()
        return s


class Executable(Generative):
    """Mark a ClauseElement as supporting execution.

    :class:`.Executable` is a superclass for all "statement" types
    of objects, including :func:`select`, :func:`delete`, :func:`update`,
    :func:`insert`, :func:`text`.

    """

    supports_execution = True
    _execution_options = util.immutabledict()
    _bind = None

    @_generative
    def execution_options(self, **kw):
        """ Set non-SQL options for the statement which take effect during
        execution.

        Execution options can be set on a per-statement or
        per :class:`.Connection` basis.   Additionally, the
        :class:`.Engine` and ORM :class:`~.orm.query.Query` objects provide
        access to execution options which they in turn configure upon
        connections.

        The :meth:`execution_options` method is generative.  A new
        instance of this statement is returned that contains the options::

            statement = select([table.c.x, table.c.y])
            statement = statement.execution_options(autocommit=True)

        Note that only a subset of possible execution options can be applied
        to a statement - these include "autocommit" and "stream_results",
        but not "isolation_level" or "compiled_cache".
        See :meth:`.Connection.execution_options` for a full list of
        possible options.

        .. seealso::

            :meth:`.Connection.execution_options()`

            :meth:`.Query.execution_options()`

        """
        if 'isolation_level' in kw:
            raise exc.ArgumentError(
                "'isolation_level' execution option may only be specified "
                "on Connection.execution_options(), or "
                "per-engine using the isolation_level "
                "argument to create_engine()."
            )
        if 'compiled_cache' in kw:
            raise exc.ArgumentError(
                "'compiled_cache' execution option may only be specified "
                "on Connection.execution_options(), not per statement."
            )
        self._execution_options = self._execution_options.union(kw)

    def execute(self, *multiparams, **params):
        """Compile and execute this :class:`.Executable`."""
        e = self.bind
        if e is None:
            label = getattr(self, 'description', self.__class__.__name__)
            msg = ('This %s is not directly bound to a Connection or Engine.'
                   'Use the .execute() method of a Connection or Engine '
                   'to execute this construct.' % label)
            raise exc.UnboundExecutionError(msg)
        return e._execute_clauseelement(self, multiparams, params)

    def scalar(self, *multiparams, **params):
        """Compile and execute this :class:`.Executable`, returning the
        result's scalar representation.

        """
        return self.execute(*multiparams, **params).scalar()

    @property
    def bind(self):
        """Returns the :class:`.Engine` or :class:`.Connection` to
        which this :class:`.Executable` is bound, or None if none found.

        This is a traversal which checks locally, then
        checks among the "from" clauses of associated objects
        until a bound engine or connection is found.

        """
        if self._bind is not None:
            return self._bind

        for f in _from_objects(self):
            if f is self:
                continue
            engine = f.bind
            if engine is not None:
                return engine
        else:
            return None


class SchemaEventTarget(object):
    """Base class for elements that are the targets of :class:`.DDLEvents`
    events.

    This includes :class:`.SchemaItem` as well as :class:`.SchemaType`.

    """

    def _set_parent(self, parent):
        """Associate with this SchemaEvent's parent object."""

    def _set_parent_with_dispatch(self, parent):
        self.dispatch.before_parent_attach(self, parent)
        self._set_parent(parent)
        self.dispatch.after_parent_attach(self, parent)


class SchemaVisitor(ClauseVisitor):
    """Define the visiting for ``SchemaItem`` objects."""

    __traverse_options__ = {'schema_visitor': True}


class ColumnCollection(util.OrderedProperties):
    """An ordered dictionary that stores a list of ColumnElement
    instances.

    Overrides the ``__eq__()`` method to produce SQL clauses between
    sets of correlated columns.

    """

    __slots__ = '_all_columns'

    def __init__(self, *columns):
        super(ColumnCollection, self).__init__()
        object.__setattr__(self, '_all_columns', [])
        for c in columns:
            self.add(c)

    def __str__(self):
        return repr([str(c) for c in self])

    def replace(self, column):
        """add the given column to this collection, removing unaliased
           versions of this column  as well as existing columns with the
           same key.

            e.g.::

                t = Table('sometable', metadata, Column('col1', Integer))
                t.columns.replace(Column('col1', Integer, key='columnone'))

            will remove the original 'col1' from the collection, and add
            the new column under the name 'columnname'.

           Used by schema.Column to override columns during table reflection.

        """
        remove_col = None
        if column.name in self and column.key != column.name:
            other = self[column.name]
            if other.name == other.key:
                remove_col = other
                del self._data[other.key]

        if column.key in self._data:
            remove_col = self._data[column.key]

        self._data[column.key] = column
        if remove_col is not None:
            self._all_columns[:] = [column if c is remove_col
                                    else c for c in self._all_columns]
        else:
            self._all_columns.append(column)

    def add(self, column):
        """Add a column to this collection.

        The key attribute of the column will be used as the hash key
        for this dictionary.

        """
        if not column.key:
            raise exc.ArgumentError(
                "Can't add unnamed column to column collection")
        self[column.key] = column

    def __delitem__(self, key):
        raise NotImplementedError()

    def __setattr__(self, key, object):
        raise NotImplementedError()

    def __setitem__(self, key, value):
        if key in self:

            # this warning is primarily to catch select() statements
            # which have conflicting column names in their exported
            # columns collection

            existing = self[key]

            if existing is value:
                return

            if not existing.shares_lineage(value):
                util.warn('Column %r on table %r being replaced by '
                          '%r, which has the same key.  Consider '
                          'use_labels for select() statements.' %
                          (key, getattr(existing, 'table', None), value))

            # pop out memoized proxy_set as this
            # operation may very well be occurring
            # in a _make_proxy operation
            util.memoized_property.reset(value, "proxy_set")

        self._all_columns.append(value)
        self._data[key] = value

    def clear(self):
        raise NotImplementedError()

    def remove(self, column):
        del self._data[column.key]
        self._all_columns[:] = [
            c for c in self._all_columns if c is not column]

    def update(self, iter):
        cols = list(iter)
        all_col_set = set(self._all_columns)
        self._all_columns.extend(
            c for label, c in cols if c not in all_col_set)
        self._data.update((label, c) for label, c in cols)

    def extend(self, iter):
        cols = list(iter)
        all_col_set = set(self._all_columns)
        self._all_columns.extend(c for c in cols if c not in all_col_set)
        self._data.update((c.key, c) for c in cols)

    __hash__ = None

    @util.dependencies("sqlalchemy.sql.elements")
    def __eq__(self, elements, other):
        l = []
        for c in getattr(other, "_all_columns", other):
            for local in self._all_columns:
                if c.shares_lineage(local):
                    l.append(c == local)
        return elements.and_(*l)

    def __contains__(self, other):
        if not isinstance(other, util.string_types):
            raise exc.ArgumentError("__contains__ requires a string argument")
        return util.OrderedProperties.__contains__(self, other)

    def __getstate__(self):
        return {'_data': self._data,
                '_all_columns': self._all_columns}

    def __setstate__(self, state):
        object.__setattr__(self, '_data', state['_data'])
        object.__setattr__(self, '_all_columns', state['_all_columns'])

    def contains_column(self, col):
        return col in set(self._all_columns)

    def as_immutable(self):
        return ImmutableColumnCollection(self._data, self._all_columns)


class ImmutableColumnCollection(util.ImmutableProperties, ColumnCollection):
    def __init__(self, data, all_columns):
        util.ImmutableProperties.__init__(self, data)
        object.__setattr__(self, '_all_columns', all_columns)

    extend = remove = util.ImmutableProperties._immutable


class ColumnSet(util.ordered_column_set):
    def contains_column(self, col):
        return col in self

    def extend(self, cols):
        for col in cols:
            self.add(col)

    def __add__(self, other):
        return list(self) + list(other)

    @util.dependencies("sqlalchemy.sql.elements")
    def __eq__(self, elements, other):
        l = []
        for c in other:
            for local in self:
                if c.shares_lineage(local):
                    l.append(c == local)
        return elements.and_(*l)

    def __hash__(self):
        return hash(tuple(x for x in self))


def _bind_or_error(schemaitem, msg=None):
    bind = schemaitem.bind
    if not bind:
        name = schemaitem.__class__.__name__
        label = getattr(schemaitem, 'fullname',
                        getattr(schemaitem, 'name', None))
        if label:
            item = '%s object %r' % (name, label)
        else:
            item = '%s object' % name
        if msg is None:
            msg = "%s is not bound to an Engine or Connection.  "\
                "Execution can not proceed without a database to execute "\
                "against." % item
        raise exc.UnboundExecutionError(msg)
    return bind
