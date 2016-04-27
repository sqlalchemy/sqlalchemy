# sql/functions.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL function API, factories, and built-in functions.

"""
from . import sqltypes, schema
from .base import Executable, ColumnCollection
from .elements import ClauseList, Cast, Extract, _literal_as_binds, \
    literal_column, _type_from_args, ColumnElement, _clone,\
    Over, BindParameter, FunctionFilter
from .selectable import FromClause, Select, Alias

from . import operators
from .visitors import VisitableType
from .. import util
from . import annotation

_registry = util.defaultdict(dict)


def register_function(identifier, fn, package="_default"):
    """Associate a callable with a particular func. name.

    This is normally called by _GenericMeta, but is also
    available by itself so that a non-Function construct
    can be associated with the :data:`.func` accessor (i.e.
    CAST, EXTRACT).

    """
    reg = _registry[package]
    reg[identifier] = fn


class FunctionElement(Executable, ColumnElement, FromClause):
    """Base for SQL function-oriented constructs.

    .. seealso::

        :class:`.Function` - named SQL function.

        :data:`.func` - namespace which produces registered or ad-hoc
        :class:`.Function` instances.

        :class:`.GenericFunction` - allows creation of registered function
        types.

    """

    packagenames = ()

    def __init__(self, *clauses, **kwargs):
        """Construct a :class:`.FunctionElement`.
        """
        args = [_literal_as_binds(c, self.name) for c in clauses]
        self.clause_expr = ClauseList(
            operator=operators.comma_op,
            group_contents=True, *args).\
            self_group()

    def _execute_on_connection(self, connection, multiparams, params):
        return connection._execute_function(self, multiparams, params)

    @property
    def columns(self):
        """The set of columns exported by this :class:`.FunctionElement`.

        Function objects currently have no result column names built in;
        this method returns a single-element column collection with
        an anonymously named column.

        An interim approach to providing named columns for a function
        as a FROM clause is to build a :func:`.select` with the
        desired columns::

            from sqlalchemy.sql import column

            stmt = select([column('x'), column('y')]).\
                select_from(func.myfunction())


        """
        return ColumnCollection(self.label(None))

    @util.memoized_property
    def clauses(self):
        """Return the underlying :class:`.ClauseList` which contains
        the arguments for this :class:`.FunctionElement`.

        """
        return self.clause_expr.element

    def over(self, partition_by=None, order_by=None):
        """Produce an OVER clause against this function.

        Used against aggregate or so-called "window" functions,
        for database backends that support window functions.

        The expression::

            func.row_number().over(order_by='x')

        is shorthand for::

            from sqlalchemy import over
            over(func.row_number(), order_by='x')

        See :func:`~.expression.over` for a full description.

        .. versionadded:: 0.7

        """
        return Over(self, partition_by=partition_by, order_by=order_by)

    def filter(self, *criterion):
        """Produce a FILTER clause against this function.

        Used against aggregate and window functions,
        for database backends that support the "FILTER" clause.

        The expression::

            func.count(1).filter(True)

        is shorthand for::

            from sqlalchemy import funcfilter
            funcfilter(func.count(1), True)

        .. versionadded:: 1.0.0

        .. seealso::

            :class:`.FunctionFilter`

            :func:`.funcfilter`


        """
        if not criterion:
            return self
        return FunctionFilter(self, *criterion)

    @property
    def _from_objects(self):
        return self.clauses._from_objects

    def get_children(self, **kwargs):
        return self.clause_expr,

    def _copy_internals(self, clone=_clone, **kw):
        self.clause_expr = clone(self.clause_expr, **kw)
        self._reset_exported()
        FunctionElement.clauses._reset(self)

    def alias(self, name=None, flat=False):
        """Produce a :class:`.Alias` construct against this
        :class:`.FunctionElement`.

        This construct wraps the function in a named alias which
        is suitable for the FROM clause, in the style accepted for example
        by Postgresql.

        e.g.::

            from sqlalchemy.sql import column

            stmt = select([column('data_view')]).\\
                select_from(SomeTable).\\
                select_from(func.unnest(SomeTable.data).alias('data_view')
            )

        Would produce:

        .. sourcecode:: sql

            SELECT data_view
            FROM sometable, unnest(sometable.data) AS data_view

        .. versionadded:: 0.9.8 The :meth:`.FunctionElement.alias` method
           is now supported.  Previously, this method's behavior was
           undefined and did not behave consistently across versions.

        """

        return Alias(self, name)

    def select(self):
        """Produce a :func:`~.expression.select` construct
        against this :class:`.FunctionElement`.

        This is shorthand for::

            s = select([function_element])

        """
        s = Select([self])
        if self._execution_options:
            s = s.execution_options(**self._execution_options)
        return s

    def scalar(self):
        """Execute this :class:`.FunctionElement` against an embedded
        'bind' and return a scalar value.

        This first calls :meth:`~.FunctionElement.select` to
        produce a SELECT construct.

        Note that :class:`.FunctionElement` can be passed to
        the :meth:`.Connectable.scalar` method of :class:`.Connection`
        or :class:`.Engine`.

        """
        return self.select().execute().scalar()

    def execute(self):
        """Execute this :class:`.FunctionElement` against an embedded
        'bind'.

        This first calls :meth:`~.FunctionElement.select` to
        produce a SELECT construct.

        Note that :class:`.FunctionElement` can be passed to
        the :meth:`.Connectable.execute` method of :class:`.Connection`
        or :class:`.Engine`.

        """
        return self.select().execute()

    def _bind_param(self, operator, obj):
        return BindParameter(None, obj, _compared_to_operator=operator,
                             _compared_to_type=self.type, unique=True)


class _FunctionGenerator(object):
    """Generate :class:`.Function` objects based on getattr calls."""

    def __init__(self, **opts):
        self.__names = []
        self.opts = opts

    def __getattr__(self, name):
        # passthru __ attributes; fixes pydoc
        if name.startswith('__'):
            try:
                return self.__dict__[name]
            except KeyError:
                raise AttributeError(name)

        elif name.endswith('_'):
            name = name[0:-1]
        f = _FunctionGenerator(**self.opts)
        f.__names = list(self.__names) + [name]
        return f

    def __call__(self, *c, **kwargs):
        o = self.opts.copy()
        o.update(kwargs)

        tokens = len(self.__names)

        if tokens == 2:
            package, fname = self.__names
        elif tokens == 1:
            package, fname = "_default", self.__names[0]
        else:
            package = None

        if package is not None:
            func = _registry[package].get(fname)
            if func is not None:
                return func(*c, **o)

        return Function(self.__names[-1],
                        packagenames=self.__names[0:-1], *c, **o)


func = _FunctionGenerator()
"""Generate SQL function expressions.

   :data:`.func` is a special object instance which generates SQL
   functions based on name-based attributes, e.g.::

        >>> print(func.count(1))
        count(:param_1)

   The element is a column-oriented SQL element like any other, and is
   used in that way::

        >>> print(select([func.count(table.c.id)]))
        SELECT count(sometable.id) FROM sometable

   Any name can be given to :data:`.func`. If the function name is unknown to
   SQLAlchemy, it will be rendered exactly as is. For common SQL functions
   which SQLAlchemy is aware of, the name may be interpreted as a *generic
   function* which will be compiled appropriately to the target database::

        >>> print(func.current_timestamp())
        CURRENT_TIMESTAMP

   To call functions which are present in dot-separated packages,
   specify them in the same manner::

        >>> print(func.stats.yield_curve(5, 10))
        stats.yield_curve(:yield_curve_1, :yield_curve_2)

   SQLAlchemy can be made aware of the return type of functions to enable
   type-specific lexical and result-based behavior. For example, to ensure
   that a string-based function returns a Unicode value and is similarly
   treated as a string in expressions, specify
   :class:`~sqlalchemy.types.Unicode` as the type:

        >>> print(func.my_string(u'hi', type_=Unicode) + ' ' +
        ...       func.my_string(u'there', type_=Unicode))
        my_string(:my_string_1) || :my_string_2 || my_string(:my_string_3)

   The object returned by a :data:`.func` call is usually an instance of
   :class:`.Function`.
   This object meets the "column" interface, including comparison and labeling
   functions.  The object can also be passed the :meth:`~.Connectable.execute`
   method of a :class:`.Connection` or :class:`.Engine`, where it will be
   wrapped inside of a SELECT statement first::

        print(connection.execute(func.current_timestamp()).scalar())

   In a few exception cases, the :data:`.func` accessor
   will redirect a name to a built-in expression such as :func:`.cast`
   or :func:`.extract`, as these names have well-known meaning
   but are not exactly the same as "functions" from a SQLAlchemy
   perspective.

   .. versionadded:: 0.8 :data:`.func` can return non-function expression
      constructs for common quasi-functional names like :func:`.cast`
      and :func:`.extract`.

   Functions which are interpreted as "generic" functions know how to
   calculate their return type automatically. For a listing of known generic
   functions, see :ref:`generic_functions`.

   .. note::

        The :data:`.func` construct has only limited support for calling
        standalone "stored procedures", especially those with special
        parameterization concerns.

        See the section :ref:`stored_procedures` for details on how to use
        the DBAPI-level ``callproc()`` method for fully traditional stored
        procedures.

"""

modifier = _FunctionGenerator(group=False)


class Function(FunctionElement):
    """Describe a named SQL function.

    See the superclass :class:`.FunctionElement` for a description
    of public methods.

    .. seealso::

        :data:`.func` - namespace which produces registered or ad-hoc
        :class:`.Function` instances.

        :class:`.GenericFunction` - allows creation of registered function
        types.

    """

    __visit_name__ = 'function'

    def __init__(self, name, *clauses, **kw):
        """Construct a :class:`.Function`.

        The :data:`.func` construct is normally used to construct
        new :class:`.Function` instances.

        """
        self.packagenames = kw.pop('packagenames', None) or []
        self.name = name
        self._bind = kw.get('bind', None)
        self.type = sqltypes.to_instance(kw.get('type_', None))

        FunctionElement.__init__(self, *clauses, **kw)

    def _bind_param(self, operator, obj):
        return BindParameter(self.name, obj,
                             _compared_to_operator=operator,
                             _compared_to_type=self.type,
                             unique=True)


class _GenericMeta(VisitableType):
    def __init__(cls, clsname, bases, clsdict):
        if annotation.Annotated not in cls.__mro__:
            cls.name = name = clsdict.get('name', clsname)
            cls.identifier = identifier = clsdict.get('identifier', name)
            package = clsdict.pop('package', '_default')
            # legacy
            if '__return_type__' in clsdict:
                cls.type = clsdict['__return_type__']
            register_function(identifier, cls, package)
        super(_GenericMeta, cls).__init__(clsname, bases, clsdict)


class GenericFunction(util.with_metaclass(_GenericMeta, Function)):
    """Define a 'generic' function.

    A generic function is a pre-established :class:`.Function`
    class that is instantiated automatically when called
    by name from the :data:`.func` attribute.    Note that
    calling any name from :data:`.func` has the effect that
    a new :class:`.Function` instance is created automatically,
    given that name.  The primary use case for defining
    a :class:`.GenericFunction` class is so that a function
    of a particular name may be given a fixed return type.
    It can also include custom argument parsing schemes as well
    as additional methods.

    Subclasses of :class:`.GenericFunction` are automatically
    registered under the name of the class.  For
    example, a user-defined function ``as_utc()`` would
    be available immediately::

        from sqlalchemy.sql.functions import GenericFunction
        from sqlalchemy.types import DateTime

        class as_utc(GenericFunction):
            type = DateTime

        print select([func.as_utc()])

    User-defined generic functions can be organized into
    packages by specifying the "package" attribute when defining
    :class:`.GenericFunction`.   Third party libraries
    containing many functions may want to use this in order
    to avoid name conflicts with other systems.   For example,
    if our ``as_utc()`` function were part of a package
    "time"::

        class as_utc(GenericFunction):
            type = DateTime
            package = "time"

    The above function would be available from :data:`.func`
    using the package name ``time``::

        print select([func.time.as_utc()])

    A final option is to allow the function to be accessed
    from one name in :data:`.func` but to render as a different name.
    The ``identifier`` attribute will override the name used to
    access the function as loaded from :data:`.func`, but will retain
    the usage of ``name`` as the rendered name::

        class GeoBuffer(GenericFunction):
            type = Geometry
            package = "geo"
            name = "ST_Buffer"
            identifier = "buffer"

    The above function will render as follows::

        >>> print func.geo.buffer()
        ST_Buffer()

    .. versionadded:: 0.8 :class:`.GenericFunction` now supports
       automatic registration of new functions as well as package
       and custom naming support.

    .. versionchanged:: 0.8 The attribute name ``type`` is used
       to specify the function's return type at the class level.
       Previously, the name ``__return_type__`` was used.  This
       name is still recognized for backwards-compatibility.

    """

    coerce_arguments = True

    def __init__(self, *args, **kwargs):
        parsed_args = kwargs.pop('_parsed_args', None)
        if parsed_args is None:
            parsed_args = [_literal_as_binds(c) for c in args]
        self.packagenames = []
        self._bind = kwargs.get('bind', None)
        self.clause_expr = ClauseList(
            operator=operators.comma_op,
            group_contents=True, *parsed_args).self_group()
        self.type = sqltypes.to_instance(
            kwargs.pop("type_", None) or getattr(self, 'type', None))

register_function("cast", Cast)
register_function("extract", Extract)


class next_value(GenericFunction):
    """Represent the 'next value', given a :class:`.Sequence`
    as its single argument.

    Compiles into the appropriate function on each backend,
    or will raise NotImplementedError if used on a backend
    that does not provide support for sequences.

    """
    type = sqltypes.Integer()
    name = "next_value"

    def __init__(self, seq, **kw):
        assert isinstance(seq, schema.Sequence), \
            "next_value() accepts a Sequence object as input."
        self._bind = kw.get('bind', None)
        self.sequence = seq

    @property
    def _from_objects(self):
        return []


class AnsiFunction(GenericFunction):
    def __init__(self, **kwargs):
        GenericFunction.__init__(self, **kwargs)


class ReturnTypeFromArgs(GenericFunction):
    """Define a function whose return type is the same as its arguments."""

    def __init__(self, *args, **kwargs):
        args = [_literal_as_binds(c) for c in args]
        kwargs.setdefault('type_', _type_from_args(args))
        kwargs['_parsed_args'] = args
        GenericFunction.__init__(self, *args, **kwargs)


class coalesce(ReturnTypeFromArgs):
    pass


class max(ReturnTypeFromArgs):
    pass


class min(ReturnTypeFromArgs):
    pass


class sum(ReturnTypeFromArgs):
    pass


class now(GenericFunction):
    type = sqltypes.DateTime


class concat(GenericFunction):
    type = sqltypes.String


class char_length(GenericFunction):
    type = sqltypes.Integer

    def __init__(self, arg, **kwargs):
        GenericFunction.__init__(self, arg, **kwargs)


class random(GenericFunction):
    pass


class count(GenericFunction):
    """The ANSI COUNT aggregate function.  With no arguments,
    emits COUNT \*.

    """
    type = sqltypes.Integer

    def __init__(self, expression=None, **kwargs):
        if expression is None:
            expression = literal_column('*')
        GenericFunction.__init__(self, expression, **kwargs)


class current_date(AnsiFunction):
    type = sqltypes.Date


class current_time(AnsiFunction):
    type = sqltypes.Time


class current_timestamp(AnsiFunction):
    type = sqltypes.DateTime


class current_user(AnsiFunction):
    type = sqltypes.String


class localtime(AnsiFunction):
    type = sqltypes.DateTime


class localtimestamp(AnsiFunction):
    type = sqltypes.DateTime


class session_user(AnsiFunction):
    type = sqltypes.String


class sysdate(AnsiFunction):
    type = sqltypes.DateTime


class user(AnsiFunction):
    type = sqltypes.String
