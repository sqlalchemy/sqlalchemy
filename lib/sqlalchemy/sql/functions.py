# sql/functions.py
# Copyright (C) 2005-2019 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""SQL function API, factories, and built-in functions.

"""
from . import annotation
from . import operators
from . import schema
from . import sqltypes
from . import util as sqlutil
from .base import ColumnCollection
from .base import Executable
from .elements import _clone
from .elements import _literal_as_binds
from .elements import _type_from_args
from .elements import BinaryExpression
from .elements import BindParameter
from .elements import Cast
from .elements import ClauseList
from .elements import ColumnElement
from .elements import Extract
from .elements import FunctionFilter
from .elements import Grouping
from .elements import literal_column
from .elements import Over
from .elements import WithinGroup
from .selectable import Alias
from .selectable import FromClause
from .selectable import Select
from .visitors import VisitableType
from .. import util


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

    _has_args = False

    def __init__(self, *clauses, **kwargs):
        """Construct a :class:`.FunctionElement`.
        """
        args = [_literal_as_binds(c, self.name) for c in clauses]
        self._has_args = self._has_args or bool(args)
        self.clause_expr = ClauseList(
            operator=operators.comma_op, group_contents=True, *args
        ).self_group()

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

    def over(self, partition_by=None, order_by=None, rows=None, range_=None):
        """Produce an OVER clause against this function.

        Used against aggregate or so-called "window" functions,
        for database backends that support window functions.

        The expression::

            func.row_number().over(order_by='x')

        is shorthand for::

            from sqlalchemy import over
            over(func.row_number(), order_by='x')

        See :func:`~.expression.over` for a full description.

        """
        return Over(
            self,
            partition_by=partition_by,
            order_by=order_by,
            rows=rows,
            range_=range_,
        )

    def within_group(self, *order_by):
        """Produce a WITHIN GROUP (ORDER BY expr) clause against this function.

        Used against so-called "ordered set aggregate" and "hypothetical
        set aggregate" functions, including :class:`.percentile_cont`,
        :class:`.rank`, :class:`.dense_rank`, etc.

        See :func:`~.expression.within_group` for a full description.

        .. versionadded:: 1.1


        """
        return WithinGroup(self, *order_by)

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

    def as_comparison(self, left_index, right_index):
        """Interpret this expression as a boolean comparison between two values.

        A hypothetical SQL function "is_equal()" which compares to values
        for equality would be written in the Core expression language as::

            expr = func.is_equal("a", "b")

        If "is_equal()" above is comparing "a" and "b" for equality, the
        :meth:`.FunctionElement.as_comparison` method would be invoked as::

            expr = func.is_equal("a", "b").as_comparison(1, 2)

        Where above, the integer value "1" refers to the first argument of the
        "is_equal()" function and the integer value "2" refers to the second.

        This would create a :class:`.BinaryExpression` that is equivalent to::

            BinaryExpression("a", "b", operator=op.eq)

        However, at the SQL level it would still render as
        "is_equal('a', 'b')".

        The ORM, when it loads a related object or collection, needs to be able
        to manipulate the "left" and "right" sides of the ON clause of a JOIN
        expression. The purpose of this method is to provide a SQL function
        construct that can also supply this information to the ORM, when used
        with the :paramref:`.relationship.primaryjoin` parameter.  The return
        value is a containment object called :class:`.FunctionAsBinary`.

        An ORM example is as follows::

            class Venue(Base):
                __tablename__ = 'venue'
                id = Column(Integer, primary_key=True)
                name = Column(String)

                descendants = relationship(
                    "Venue",
                    primaryjoin=func.instr(
                        remote(foreign(name)), name + "/"
                    ).as_comparison(1, 2) == 1,
                    viewonly=True,
                    order_by=name
                )

        Above, the "Venue" class can load descendant "Venue" objects by
        determining if the name of the parent Venue is contained within the
        start of the hypothetical descendant value's name, e.g. "parent1" would
        match up to "parent1/child1", but not to "parent2/child1".

        Possible use cases include the "materialized path" example given above,
        as well as making use of special SQL functions such as geometric
        functions to create join conditions.

        :param left_index: the integer 1-based index of the function argument
         that serves as the "left" side of the expression.
        :param right_index: the integer 1-based index of the function argument
         that serves as the "right" side of the expression.

        .. versionadded:: 1.3

        """
        return FunctionAsBinary(self, left_index, right_index)

    @property
    def _from_objects(self):
        return self.clauses._from_objects

    def get_children(self, **kwargs):
        return (self.clause_expr,)

    def _copy_internals(self, clone=_clone, **kw):
        self.clause_expr = clone(self.clause_expr, **kw)
        self._reset_exported()
        FunctionElement.clauses._reset(self)

    def within_group_type(self, within_group):
        """For types that define their return type as based on the criteria
        within a WITHIN GROUP (ORDER BY) expression, called by the
        :class:`.WithinGroup` construct.

        Returns None by default, in which case the function's normal ``.type``
        is used.

        """

        return None

    def alias(self, name=None, flat=False):
        r"""Produce a :class:`.Alias` construct against this
        :class:`.FunctionElement`.

        This construct wraps the function in a named alias which
        is suitable for the FROM clause, in the style accepted for example
        by PostgreSQL.

        e.g.::

            from sqlalchemy.sql import column

            stmt = select([column('data_view')]).\
                select_from(SomeTable).\
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

    def _bind_param(self, operator, obj, type_=None):
        return BindParameter(
            None,
            obj,
            _compared_to_operator=operator,
            _compared_to_type=self.type,
            unique=True,
            type_=type_,
        )

    def self_group(self, against=None):
        # for the moment, we are parenthesizing all array-returning
        # expressions against getitem.  This may need to be made
        # more portable if in the future we support other DBs
        # besides postgresql.
        if against is operators.getitem and isinstance(
            self.type, sqltypes.ARRAY
        ):
            return Grouping(self)
        else:
            return super(FunctionElement, self).self_group(against=against)


class FunctionAsBinary(BinaryExpression):
    def __init__(self, fn, left_index, right_index):
        left = fn.clauses.clauses[left_index - 1]
        right = fn.clauses.clauses[right_index - 1]
        self.sql_function = fn
        self.left_index = left_index
        self.right_index = right_index

        super(FunctionAsBinary, self).__init__(
            left,
            right,
            operators.function_as_comparison_op,
            type_=sqltypes.BOOLEANTYPE,
        )

    @property
    def left(self):
        return self.sql_function.clauses.clauses[self.left_index - 1]

    @left.setter
    def left(self, value):
        self.sql_function.clauses.clauses[self.left_index - 1] = value

    @property
    def right(self):
        return self.sql_function.clauses.clauses[self.right_index - 1]

    @right.setter
    def right(self, value):
        self.sql_function.clauses.clauses[self.right_index - 1] = value

    def _copy_internals(self, **kw):
        clone = kw.pop("clone")
        self.sql_function = clone(self.sql_function, **kw)
        super(FunctionAsBinary, self)._copy_internals(**kw)


class _FunctionGenerator(object):
    """Generate :class:`.Function` objects based on getattr calls."""

    def __init__(self, **opts):
        self.__names = []
        self.opts = opts

    def __getattr__(self, name):
        # passthru __ attributes; fixes pydoc
        if name.startswith("__"):
            try:
                return self.__dict__[name]
            except KeyError:
                raise AttributeError(name)

        elif name.endswith("_"):
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

        return Function(
            self.__names[-1], packagenames=self.__names[0:-1], *c, **o
        )


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

    __visit_name__ = "function"

    def __init__(self, name, *clauses, **kw):
        """Construct a :class:`.Function`.

        The :data:`.func` construct is normally used to construct
        new :class:`.Function` instances.

        """
        self.packagenames = kw.pop("packagenames", None) or []
        self.name = name
        self._bind = kw.get("bind", None)
        self.type = sqltypes.to_instance(kw.get("type_", None))

        FunctionElement.__init__(self, *clauses, **kw)

    def _bind_param(self, operator, obj, type_=None):
        return BindParameter(
            self.name,
            obj,
            _compared_to_operator=operator,
            _compared_to_type=self.type,
            type_=type_,
            unique=True,
        )


class _GenericMeta(VisitableType):
    def __init__(cls, clsname, bases, clsdict):
        if annotation.Annotated not in cls.__mro__:
            cls.name = name = clsdict.get("name", clsname)
            cls.identifier = identifier = clsdict.get("identifier", name)
            package = clsdict.pop("package", "_default")
            # legacy
            if "__return_type__" in clsdict:
                cls.type = clsdict["__return_type__"]
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

    """

    coerce_arguments = True

    def __init__(self, *args, **kwargs):
        parsed_args = kwargs.pop("_parsed_args", None)
        if parsed_args is None:
            parsed_args = [_literal_as_binds(c, self.name) for c in args]
        self._has_args = self._has_args or bool(parsed_args)
        self.packagenames = []
        self._bind = kwargs.get("bind", None)
        self.clause_expr = ClauseList(
            operator=operators.comma_op, group_contents=True, *parsed_args
        ).self_group()
        self.type = sqltypes.to_instance(
            kwargs.pop("type_", None) or getattr(self, "type", None)
        )


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
        assert isinstance(
            seq, schema.Sequence
        ), "next_value() accepts a Sequence object as input."
        self._bind = kw.get("bind", None)
        self.sequence = seq

    @property
    def _from_objects(self):
        return []


class AnsiFunction(GenericFunction):
    def __init__(self, *args, **kwargs):
        GenericFunction.__init__(self, *args, **kwargs)


class ReturnTypeFromArgs(GenericFunction):
    """Define a function whose return type is the same as its arguments."""

    def __init__(self, *args, **kwargs):
        args = [_literal_as_binds(c, self.name) for c in args]
        kwargs.setdefault("type_", _type_from_args(args))
        kwargs["_parsed_args"] = args
        super(ReturnTypeFromArgs, self).__init__(*args, **kwargs)


class coalesce(ReturnTypeFromArgs):
    _has_args = True


class max(ReturnTypeFromArgs):  # noqa
    pass


class min(ReturnTypeFromArgs):  # noqa
    pass


class sum(ReturnTypeFromArgs):  # noqa
    pass


class now(GenericFunction):  # noqa
    type = sqltypes.DateTime


class concat(GenericFunction):
    type = sqltypes.String


class char_length(GenericFunction):
    type = sqltypes.Integer

    def __init__(self, arg, **kwargs):
        GenericFunction.__init__(self, arg, **kwargs)


class random(GenericFunction):
    _has_args = True


class count(GenericFunction):
    r"""The ANSI COUNT aggregate function.  With no arguments,
    emits COUNT \*.

    E.g.::

        from sqlalchemy import func
        from sqlalchemy import select
        from sqlalchemy import table, column

        my_table = table('some_table', column('id'))

        stmt = select([func.count()]).select_from(my_table)

    Executing ``stmt`` would emit::

        SELECT count(*) AS count_1
        FROM some_table


    """
    type = sqltypes.Integer

    def __init__(self, expression=None, **kwargs):
        if expression is None:
            expression = literal_column("*")
        super(count, self).__init__(expression, **kwargs)


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


class array_agg(GenericFunction):
    """support for the ARRAY_AGG function.

    The ``func.array_agg(expr)`` construct returns an expression of
    type :class:`.types.ARRAY`.

    e.g.::

        stmt = select([func.array_agg(table.c.values)[2:5]])

    .. versionadded:: 1.1

    .. seealso::

        :func:`.postgresql.array_agg` - PostgreSQL-specific version that
        returns :class:`.postgresql.ARRAY`, which has PG-specific operators
        added.

    """

    type = sqltypes.ARRAY

    def __init__(self, *args, **kwargs):
        args = [_literal_as_binds(c) for c in args]

        default_array_type = kwargs.pop("_default_array_type", sqltypes.ARRAY)
        if "type_" not in kwargs:

            type_from_args = _type_from_args(args)
            if isinstance(type_from_args, sqltypes.ARRAY):
                kwargs["type_"] = type_from_args
            else:
                kwargs["type_"] = default_array_type(type_from_args)
        kwargs["_parsed_args"] = args
        super(array_agg, self).__init__(*args, **kwargs)


class OrderedSetAgg(GenericFunction):
    """Define a function where the return type is based on the sort
    expression type as defined by the expression passed to the
    :meth:`.FunctionElement.within_group` method."""

    array_for_multi_clause = False

    def within_group_type(self, within_group):
        func_clauses = self.clause_expr.element
        order_by = sqlutil.unwrap_order_by(within_group.order_by)
        if self.array_for_multi_clause and len(func_clauses.clauses) > 1:
            return sqltypes.ARRAY(order_by[0].type)
        else:
            return order_by[0].type


class mode(OrderedSetAgg):
    """implement the ``mode`` ordered-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is the same as the sort expression.

    .. versionadded:: 1.1

    """


class percentile_cont(OrderedSetAgg):
    """implement the ``percentile_cont`` ordered-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is the same as the sort expression,
    or if the arguments are an array, an :class:`.types.ARRAY` of the sort
    expression's type.

    .. versionadded:: 1.1

    """

    array_for_multi_clause = True


class percentile_disc(OrderedSetAgg):
    """implement the ``percentile_disc`` ordered-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is the same as the sort expression,
    or if the arguments are an array, an :class:`.types.ARRAY` of the sort
    expression's type.

    .. versionadded:: 1.1

    """

    array_for_multi_clause = True


class rank(GenericFunction):
    """Implement the ``rank`` hypothetical-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is :class:`.Integer`.

    .. versionadded:: 1.1

    """

    type = sqltypes.Integer()


class dense_rank(GenericFunction):
    """Implement the ``dense_rank`` hypothetical-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is :class:`.Integer`.

    .. versionadded:: 1.1

    """

    type = sqltypes.Integer()


class percent_rank(GenericFunction):
    """Implement the ``percent_rank`` hypothetical-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is :class:`.Numeric`.

    .. versionadded:: 1.1

    """

    type = sqltypes.Numeric()


class cume_dist(GenericFunction):
    """Implement the ``cume_dist`` hypothetical-set aggregate function.

    This function must be used with the :meth:`.FunctionElement.within_group`
    modifier to supply a sort expression to operate upon.

    The return type of this function is :class:`.Numeric`.

    .. versionadded:: 1.1

    """

    type = sqltypes.Numeric()


class cube(GenericFunction):
    r"""Implement the ``CUBE`` grouping operation.

    This function is used as part of the GROUP BY of a statement,
    e.g. :meth:`.Select.group_by`::

        stmt = select(
            [func.sum(table.c.value), table.c.col_1, table.c.col_2]
            ).group_by(func.cube(table.c.col_1, table.c.col_2))

    .. versionadded:: 1.2

    """
    _has_args = True


class rollup(GenericFunction):
    r"""Implement the ``ROLLUP`` grouping operation.

    This function is used as part of the GROUP BY of a statement,
    e.g. :meth:`.Select.group_by`::

        stmt = select(
            [func.sum(table.c.value), table.c.col_1, table.c.col_2]
        ).group_by(func.rollup(table.c.col_1, table.c.col_2))

    .. versionadded:: 1.2

    """
    _has_args = True


class grouping_sets(GenericFunction):
    r"""Implement the ``GROUPING SETS`` grouping operation.

    This function is used as part of the GROUP BY of a statement,
    e.g. :meth:`.Select.group_by`::

        stmt = select(
            [func.sum(table.c.value), table.c.col_1, table.c.col_2]
        ).group_by(func.grouping_sets(table.c.col_1, table.c.col_2))

    In order to group by multiple sets, use the :func:`.tuple_` construct::

        from sqlalchemy import tuple_

        stmt = select(
            [
                func.sum(table.c.value),
                table.c.col_1, table.c.col_2,
                table.c.col_3]
        ).group_by(
            func.grouping_sets(
                tuple_(table.c.col_1, table.c.col_2),
                tuple_(table.c.value, table.c.col_3),
            )
        )


    .. versionadded:: 1.2

    """
    _has_args = True
