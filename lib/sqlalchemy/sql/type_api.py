# sql/types_api.py
# Copyright (C) 2005-2015 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Base types API.

"""


from .. import exc, util
from . import operators
from .visitors import Visitable

# these are back-assigned by sqltypes.
BOOLEANTYPE = None
INTEGERTYPE = None
NULLTYPE = None
STRINGTYPE = None


class TypeEngine(Visitable):
    """The ultimate base class for all SQL datatypes.

    Common subclasses of :class:`.TypeEngine` include
    :class:`.String`, :class:`.Integer`, and :class:`.Boolean`.

    For an overview of the SQLAlchemy typing system, see
    :ref:`types_toplevel`.

    .. seealso::

        :ref:`types_toplevel`

    """

    _sqla_type = True
    _isnull = False

    class Comparator(operators.ColumnOperators):
        """Base class for custom comparison operations defined at the
        type level.  See :attr:`.TypeEngine.comparator_factory`.


        """

        def __init__(self, expr):
            self.expr = expr

        def __reduce__(self):
            return _reconstitute_comparator, (self.expr, )

    hashable = True
    """Flag, if False, means values from this type aren't hashable.

    Used by the ORM when uniquing result lists.

    """

    comparator_factory = Comparator
    """A :class:`.TypeEngine.Comparator` class which will apply
    to operations performed by owning :class:`.ColumnElement` objects.

    The :attr:`.comparator_factory` attribute is a hook consulted by
    the core expression system when column and SQL expression operations
    are performed.   When a :class:`.TypeEngine.Comparator` class is
    associated with this attribute, it allows custom re-definition of
    all existing operators, as well as definition of new operators.
    Existing operators include those provided by Python operator overloading
    such as :meth:`.operators.ColumnOperators.__add__` and
    :meth:`.operators.ColumnOperators.__eq__`,
    those provided as standard
    attributes of :class:`.operators.ColumnOperators` such as
    :meth:`.operators.ColumnOperators.like`
    and :meth:`.operators.ColumnOperators.in_`.

    Rudimentary usage of this hook is allowed through simple subclassing
    of existing types, or alternatively by using :class:`.TypeDecorator`.
    See the documentation section :ref:`types_operators` for examples.

    .. versionadded:: 0.8  The expression system was enhanced to support
      customization of operators on a per-type level.

    """

    def copy_value(self, value):
        return value

    def literal_processor(self, dialect):
        """Return a conversion function for processing literal values that are
        to be rendered directly without using binds.

        This function is used when the compiler makes use of the
        "literal_binds" flag, typically used in DDL generation as well
        as in certain scenarios where backends don't accept bound parameters.

        .. versionadded:: 0.9.0

        """
        return None

    def bind_processor(self, dialect):
        """Return a conversion function for processing bind values.

        Returns a callable which will receive a bind parameter value
        as the sole positional argument and will return a value to
        send to the DB-API.

        If processing is not necessary, the method should return ``None``.

        :param dialect: Dialect instance in use.

        """
        return None

    def result_processor(self, dialect, coltype):
        """Return a conversion function for processing result row values.

        Returns a callable which will receive a result row column
        value as the sole positional argument and will return a value
        to return to the user.

        If processing is not necessary, the method should return ``None``.

        :param dialect: Dialect instance in use.

        :param coltype: DBAPI coltype argument received in cursor.description.

        """
        return None

    def column_expression(self, colexpr):
        """Given a SELECT column expression, return a wrapping SQL expression.

        This is typically a SQL function that wraps a column expression
        as rendered in the columns clause of a SELECT statement.
        It is used for special data types that require
        columns to be wrapped in some special database function in order
        to coerce the value before being sent back to the application.
        It is the SQL analogue of the :meth:`.TypeEngine.result_processor`
        method.

        The method is evaluated at statement compile time, as opposed
        to statement construction time.

        See also:

        :ref:`types_sql_value_processing`

        """

        return None

    @util.memoized_property
    def _has_column_expression(self):
        """memoized boolean, check if column_expression is implemented.

        Allows the method to be skipped for the vast majority of expression
        types that don't use this feature.

        """

        return self.__class__.column_expression.__code__ \
            is not TypeEngine.column_expression.__code__

    def bind_expression(self, bindvalue):
        """"Given a bind value (i.e. a :class:`.BindParameter` instance),
        return a SQL expression in its place.

        This is typically a SQL function that wraps the existing bound
        parameter within the statement.  It is used for special data types
        that require literals being wrapped in some special database function
        in order to coerce an application-level value into a database-specific
        format.  It is the SQL analogue of the
        :meth:`.TypeEngine.bind_processor` method.

        The method is evaluated at statement compile time, as opposed
        to statement construction time.

        Note that this method, when implemented, should always return
        the exact same structure, without any conditional logic, as it
        may be used in an executemany() call against an arbitrary number
        of bound parameter sets.

        See also:

        :ref:`types_sql_value_processing`

        """
        return None

    @util.memoized_property
    def _has_bind_expression(self):
        """memoized boolean, check if bind_expression is implemented.

        Allows the method to be skipped for the vast majority of expression
        types that don't use this feature.

        """

        return self.__class__.bind_expression.__code__ \
            is not TypeEngine.bind_expression.__code__

    def compare_values(self, x, y):
        """Compare two values for equality."""

        return x == y

    def get_dbapi_type(self, dbapi):
        """Return the corresponding type object from the underlying DB-API, if
        any.

         This can be useful for calling ``setinputsizes()``, for example.

        """
        return None

    @property
    def python_type(self):
        """Return the Python type object expected to be returned
        by instances of this type, if known.

        Basically, for those types which enforce a return type,
        or are known across the board to do such for all common
        DBAPIs (like ``int`` for example), will return that type.

        If a return type is not defined, raises
        ``NotImplementedError``.

        Note that any type also accommodates NULL in SQL which
        means you can also get back ``None`` from any type
        in practice.

        """
        raise NotImplementedError()

    def with_variant(self, type_, dialect_name):
        """Produce a new type object that will utilize the given
        type when applied to the dialect of the given name.

        e.g.::

            from sqlalchemy.types import String
            from sqlalchemy.dialects import mysql

            s = String()

            s = s.with_variant(mysql.VARCHAR(collation='foo'), 'mysql')

        The construction of :meth:`.TypeEngine.with_variant` is always
        from the "fallback" type to that which is dialect specific.
        The returned type is an instance of :class:`.Variant`, which
        itself provides a :meth:`.Variant.with_variant`
        that can be called repeatedly.

        :param type_: a :class:`.TypeEngine` that will be selected
         as a variant from the originating type, when a dialect
         of the given name is in use.
        :param dialect_name: base name of the dialect which uses
         this type. (i.e. ``'postgresql'``, ``'mysql'``, etc.)

        .. versionadded:: 0.7.2

        """
        return Variant(self, {dialect_name: to_instance(type_)})

    @util.memoized_property
    def _type_affinity(self):
        """Return a rudimental 'affinity' value expressing the general class
        of type."""

        typ = None
        for t in self.__class__.__mro__:
            if t in (TypeEngine, UserDefinedType):
                return typ
            elif issubclass(t, (TypeEngine, UserDefinedType)):
                typ = t
        else:
            return self.__class__

    def dialect_impl(self, dialect):
        """Return a dialect-specific implementation for this
        :class:`.TypeEngine`.

        """
        try:
            return dialect._type_memos[self]['impl']
        except KeyError:
            return self._dialect_info(dialect)['impl']

    def _cached_literal_processor(self, dialect):
        """Return a dialect-specific literal processor for this type."""
        try:
            return dialect._type_memos[self]['literal']
        except KeyError:
            d = self._dialect_info(dialect)
            d['literal'] = lp = d['impl'].literal_processor(dialect)
            return lp

    def _cached_bind_processor(self, dialect):
        """Return a dialect-specific bind processor for this type."""

        try:
            return dialect._type_memos[self]['bind']
        except KeyError:
            d = self._dialect_info(dialect)
            d['bind'] = bp = d['impl'].bind_processor(dialect)
            return bp

    def _cached_result_processor(self, dialect, coltype):
        """Return a dialect-specific result processor for this type."""

        try:
            return dialect._type_memos[self][coltype]
        except KeyError:
            d = self._dialect_info(dialect)
            # key assumption: DBAPI type codes are
            # constants.  Else this dictionary would
            # grow unbounded.
            d[coltype] = rp = d['impl'].result_processor(dialect, coltype)
            return rp

    def _dialect_info(self, dialect):
        """Return a dialect-specific registry which
        caches a dialect-specific implementation, bind processing
        function, and one or more result processing functions."""

        if self in dialect._type_memos:
            return dialect._type_memos[self]
        else:
            impl = self._gen_dialect_impl(dialect)
            if impl is self:
                impl = self.adapt(type(self))
            # this can't be self, else we create a cycle
            assert impl is not self
            dialect._type_memos[self] = d = {'impl': impl}
            return d

    def _gen_dialect_impl(self, dialect):
        return dialect.type_descriptor(self)

    def adapt(self, cls, **kw):
        """Produce an "adapted" form of this type, given an "impl" class
        to work with.

        This method is used internally to associate generic
        types with "implementation" types that are specific to a particular
        dialect.
        """
        return util.constructor_copy(self, cls, **kw)

    def coerce_compared_value(self, op, value):
        """Suggest a type for a 'coerced' Python value in an expression.

        Given an operator and value, gives the type a chance
        to return a type which the value should be coerced into.

        The default behavior here is conservative; if the right-hand
        side is already coerced into a SQL type based on its
        Python type, it is usually left alone.

        End-user functionality extension here should generally be via
        :class:`.TypeDecorator`, which provides more liberal behavior in that
        it defaults to coercing the other side of the expression into this
        type, thus applying special Python conversions above and beyond those
        needed by the DBAPI to both ides. It also provides the public method
        :meth:`.TypeDecorator.coerce_compared_value` which is intended for
        end-user customization of this behavior.

        """
        _coerced_type = _type_map.get(type(value), NULLTYPE)
        if _coerced_type is NULLTYPE or _coerced_type._type_affinity \
                is self._type_affinity:
            return self
        else:
            return _coerced_type

    def _compare_type_affinity(self, other):
        return self._type_affinity is other._type_affinity

    def compile(self, dialect=None):
        """Produce a string-compiled form of this :class:`.TypeEngine`.

        When called with no arguments, uses a "default" dialect
        to produce a string result.

        :param dialect: a :class:`.Dialect` instance.

        """
        # arg, return value is inconsistent with
        # ClauseElement.compile()....this is a mistake.

        if not dialect:
            dialect = self._default_dialect()

        return dialect.type_compiler.process(self)

    @util.dependencies("sqlalchemy.engine.default")
    def _default_dialect(self, default):
        if self.__class__.__module__.startswith("sqlalchemy.dialects"):
            tokens = self.__class__.__module__.split(".")[0:3]
            mod = ".".join(tokens)
            return getattr(__import__(mod).dialects, tokens[-1]).dialect()
        else:
            return default.DefaultDialect()

    def __str__(self):
        if util.py2k:
            return unicode(self.compile()).\
                encode('ascii', 'backslashreplace')
        else:
            return str(self.compile())

    def __repr__(self):
        return util.generic_repr(self)


class UserDefinedType(TypeEngine):
    """Base for user defined types.

    This should be the base of new types.  Note that
    for most cases, :class:`.TypeDecorator` is probably
    more appropriate::

      import sqlalchemy.types as types

      class MyType(types.UserDefinedType):
          def __init__(self, precision = 8):
              self.precision = precision

          def get_col_spec(self):
              return "MYTYPE(%s)" % self.precision

          def bind_processor(self, dialect):
              def process(value):
                  return value
              return process

          def result_processor(self, dialect, coltype):
              def process(value):
                  return value
              return process

    Once the type is made, it's immediately usable::

      table = Table('foo', meta,
          Column('id', Integer, primary_key=True),
          Column('data', MyType(16))
          )

    """
    __visit_name__ = "user_defined"

    class Comparator(TypeEngine.Comparator):
        def _adapt_expression(self, op, other_comparator):
            if hasattr(self.type, 'adapt_operator'):
                util.warn_deprecated(
                    "UserDefinedType.adapt_operator is deprecated.  Create "
                    "a UserDefinedType.Comparator subclass instead which "
                    "generates the desired expression constructs, given a "
                    "particular operator."
                )
                return self.type.adapt_operator(op), self.type
            else:
                return op, self.type

    comparator_factory = Comparator

    def coerce_compared_value(self, op, value):
        """Suggest a type for a 'coerced' Python value in an expression.

        Default behavior for :class:`.UserDefinedType` is the
        same as that of :class:`.TypeDecorator`; by default it returns
        ``self``, assuming the compared value should be coerced into
        the same type as this one.  See
        :meth:`.TypeDecorator.coerce_compared_value` for more detail.

        .. versionchanged:: 0.8 :meth:`.UserDefinedType.coerce_compared_value`
           now returns ``self`` by default, rather than falling onto the
           more fundamental behavior of
           :meth:`.TypeEngine.coerce_compared_value`.

        """

        return self


class TypeDecorator(TypeEngine):
    """Allows the creation of types which add additional functionality
    to an existing type.

    This method is preferred to direct subclassing of SQLAlchemy's
    built-in types as it ensures that all required functionality of
    the underlying type is kept in place.

    Typical usage::

      import sqlalchemy.types as types

      class MyType(types.TypeDecorator):
          '''Prefixes Unicode values with "PREFIX:" on the way in and
          strips it off on the way out.
          '''

          impl = types.Unicode

          def process_bind_param(self, value, dialect):
              return "PREFIX:" + value

          def process_result_value(self, value, dialect):
              return value[7:]

          def copy(self):
              return MyType(self.impl.length)

    The class-level "impl" attribute is required, and can reference any
    TypeEngine class.  Alternatively, the load_dialect_impl() method
    can be used to provide different type classes based on the dialect
    given; in this case, the "impl" variable can reference
    ``TypeEngine`` as a placeholder.

    Types that receive a Python type that isn't similar to the ultimate type
    used may want to define the :meth:`TypeDecorator.coerce_compared_value`
    method. This is used to give the expression system a hint when coercing
    Python objects into bind parameters within expressions. Consider this
    expression::

        mytable.c.somecol + datetime.date(2009, 5, 15)

    Above, if "somecol" is an ``Integer`` variant, it makes sense that
    we're doing date arithmetic, where above is usually interpreted
    by databases as adding a number of days to the given date.
    The expression system does the right thing by not attempting to
    coerce the "date()" value into an integer-oriented bind parameter.

    However, in the case of ``TypeDecorator``, we are usually changing an
    incoming Python type to something new - ``TypeDecorator`` by default will
    "coerce" the non-typed side to be the same type as itself. Such as below,
    we define an "epoch" type that stores a date value as an integer::

        class MyEpochType(types.TypeDecorator):
            impl = types.Integer

            epoch = datetime.date(1970, 1, 1)

            def process_bind_param(self, value, dialect):
                return (value - self.epoch).days

            def process_result_value(self, value, dialect):
                return self.epoch + timedelta(days=value)

    Our expression of ``somecol + date`` with the above type will coerce the
    "date" on the right side to also be treated as ``MyEpochType``.

    This behavior can be overridden via the
    :meth:`~TypeDecorator.coerce_compared_value` method, which returns a type
    that should be used for the value of the expression. Below we set it such
    that an integer value will be treated as an ``Integer``, and any other
    value is assumed to be a date and will be treated as a ``MyEpochType``::

        def coerce_compared_value(self, op, value):
            if isinstance(value, int):
                return Integer()
            else:
                return self

    """

    __visit_name__ = "type_decorator"

    def __init__(self, *args, **kwargs):
        """Construct a :class:`.TypeDecorator`.

        Arguments sent here are passed to the constructor
        of the class assigned to the ``impl`` class level attribute,
        assuming the ``impl`` is a callable, and the resulting
        object is assigned to the ``self.impl`` instance attribute
        (thus overriding the class attribute of the same name).

        If the class level ``impl`` is not a callable (the unusual case),
        it will be assigned to the same instance attribute 'as-is',
        ignoring those arguments passed to the constructor.

        Subclasses can override this to customize the generation
        of ``self.impl`` entirely.

        """

        if not hasattr(self.__class__, 'impl'):
            raise AssertionError("TypeDecorator implementations "
                                 "require a class-level variable "
                                 "'impl' which refers to the class of "
                                 "type being decorated")
        self.impl = to_instance(self.__class__.impl, *args, **kwargs)

    coerce_to_is_types = (util.NoneType, )
    """Specify those Python types which should be coerced at the expression
    level to "IS <constant>" when compared using ``==`` (and same for
    ``IS NOT`` in conjunction with ``!=``.

    For most SQLAlchemy types, this includes ``NoneType``, as well as
    ``bool``.

    :class:`.TypeDecorator` modifies this list to only include ``NoneType``,
    as typedecorator implementations that deal with boolean types are common.

    Custom :class:`.TypeDecorator` classes can override this attribute to
    return an empty tuple, in which case no values will be coerced to
    constants.

    ..versionadded:: 0.8.2
        Added :attr:`.TypeDecorator.coerce_to_is_types` to allow for easier
        control of ``__eq__()`` ``__ne__()`` operations.

    """

    class Comparator(TypeEngine.Comparator):

        def operate(self, op, *other, **kwargs):
            kwargs['_python_is_types'] = self.expr.type.coerce_to_is_types
            return super(TypeDecorator.Comparator, self).operate(
                op, *other, **kwargs)

        def reverse_operate(self, op, other, **kwargs):
            kwargs['_python_is_types'] = self.expr.type.coerce_to_is_types
            return super(TypeDecorator.Comparator, self).reverse_operate(
                op, other, **kwargs)

    @property
    def comparator_factory(self):
        if TypeDecorator.Comparator in self.impl.comparator_factory.__mro__:
            return self.impl.comparator_factory
        else:
            return type("TDComparator",
                        (TypeDecorator.Comparator,
                         self.impl.comparator_factory),
                        {})

    def _gen_dialect_impl(self, dialect):
        """
        #todo
        """
        adapted = dialect.type_descriptor(self)
        if adapted is not self:
            return adapted

        # otherwise adapt the impl type, link
        # to a copy of this TypeDecorator and return
        # that.
        typedesc = self.load_dialect_impl(dialect).dialect_impl(dialect)
        tt = self.copy()
        if not isinstance(tt, self.__class__):
            raise AssertionError('Type object %s does not properly '
                                 'implement the copy() method, it must '
                                 'return an object of type %s' %
                                 (self, self.__class__))
        tt.impl = typedesc
        return tt

    @property
    def _type_affinity(self):
        """
        #todo
        """
        return self.impl._type_affinity

    def type_engine(self, dialect):
        """Return a dialect-specific :class:`.TypeEngine` instance
        for this :class:`.TypeDecorator`.

        In most cases this returns a dialect-adapted form of
        the :class:`.TypeEngine` type represented by ``self.impl``.
        Makes usage of :meth:`dialect_impl` but also traverses
        into wrapped :class:`.TypeDecorator` instances.
        Behavior can be customized here by overriding
        :meth:`load_dialect_impl`.

        """
        adapted = dialect.type_descriptor(self)
        if not isinstance(adapted, type(self)):
            return adapted
        elif isinstance(self.impl, TypeDecorator):
            return self.impl.type_engine(dialect)
        else:
            return self.load_dialect_impl(dialect)

    def load_dialect_impl(self, dialect):
        """Return a :class:`.TypeEngine` object corresponding to a dialect.

        This is an end-user override hook that can be used to provide
        differing types depending on the given dialect.  It is used
        by the :class:`.TypeDecorator` implementation of :meth:`type_engine`
        to help determine what type should ultimately be returned
        for a given :class:`.TypeDecorator`.

        By default returns ``self.impl``.

        """
        return self.impl

    def __getattr__(self, key):
        """Proxy all other undefined accessors to the underlying
        implementation."""
        return getattr(self.impl, key)

    def process_literal_param(self, value, dialect):
        """Receive a literal parameter value to be rendered inline within
        a statement.

        This method is used when the compiler renders a
        literal value without using binds, typically within DDL
        such as in the "server default" of a column or an expression
        within a CHECK constraint.

        The returned string will be rendered into the output string.

        .. versionadded:: 0.9.0

        """
        raise NotImplementedError()

    def process_bind_param(self, value, dialect):
        """Receive a bound parameter value to be converted.

        Subclasses override this method to return the
        value that should be passed along to the underlying
        :class:`.TypeEngine` object, and from there to the
        DBAPI ``execute()`` method.

        The operation could be anything desired to perform custom
        behavior, such as transforming or serializing data.
        This could also be used as a hook for validating logic.

        This operation should be designed with the reverse operation
        in mind, which would be the process_result_value method of
        this class.

        :param value: Data to operate upon, of any type expected by
         this method in the subclass.  Can be ``None``.
        :param dialect: the :class:`.Dialect` in use.

        """

        raise NotImplementedError()

    def process_result_value(self, value, dialect):
        """Receive a result-row column value to be converted.

        Subclasses should implement this method to operate on data
        fetched from the database.

        Subclasses override this method to return the
        value that should be passed back to the application,
        given a value that is already processed by
        the underlying :class:`.TypeEngine` object, originally
        from the DBAPI cursor method ``fetchone()`` or similar.

        The operation could be anything desired to perform custom
        behavior, such as transforming or serializing data.
        This could also be used as a hook for validating logic.

        :param value: Data to operate upon, of any type expected by
         this method in the subclass.  Can be ``None``.
        :param dialect: the :class:`.Dialect` in use.

        This operation should be designed to be reversible by
        the "process_bind_param" method of this class.

        """

        raise NotImplementedError()

    @util.memoized_property
    def _has_bind_processor(self):
        """memoized boolean, check if process_bind_param is implemented.

        Allows the base process_bind_param to raise
        NotImplementedError without needing to test an expensive
        exception throw.

        """

        return self.__class__.process_bind_param.__code__ \
            is not TypeDecorator.process_bind_param.__code__

    @util.memoized_property
    def _has_literal_processor(self):
        """memoized boolean, check if process_literal_param is implemented.


        """

        return self.__class__.process_literal_param.__code__ \
            is not TypeDecorator.process_literal_param.__code__

    def literal_processor(self, dialect):
        """Provide a literal processing function for the given
        :class:`.Dialect`.

        Subclasses here will typically override
        :meth:`.TypeDecorator.process_literal_param` instead of this method
        directly.

        By default, this method makes use of
        :meth:`.TypeDecorator.process_bind_param` if that method is
        implemented, where :meth:`.TypeDecorator.process_literal_param` is
        not.  The rationale here is that :class:`.TypeDecorator` typically
        deals with Python conversions of data that are above the layer of
        database presentation.  With the value converted by
        :meth:`.TypeDecorator.process_bind_param`, the underlying type will
        then handle whether it needs to be presented to the DBAPI as a bound
        parameter or to the database as an inline SQL value.

        .. versionadded:: 0.9.0

        """
        if self._has_literal_processor:
            process_param = self.process_literal_param
        elif self._has_bind_processor:
            # the bind processor should normally be OK
            # for TypeDecorator since it isn't doing DB-level
            # handling, the handling here won't be different for bound vs.
            # literals.
            process_param = self.process_bind_param
        else:
            process_param = None

        if process_param:
            impl_processor = self.impl.literal_processor(dialect)
            if impl_processor:
                def process(value):
                    return impl_processor(process_param(value, dialect))
            else:
                def process(value):
                    return process_param(value, dialect)

            return process
        else:
            return self.impl.literal_processor(dialect)

    def bind_processor(self, dialect):
        """Provide a bound value processing function for the
        given :class:`.Dialect`.

        This is the method that fulfills the :class:`.TypeEngine`
        contract for bound value conversion.   :class:`.TypeDecorator`
        will wrap a user-defined implementation of
        :meth:`process_bind_param` here.

        User-defined code can override this method directly,
        though its likely best to use :meth:`process_bind_param` so that
        the processing provided by ``self.impl`` is maintained.

        :param dialect: Dialect instance in use.

        This method is the reverse counterpart to the
        :meth:`result_processor` method of this class.

        """
        if self._has_bind_processor:
            process_param = self.process_bind_param
            impl_processor = self.impl.bind_processor(dialect)
            if impl_processor:
                def process(value):
                    return impl_processor(process_param(value, dialect))

            else:
                def process(value):
                    return process_param(value, dialect)

            return process
        else:
            return self.impl.bind_processor(dialect)

    @util.memoized_property
    def _has_result_processor(self):
        """memoized boolean, check if process_result_value is implemented.

        Allows the base process_result_value to raise
        NotImplementedError without needing to test an expensive
        exception throw.

        """
        return self.__class__.process_result_value.__code__ \
            is not TypeDecorator.process_result_value.__code__

    def result_processor(self, dialect, coltype):
        """Provide a result value processing function for the given
        :class:`.Dialect`.

        This is the method that fulfills the :class:`.TypeEngine`
        contract for result value conversion.   :class:`.TypeDecorator`
        will wrap a user-defined implementation of
        :meth:`process_result_value` here.

        User-defined code can override this method directly,
        though its likely best to use :meth:`process_result_value` so that
        the processing provided by ``self.impl`` is maintained.

        :param dialect: Dialect instance in use.
        :param coltype: An SQLAlchemy data type

        This method is the reverse counterpart to the
        :meth:`bind_processor` method of this class.

        """
        if self._has_result_processor:
            process_value = self.process_result_value
            impl_processor = self.impl.result_processor(dialect,
                                                        coltype)
            if impl_processor:
                def process(value):
                    return process_value(impl_processor(value), dialect)

            else:
                def process(value):
                    return process_value(value, dialect)

            return process
        else:
            return self.impl.result_processor(dialect, coltype)

    def coerce_compared_value(self, op, value):
        """Suggest a type for a 'coerced' Python value in an expression.

        By default, returns self.   This method is called by
        the expression system when an object using this type is
        on the left or right side of an expression against a plain Python
        object which does not yet have a SQLAlchemy type assigned::

            expr = table.c.somecolumn + 35

        Where above, if ``somecolumn`` uses this type, this method will
        be called with the value ``operator.add``
        and ``35``.  The return value is whatever SQLAlchemy type should
        be used for ``35`` for this particular operation.

        """
        return self

    def copy(self):
        """Produce a copy of this :class:`.TypeDecorator` instance.

        This is a shallow copy and is provided to fulfill part of
        the :class:`.TypeEngine` contract.  It usually does not
        need to be overridden unless the user-defined :class:`.TypeDecorator`
        has local state that should be deep-copied.

        """

        instance = self.__class__.__new__(self.__class__)
        instance.__dict__.update(self.__dict__)
        return instance

    def get_dbapi_type(self, dbapi):
        """Return the DBAPI type object represented by this
        :class:`.TypeDecorator`.

        By default this calls upon :meth:`.TypeEngine.get_dbapi_type` of the
        underlying "impl".
        """
        return self.impl.get_dbapi_type(dbapi)

    def compare_values(self, x, y):
        """Given two values, compare them for equality.

        By default this calls upon :meth:`.TypeEngine.compare_values`
        of the underlying "impl", which in turn usually
        uses the Python equals operator ``==``.

        This function is used by the ORM to compare
        an original-loaded value with an intercepted
        "changed" value, to determine if a net change
        has occurred.

        """
        return self.impl.compare_values(x, y)

    def __repr__(self):
        return util.generic_repr(self, to_inspect=self.impl)


class Variant(TypeDecorator):
    """A wrapping type that selects among a variety of
    implementations based on dialect in use.

    The :class:`.Variant` type is typically constructed
    using the :meth:`.TypeEngine.with_variant` method.

    .. versionadded:: 0.7.2

    .. seealso:: :meth:`.TypeEngine.with_variant` for an example of use.

    """

    def __init__(self, base, mapping):
        """Construct a new :class:`.Variant`.

        :param base: the base 'fallback' type
        :param mapping: dictionary of string dialect names to
          :class:`.TypeEngine` instances.

        """
        self.impl = base
        self.mapping = mapping

    def load_dialect_impl(self, dialect):
        if dialect.name in self.mapping:
            return self.mapping[dialect.name]
        else:
            return self.impl

    def with_variant(self, type_, dialect_name):
        """Return a new :class:`.Variant` which adds the given
        type + dialect name to the mapping, in addition to the
        mapping present in this :class:`.Variant`.

        :param type_: a :class:`.TypeEngine` that will be selected
         as a variant from the originating type, when a dialect
         of the given name is in use.
        :param dialect_name: base name of the dialect which uses
         this type. (i.e. ``'postgresql'``, ``'mysql'``, etc.)

        """

        if dialect_name in self.mapping:
            raise exc.ArgumentError(
                "Dialect '%s' is already present in "
                "the mapping for this Variant" % dialect_name)
        mapping = self.mapping.copy()
        mapping[dialect_name] = type_
        return Variant(self.impl, mapping)

    @property
    def comparator_factory(self):
        """express comparison behavior in terms of the base type"""
        return self.impl.comparator_factory


def _reconstitute_comparator(expression):
    return expression.comparator


def to_instance(typeobj, *arg, **kw):
    if typeobj is None:
        return NULLTYPE

    if util.callable(typeobj):
        return typeobj(*arg, **kw)
    else:
        return typeobj


def adapt_type(typeobj, colspecs):
    if isinstance(typeobj, type):
        typeobj = typeobj()
    for t in typeobj.__class__.__mro__[0:-1]:
        try:
            impltype = colspecs[t]
            break
        except KeyError:
            pass
    else:
        # couldn't adapt - so just return the type itself
        # (it may be a user-defined type)
        return typeobj
    # if we adapted the given generic type to a database-specific type,
    # but it turns out the originally given "generic" type
    # is actually a subclass of our resulting type, then we were already
    # given a more specific type than that required; so use that.
    if (issubclass(typeobj.__class__, impltype)):
        return typeobj
    return typeobj.adapt(impltype)
