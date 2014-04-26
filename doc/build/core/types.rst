.. _types_toplevel:

Column and Data Types
=====================

.. module:: sqlalchemy.types

SQLAlchemy provides abstractions for most common database data types,
and a mechanism for specifying your own custom data types.

The methods and attributes of type objects are rarely used directly.
Type objects are supplied to :class:`~sqlalchemy.schema.Table` definitions
and can be supplied as type hints to `functions` for occasions where
the database driver returns an incorrect type.

.. code-block:: pycon

  >>> users = Table('users', metadata,
  ...               Column('id', Integer, primary_key=True)
  ...               Column('login', String(32))
  ...              )


SQLAlchemy will use the ``Integer`` and ``String(32)`` type
information when issuing a ``CREATE TABLE`` statement and will use it
again when reading back rows ``SELECTed`` from the database.
Functions that accept a type (such as :func:`~sqlalchemy.schema.Column`) will
typically accept a type class or instance; ``Integer`` is equivalent
to ``Integer()`` with no construction arguments in this case.

.. _types_generic:

Generic Types
-------------

Generic types specify a column that can read, write and store a
particular type of Python data.  SQLAlchemy will choose the best
database column type available on the target database when issuing a
``CREATE TABLE`` statement.  For complete control over which column
type is emitted in ``CREATE TABLE``, such as ``VARCHAR`` see `SQL
Standard Types`_ and the other sections of this chapter.

.. autoclass:: BigInteger
   :members:

.. autoclass:: Boolean
   :members:

.. autoclass:: Date
   :members:

.. autoclass:: DateTime
   :members:

.. autoclass:: Enum
  :members: __init__, create, drop

.. autoclass:: Float
  :members:

.. autoclass:: Integer
  :members:

.. autoclass:: Interval
  :members:

.. autoclass:: LargeBinary
  :members:

.. autoclass:: Numeric
  :members:

.. autoclass:: PickleType
  :members:

.. autoclass:: SchemaType
  :members:
  :undoc-members:

.. autoclass:: SmallInteger
  :members:

.. autoclass:: String
   :members:

.. autoclass:: Text
   :members:

.. autoclass:: Time
  :members:

.. autoclass:: Unicode
  :members:

.. autoclass:: UnicodeText
   :members:

.. _types_sqlstandard:

SQL Standard Types
------------------

The SQL standard types always create database column types of the same
name when ``CREATE TABLE`` is issued.  Some types may not be supported
on all databases.

.. autoclass:: BIGINT


.. autoclass:: BINARY


.. autoclass:: BLOB


.. autoclass:: BOOLEAN


.. autoclass:: CHAR


.. autoclass:: CLOB


.. autoclass:: DATE


.. autoclass:: DATETIME


.. autoclass:: DECIMAL


.. autoclass:: FLOAT


.. autoclass:: INT


.. autoclass:: sqlalchemy.types.INTEGER


.. autoclass:: NCHAR


.. autoclass:: NVARCHAR


.. autoclass:: NUMERIC


.. autoclass:: REAL


.. autoclass:: SMALLINT


.. autoclass:: TEXT


.. autoclass:: TIME


.. autoclass:: TIMESTAMP


.. autoclass:: VARBINARY


.. autoclass:: VARCHAR


.. _types_vendor:

Vendor-Specific Types
---------------------

Database-specific types are also available for import from each
database's dialect module. See the :ref:`dialect_toplevel`
reference for the database you're interested in.

For example, MySQL has a ``BIGINT`` type and PostgreSQL has an
``INET`` type.  To use these, import them from the module explicitly::

    from sqlalchemy.dialects import mysql

    table = Table('foo', metadata,
        Column('id', mysql.BIGINT),
        Column('enumerates', mysql.ENUM('a', 'b', 'c'))
    )

Or some PostgreSQL types::

    from sqlalchemy.dialects import postgresql

    table = Table('foo', metadata,
        Column('ipaddress', postgresql.INET),
        Column('elements', postgresql.ARRAY(String))
    )

Each dialect provides the full set of typenames supported by
that backend within its `__all__` collection, so that a simple
`import *` or similar will import all supported types as
implemented for that backend::

    from sqlalchemy.dialects.postgresql import *

    t = Table('mytable', metadata,
               Column('id', INTEGER, primary_key=True),
               Column('name', VARCHAR(300)),
               Column('inetaddr', INET)
    )

Where above, the INTEGER and VARCHAR types are ultimately from
sqlalchemy.types, and INET is specific to the Postgresql dialect.

Some dialect level types have the same name as the SQL standard type,
but also provide additional arguments.  For example, MySQL implements
the full range of character and string types including additional arguments
such as `collation` and `charset`::

    from sqlalchemy.dialects.mysql import VARCHAR, TEXT

    table = Table('foo', meta,
        Column('col1', VARCHAR(200, collation='binary')),
        Column('col2', TEXT(charset='latin1'))
    )

.. _types_custom:

Custom Types
------------

A variety of methods exist to redefine the behavior of existing types
as well as to provide new ones.

Overriding Type Compilation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A frequent need is to force the "string" version of a type, that is
the one rendered in a CREATE TABLE statement or other SQL function
like CAST, to be changed.   For example, an application may want
to force the rendering of ``BINARY`` for all platforms
except for one, in which is wants ``BLOB`` to be rendered.  Usage
of an existing generic type, in this case :class:`.LargeBinary`, is
preferred for most use cases.  But to control
types more accurately, a compilation directive that is per-dialect
can be associated with any type::

    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.types import BINARY

    @compiles(BINARY, "sqlite")
    def compile_binary_sqlite(type_, compiler, **kw):
        return "BLOB"

The above code allows the usage of :class:`.types.BINARY`, which
will produce the string ``BINARY`` against all backends except SQLite,
in which case it will produce ``BLOB``.

See the section :ref:`type_compilation_extension`, a subsection of
:ref:`sqlalchemy.ext.compiler_toplevel`, for additional examples.

.. _types_typedecorator:

Augmenting Existing Types
~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.TypeDecorator` allows the creation of custom types which
add bind-parameter and result-processing behavior to an existing
type object.  It is used when additional in-Python marshaling of data
to and from the database is required.

.. note::

  The bind- and result-processing of :class:`.TypeDecorator`
  is *in addition* to the processing already performed by the hosted
  type, which is customized by SQLAlchemy on a per-DBAPI basis to perform
  processing specific to that DBAPI.  To change the DBAPI-level processing
  for an existing type, see the section :ref:`replacing_processors`.

.. autoclass:: TypeDecorator
   :members:
   :inherited-members:


TypeDecorator Recipes
~~~~~~~~~~~~~~~~~~~~~
A few key :class:`.TypeDecorator` recipes follow.

.. _coerce_to_unicode:

Coercing Encoded Strings to Unicode
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A common source of confusion regarding the :class:`.Unicode` type
is that it is intended to deal *only* with Python ``unicode`` objects
on the Python side, meaning values passed to it as bind parameters
must be of the form ``u'some string'`` if using Python 2 and not 3.
The encoding/decoding functions it performs are only to suit what the
DBAPI in use requires, and are primarily a private implementation detail.

The use case of a type that can safely receive Python bytestrings,
that is strings that contain non-ASCII characters and are not ``u''``
objects in Python 2, can be achieved using a :class:`.TypeDecorator`
which coerces as needed::

    from sqlalchemy.types import TypeDecorator, Unicode

    class CoerceUTF8(TypeDecorator):
        """Safely coerce Python bytestrings to Unicode
        before passing off to the database."""

        impl = Unicode

        def process_bind_param(self, value, dialect):
            if isinstance(value, str):
                value = value.decode('utf-8')
            return value

Rounding Numerics
^^^^^^^^^^^^^^^^^

Some database connectors like those of SQL Server choke if a Decimal is passed with too
many decimal places.   Here's a recipe that rounds them down::

    from sqlalchemy.types import TypeDecorator, Numeric
    from decimal import Decimal

    class SafeNumeric(TypeDecorator):
        """Adds quantization to Numeric."""

        impl = Numeric

        def __init__(self, *arg, **kw):
            TypeDecorator.__init__(self, *arg, **kw)
            self.quantize_int = -(self.impl.precision - self.impl.scale)
            self.quantize = Decimal(10) ** self.quantize_int

        def process_bind_param(self, value, dialect):
            if isinstance(value, Decimal) and \
                value.as_tuple()[2] < self.quantize_int:
                value = value.quantize(self.quantize)
            return value

.. _custom_guid_type:

Backend-agnostic GUID Type
^^^^^^^^^^^^^^^^^^^^^^^^^^

Receives and returns Python uuid() objects.  Uses the PG UUID type
when using Postgresql, CHAR(32) on other backends, storing them
in stringified hex format.   Can be modified to store
binary in CHAR(16) if desired::

    from sqlalchemy.types import TypeDecorator, CHAR
    from sqlalchemy.dialects.postgresql import UUID
    import uuid

    class GUID(TypeDecorator):
        """Platform-independent GUID type.

        Uses Postgresql's UUID type, otherwise uses
        CHAR(32), storing as stringified hex values.

        """
        impl = CHAR

        def load_dialect_impl(self, dialect):
            if dialect.name == 'postgresql':
                return dialect.type_descriptor(UUID())
            else:
                return dialect.type_descriptor(CHAR(32))

        def process_bind_param(self, value, dialect):
            if value is None:
                return value
            elif dialect.name == 'postgresql':
                return str(value)
            else:
                if not isinstance(value, uuid.UUID):
                    return "%.32x" % uuid.UUID(value)
                else:
                    # hexstring
                    return "%.32x" % value

        def process_result_value(self, value, dialect):
            if value is None:
                return value
            else:
                return uuid.UUID(value)

Marshal JSON Strings
^^^^^^^^^^^^^^^^^^^^^

This type uses ``simplejson`` to marshal Python data structures
to/from JSON.   Can be modified to use Python's builtin json encoder::

    from sqlalchemy.types import TypeDecorator, VARCHAR
    import json

    class JSONEncodedDict(TypeDecorator):
        """Represents an immutable structure as a json-encoded string.

        Usage::

            JSONEncodedDict(255)

        """

        impl = VARCHAR

        def process_bind_param(self, value, dialect):
            if value is not None:
                value = json.dumps(value)

            return value

        def process_result_value(self, value, dialect):
            if value is not None:
                value = json.loads(value)
            return value

Note that the ORM by default will not detect "mutability" on such a type -
meaning, in-place changes to values will not be detected and will not be
flushed. Without further steps, you instead would need to replace the existing
value with a new one on each parent object to detect changes. Note that
there's nothing wrong with this, as many applications may not require that the
values are ever mutated once created.  For those which do have this requirement,
support for mutability is best applied using the ``sqlalchemy.ext.mutable``
extension - see the example in :ref:`mutable_toplevel`.

.. _replacing_processors:

Replacing the Bind/Result Processing of Existing Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most augmentation of type behavior at the bind/result level
is achieved using :class:`.TypeDecorator`.   For the rare scenario
where the specific processing applied by SQLAlchemy at the DBAPI
level needs to be replaced, the SQLAlchemy type can be subclassed
directly, and the ``bind_processor()`` or ``result_processor()``
methods can be overridden.   Doing so requires that the
``adapt()`` method also be overridden.  This method is the mechanism
by which SQLAlchemy produces DBAPI-specific type behavior during
statement execution.  Overriding it allows a copy of the custom
type to be used in lieu of a DBAPI-specific type.  Below we subclass
the :class:`.types.TIME` type to have custom result processing behavior.
The ``process()`` function will receive ``value`` from the DBAPI
cursor directly::

  class MySpecialTime(TIME):
      def __init__(self, special_argument):
          super(MySpecialTime, self).__init__()
          self.special_argument = special_argument

      def result_processor(self, dialect, coltype):
          import datetime
          time = datetime.time
          def process(value):
              if value is not None:
                  microseconds = value.microseconds
                  seconds = value.seconds
                  minutes = seconds / 60
                  return time(
                            minutes / 60,
                            minutes % 60,
                            seconds - minutes * 60,
                            microseconds)
              else:
                  return None
          return process

      def adapt(self, impltype):
          return MySpecialTime(self.special_argument)

.. _types_sql_value_processing:

Applying SQL-level Bind/Result Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As seen in the sections :ref:`types_typedecorator` and :ref:`replacing_processors`,
SQLAlchemy allows Python functions to be invoked both when parameters are sent
to a statement, as well as when result rows are loaded from the database, to apply
transformations to the values as they are sent to or from the database.   It is also
possible to define SQL-level transformations as well.  The rationale here is when
only the relational database contains a particular series of functions that are necessary
to coerce incoming and outgoing data between an application and persistence format.
Examples include using database-defined encryption/decryption functions, as well
as stored procedures that handle geographic data.  The Postgis extension to Postgresql
includes an extensive array of SQL functions that are necessary for coercing
data into particular formats.

Any :class:`.TypeEngine`, :class:`.UserDefinedType` or :class:`.TypeDecorator` subclass
can include implementations of
:meth:`.TypeEngine.bind_expression` and/or :meth:`.TypeEngine.column_expression`, which
when defined to return a non-``None`` value should return a :class:`.ColumnElement`
expression to be injected into the SQL statement, either surrounding
bound parameters or a column expression.  For example, to build a ``Geometry``
type which will apply the Postgis function ``ST_GeomFromText`` to all outgoing
values and the function ``ST_AsText`` to all incoming data, we can create
our own subclass of :class:`.UserDefinedType` which provides these methods
in conjunction with :data:`~.sqlalchemy.sql.expression.func`::

    from sqlalchemy import func
    from sqlalchemy.types import UserDefinedType

    class Geometry(UserDefinedType):
        def get_col_spec(self):
            return "GEOMETRY"

        def bind_expression(self, bindvalue):
            return func.ST_GeomFromText(bindvalue, type_=self)

        def column_expression(self, col):
            return func.ST_AsText(col, type_=self)

We can apply the ``Geometry`` type into :class:`.Table` metadata
and use it in a :func:`.select` construct::

    geometry = Table('geometry', metadata,
                  Column('geom_id', Integer, primary_key=True),
                  Column('geom_data', Geometry)
                )

    print select([geometry]).where(
      geometry.c.geom_data == 'LINESTRING(189412 252431,189631 259122)')

The resulting SQL embeds both functions as appropriate.   ``ST_AsText``
is applied to the columns clause so that the return value is run through
the function before passing into a result set, and ``ST_GeomFromText``
is run on the bound parameter so that the passed-in value is converted::

    SELECT geometry.geom_id, ST_AsText(geometry.geom_data) AS geom_data_1
    FROM geometry
    WHERE geometry.geom_data = ST_GeomFromText(:geom_data_2)

The :meth:`.TypeEngine.column_expression` method interacts with the
mechanics of the compiler such that the SQL expression does not interfere
with the labeling of the wrapped expression.   Such as, if we rendered
a :func:`.select` against a :func:`.label` of our expression, the string
label is moved to the outside of the wrapped expression::

    print select([geometry.c.geom_data.label('my_data')])

Output::

    SELECT ST_AsText(geometry.geom_data) AS my_data
    FROM geometry

For an example of subclassing a built in type directly, we subclass
:class:`.postgresql.BYTEA` to provide a ``PGPString``, which will make use of the
Postgresql ``pgcrypto`` extension to encrpyt/decrypt values
transparently::

    from sqlalchemy import create_engine, String, select, func, \
            MetaData, Table, Column, type_coerce

    from sqlalchemy.dialects.postgresql import BYTEA

    class PGPString(BYTEA):
        def __init__(self, passphrase, length=None):
            super(PGPString, self).__init__(length)
            self.passphrase = passphrase

        def bind_expression(self, bindvalue):
            # convert the bind's type from PGPString to
            # String, so that it's passed to psycopg2 as is without
            # a dbapi.Binary wrapper
            bindvalue = type_coerce(bindvalue, String)
            return func.pgp_sym_encrypt(bindvalue, self.passphrase)

        def column_expression(self, col):
            return func.pgp_sym_decrypt(col, self.passphrase)

    metadata = MetaData()
    message = Table('message', metadata,
                    Column('username', String(50)),
                    Column('message',
                        PGPString("this is my passphrase", length=1000)),
                )

    engine = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    with engine.begin() as conn:
        metadata.create_all(conn)

        conn.execute(message.insert(), username="some user",
                                    message="this is my message")

        print conn.scalar(
                select([message.c.message]).\
                    where(message.c.username == "some user")
            )

The ``pgp_sym_encrypt`` and ``pgp_sym_decrypt`` functions are applied
to the INSERT and SELECT statements::

  INSERT INTO message (username, message)
    VALUES (%(username)s, pgp_sym_encrypt(%(message)s, %(pgp_sym_encrypt_1)s))
    {'username': 'some user', 'message': 'this is my message',
      'pgp_sym_encrypt_1': 'this is my passphrase'}

  SELECT pgp_sym_decrypt(message.message, %(pgp_sym_decrypt_1)s) AS message_1
    FROM message
    WHERE message.username = %(username_1)s
    {'pgp_sym_decrypt_1': 'this is my passphrase', 'username_1': 'some user'}


.. versionadded:: 0.8  Added the :meth:`.TypeEngine.bind_expression` and
   :meth:`.TypeEngine.column_expression` methods.

See also:

:ref:`examples_postgis`

.. _types_operators:

Redefining and Creating New Operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQLAlchemy Core defines a fixed set of expression operators available to all column expressions.
Some of these operations have the effect of overloading Python's built in operators;
examples of such operators include
:meth:`.ColumnOperators.__eq__` (``table.c.somecolumn == 'foo'``),
:meth:`.ColumnOperators.__invert__` (``~table.c.flag``),
and :meth:`.ColumnOperators.__add__` (``table.c.x + table.c.y``).  Other operators are exposed as
explicit methods on column expressions, such as
:meth:`.ColumnOperators.in_` (``table.c.value.in_(['x', 'y'])``) and :meth:`.ColumnOperators.like`
(``table.c.value.like('%ed%')``).

The Core expression constructs in all cases consult the type of the expression in order to determine
the behavior of existing operators, as well as to locate additional operators that aren't part of
the built in set.   The :class:`.TypeEngine` base class defines a root "comparison" implementation
:class:`.TypeEngine.Comparator`, and many specific types provide their own sub-implementations of this
class.   User-defined :class:`.TypeEngine.Comparator` implementations can be built directly into a
simple subclass of a particular type in order to override or define new operations.  Below,
we create a :class:`.Integer` subclass which overrides the :meth:`.ColumnOperators.__add__` operator::

    from sqlalchemy import Integer

    class MyInt(Integer):
        class comparator_factory(Integer.Comparator):
            def __add__(self, other):
                return self.op("goofy")(other)

The above configuration creates a new class ``MyInt``, which
establishes the :attr:`.TypeEngine.comparator_factory` attribute as
referring to a new class, subclassing the :class:`.TypeEngine.Comparator` class
associated with the :class:`.Integer` type.

Usage::

    >>> sometable = Table("sometable", metadata, Column("data", MyInt))
    >>> print sometable.c.data + 5
    sometable.data goofy :data_1

The implementation for :meth:`.ColumnOperators.__add__` is consulted
by an owning SQL expression, by instantiating the :class:`.TypeEngine.Comparator` with
itself as the ``expr`` attribute.   The mechanics of the expression
system are such that operations continue recursively until an
expression object produces a new SQL expression construct. Above, we
could just as well have said ``self.expr.op("goofy")(other)`` instead
of ``self.op("goofy")(other)``.

New methods added to a :class:`.TypeEngine.Comparator` are exposed on an
owning SQL expression
using a ``__getattr__`` scheme, which exposes methods added to
:class:`.TypeEngine.Comparator` onto the owning :class:`.ColumnElement`.
For example, to add a ``log()`` function
to integers::

    from sqlalchemy import Integer, func

    class MyInt(Integer):
        class comparator_factory(Integer.Comparator):
            def log(self, other):
                return func.log(self.expr, other)

Using the above type::

    >>> print sometable.c.data.log(5)
    log(:log_1, :log_2)


Unary operations
are also possible.  For example, to add an implementation of the
Postgresql factorial operator, we combine the :class:`.UnaryExpression` construct
along with a :class:`.custom_op` to produce the factorial expression::

    from sqlalchemy import Integer
    from sqlalchemy.sql.expression import UnaryExpression
    from sqlalchemy.sql import operators

    class MyInteger(Integer):
        class comparator_factory(Integer.Comparator):
            def factorial(self):
                return UnaryExpression(self.expr,
                            modifier=operators.custom_op("!"),
                            type_=MyInteger)

Using the above type::

    >>> from sqlalchemy.sql import column
    >>> print column('x', MyInteger).factorial()
    x !

See also:

:attr:`.TypeEngine.comparator_factory`

.. versionadded:: 0.8  The expression system was enhanced to support
  customization of operators on a per-type level.


Creating New Types
~~~~~~~~~~~~~~~~~~

The :class:`.UserDefinedType` class is provided as a simple base class
for defining entirely new database types.   Use this to represent native
database types not known by SQLAlchemy.   If only Python translation behavior
is needed, use :class:`.TypeDecorator` instead.

.. autoclass:: UserDefinedType
   :members:


.. _types_api:

Base Type API
--------------

.. autoclass:: TypeEngine
   :members:


.. autoclass:: Concatenable
   :members:
   :inherited-members:


.. autoclass:: NullType


.. autoclass:: Variant

   :members: with_variant, __init__
