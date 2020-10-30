.. currentmodule:: sqlalchemy.types

.. _types_custom:

Custom Types
============

A variety of methods exist to redefine the behavior of existing types
as well as to provide new ones.

Overriding Type Compilation
---------------------------

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

The above code allows the usage of :class:`_types.BINARY`, which
will produce the string ``BINARY`` against all backends except SQLite,
in which case it will produce ``BLOB``.

See the section :ref:`type_compilation_extension`, a subsection of
:ref:`sqlalchemy.ext.compiler_toplevel`, for additional examples.

.. _types_typedecorator:

Augmenting Existing Types
-------------------------

The :class:`.TypeDecorator` allows the creation of custom types which
add bind-parameter and result-processing behavior to an existing
type object.  It is used when additional in-Python :term:`marshalling` of data
to and/or from the database is required.

.. note::

  The bind- and result-processing of :class:`.TypeDecorator`
  is **in addition** to the processing already performed by the hosted
  type, which is customized by SQLAlchemy on a per-DBAPI basis to perform
  processing specific to that DBAPI.  While it is possible to replace this
  handling for a given type through direct subclassing, it is never needed in
  practice and SQLAlchemy no longer supports this as a public use case.

.. topic:: ORM Tip

   The :class:`.TypeDecorator` can be used to provide a consistent means of
   converting some type of value as it is passed into and out of the database.
   When using the ORM, a similar technique exists for converting user data
   from arbitrary formats which is to use the :func:`.validates` decorator.
   This technique may be more appropriate when data coming into an ORM model
   needs to be normalized in some way that is specific to the business case
   and isn't as generic as a datatype.

.. autoclass:: TypeDecorator
   :members:


TypeDecorator Recipes
---------------------

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
            self.quantize_int = - self.impl.scale
            self.quantize = Decimal(10) ** self.quantize_int

        def process_bind_param(self, value, dialect):
            if isinstance(value, Decimal) and \
                value.as_tuple()[2] < self.quantize_int:
                value = value.quantize(self.quantize)
            return value

Store Timezone Aware Timestamps as Timezone Naive UTC
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Timestamps in databases should always be stored in a timezone-agnostic way. For
most databases, this means ensuring a timestamp is first in the UTC timezone
before it is stored, then storing it as timezone-naive (that is, without any
timezone associated with it; UTC is assumed to be the "implicit" timezone).
Alternatively,  database-specific types like PostgreSQLs "TIMESTAMP WITH
TIMEZONE" are often preferred for their richer functionality; however, storing
as plain UTC will work on all databases and drivers.   When a
timezone-intelligent database type is not an option or is not preferred,  the
:class:`.TypeDecorator` can be used to create a datatype that convert timezone
aware timestamps into timezone naive and back again.   Below, Python's
built-in ``datetime.timezone.utc`` timezone is used to normalize and
denormalize::

    import datetime

    class TZDateTime(TypeDecorator):
        impl = DateTime

        def process_bind_param(self, value, dialect):
            if value is not None:
                if not value.tzinfo:
                    raise TypeError("tzinfo is required")
                value = value.astimezone(datetime.timezone.utc).replace(
                    tzinfo=None
                )
            return value

        def process_result_value(self, value, dialect):
            if value is not None:
                value = value.replace(tzinfo=datetime.timezone.utc)
            return value


.. _custom_guid_type:

Backend-agnostic GUID Type
^^^^^^^^^^^^^^^^^^^^^^^^^^

Receives and returns Python uuid() objects.  Uses the PG UUID type
when using PostgreSQL, CHAR(32) on other backends, storing them
in stringified hex format.   Can be modified to store
binary in CHAR(16) if desired::

    from sqlalchemy.types import TypeDecorator, CHAR
    from sqlalchemy.dialects.postgresql import UUID
    import uuid

    class GUID(TypeDecorator):
        """Platform-independent GUID type.

        Uses PostgreSQL's UUID type, otherwise uses
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
                    return "%.32x" % uuid.UUID(value).int
                else:
                    # hexstring
                    return "%.32x" % value.int

        def process_result_value(self, value, dialect):
            if value is None:
                return value
            else:
                if not isinstance(value, uuid.UUID):
                    value = uuid.UUID(value)
                return value

Marshal JSON Strings
^^^^^^^^^^^^^^^^^^^^

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

Adding Mutability
~~~~~~~~~~~~~~~~~

The ORM by default will not detect "mutability" on such a type as above -
meaning, in-place changes to values will not be detected and will not be
flushed.   Without further steps, you instead would need to replace the existing
value with a new one on each parent object to detect changes::

    obj.json_value["key"] = "value"  # will *not* be detected by the ORM

    obj.json_value = {"key": "value"}  # *will* be detected by the ORM

The above limitation may be
fine, as many applications may not require that the values are ever mutated
once created.  For those which do have this requirement, support for mutability
is best applied using the ``sqlalchemy.ext.mutable`` extension.  For a
dictionary-oriented JSON structure, we can apply this as::

    json_type = MutableDict.as_mutable(JSONEncodedDict)

    class MyClass(Base):
        #  ...

        json_data = Column(json_type)


.. seealso::

    :ref:`mutable_toplevel`

Dealing with Comparison Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default behavior of :class:`.TypeDecorator` is to coerce the "right hand side"
of any expression into the same type.  For a type like JSON, this means that
any operator used must make sense in terms of JSON.    For some cases,
users may wish for the type to behave like JSON in some circumstances, and
as plain text in others.  One example is if one wanted to handle the
LIKE operator for the JSON type.  LIKE makes no sense against a JSON structure,
but it does make sense against the underlying textual representation.  To
get at this with a type like ``JSONEncodedDict``, we need to
**coerce** the column to a textual form using :func:`.cast` or
:func:`.type_coerce` before attempting to use this operator::

    from sqlalchemy import type_coerce, String

    stmt = select(my_table).where(
        type_coerce(my_table.c.json_data, String).like('%foo%'))

:class:`.TypeDecorator` provides a built-in system for working up type
translations like these based on operators.  If we wanted to frequently use the
LIKE operator with our JSON object interpreted as a string, we can build it
into the type by overriding the :meth:`.TypeDecorator.coerce_compared_value`
method::

    from sqlalchemy.sql import operators
    from sqlalchemy import String

    class JSONEncodedDict(TypeDecorator):

        impl = VARCHAR

        def coerce_compared_value(self, op, value):
            if op in (operators.like_op, operators.not_like_op):
                return String()
            else:
                return self

        def process_bind_param(self, value, dialect):
            if value is not None:
                value = json.dumps(value)

            return value

        def process_result_value(self, value, dialect):
            if value is not None:
                value = json.loads(value)
            return value

Above is just one approach to handling an operator like "LIKE".  Other
applications may wish to raise ``NotImplementedError`` for operators that
have no meaning with a JSON object such as "LIKE", rather than automatically
coercing to text.


.. _types_sql_value_processing:

Applying SQL-level Bind/Result Processing
-----------------------------------------

As seen in the section :ref:`types_typedecorator`,
SQLAlchemy allows Python functions to be invoked both when parameters are sent
to a statement, as well as when result rows are loaded from the database, to apply
transformations to the values as they are sent to or from the database.   It is also
possible to define SQL-level transformations as well.  The rationale here is when
only the relational database contains a particular series of functions that are necessary
to coerce incoming and outgoing data between an application and persistence format.
Examples include using database-defined encryption/decryption functions, as well
as stored procedures that handle geographic data.  The PostGIS extension to PostgreSQL
includes an extensive array of SQL functions that are necessary for coercing
data into particular formats.

Any :class:`.TypeEngine`, :class:`.UserDefinedType` or :class:`.TypeDecorator` subclass
can include implementations of
:meth:`.TypeEngine.bind_expression` and/or :meth:`.TypeEngine.column_expression`, which
when defined to return a non-``None`` value should return a :class:`_expression.ColumnElement`
expression to be injected into the SQL statement, either surrounding
bound parameters or a column expression.  For example, to build a ``Geometry``
type which will apply the PostGIS function ``ST_GeomFromText`` to all outgoing
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

We can apply the ``Geometry`` type into :class:`_schema.Table` metadata
and use it in a :func:`_expression.select` construct::

    geometry = Table('geometry', metadata,
                  Column('geom_id', Integer, primary_key=True),
                  Column('geom_data', Geometry)
                )

    print(select(geometry).where(
      geometry.c.geom_data == 'LINESTRING(189412 252431,189631 259122)'))

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
a :func:`_expression.select` against a :func:`.label` of our expression, the string
label is moved to the outside of the wrapped expression::

    print(select(geometry.c.geom_data.label('my_data')))

Output::

    SELECT ST_AsText(geometry.geom_data) AS my_data
    FROM geometry

Another example is we decorate
:class:`_postgresql.BYTEA` to provide a ``PGPString``, which will make use of the
PostgreSQL ``pgcrypto`` extension to encrypt/decrypt values
transparently::

    from sqlalchemy import create_engine, String, select, func, \
            MetaData, Table, Column, type_coerce, TypeDecorator

    from sqlalchemy.dialects.postgresql import BYTEA

    class PGPString(TypeDecorator):
        impl = BYTEA

        def __init__(self, passphrase):
            super(PGPString, self).__init__()
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
                        PGPString("this is my passphrase")),
                )

    engine = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    with engine.begin() as conn:
        metadata.create_all(conn)

        conn.execute(message.insert(), username="some user",
                                    message="this is my message")

        print(conn.scalar(
                select(message.c.message).\
                    where(message.c.username == "some user")
            ))

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


.. seealso::

   :ref:`examples_postgis`

.. _types_operators:

Redefining and Creating New Operators
-------------------------------------

SQLAlchemy Core defines a fixed set of expression operators available to all column expressions.
Some of these operations have the effect of overloading Python's built-in operators;
examples of such operators include
:meth:`.ColumnOperators.__eq__` (``table.c.somecolumn == 'foo'``),
:meth:`.ColumnOperators.__invert__` (``~table.c.flag``),
and :meth:`.ColumnOperators.__add__` (``table.c.x + table.c.y``).  Other operators are exposed as
explicit methods on column expressions, such as
:meth:`.ColumnOperators.in_` (``table.c.value.in_(['x', 'y'])``) and :meth:`.ColumnOperators.like`
(``table.c.value.like('%ed%')``).

The Core expression constructs in all cases consult the type of the expression in order to determine
the behavior of existing operators, as well as to locate additional operators that aren't part of
the built-in set.   The :class:`.TypeEngine` base class defines a root "comparison" implementation
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
    >>> print(sometable.c.data + 5)
    sometable.data goofy :data_1

The implementation for :meth:`.ColumnOperators.__add__` is consulted
by an owning SQL expression, by instantiating the :class:`.TypeEngine.Comparator` with
itself as the ``expr`` attribute.   The mechanics of the expression
system are such that operations continue recursively until an
expression object produces a new SQL expression construct. Above, we
could just as well have said ``self.expr.op("goofy")(other)`` instead
of ``self.op("goofy")(other)``.

When using :meth:`.Operators.op` for comparison operations that return a
boolean result, the :paramref:`.Operators.op.is_comparison` flag should be
set to ``True``::

    class MyInt(Integer):
        class comparator_factory(Integer.Comparator):
            def is_frobnozzled(self, other):
                return self.op("--is_frobnozzled->", is_comparison=True)(other)

New methods added to a :class:`.TypeEngine.Comparator` are exposed on an
owning SQL expression
using a ``__getattr__`` scheme, which exposes methods added to
:class:`.TypeEngine.Comparator` onto the owning :class:`_expression.ColumnElement`.
For example, to add a ``log()`` function
to integers::

    from sqlalchemy import Integer, func

    class MyInt(Integer):
        class comparator_factory(Integer.Comparator):
            def log(self, other):
                return func.log(self.expr, other)

Using the above type::

    >>> print(sometable.c.data.log(5))
    log(:log_1, :log_2)

Unary operations
are also possible.  For example, to add an implementation of the
PostgreSQL factorial operator, we combine the :class:`.UnaryExpression` construct
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
    >>> print(column('x', MyInteger).factorial())
    x !

.. seealso::

    :meth:`.Operators.op`

    :attr:`.TypeEngine.comparator_factory`



Creating New Types
------------------

The :class:`.UserDefinedType` class is provided as a simple base class
for defining entirely new database types.   Use this to represent native
database types not known by SQLAlchemy.   If only Python translation behavior
is needed, use :class:`.TypeDecorator` instead.

.. autoclass:: UserDefinedType
   :members:


.. _custom_and_decorated_types_reflection:

Working with Custom Types and Reflection
-----------------------------------------

It is important to note that database types which are modified to have
additional in-Python behaviors, including types based on
:class:`.TypeDecorator` as well as other user-defined subclasses of datatypes,
do not have any representation within a database schema.    When using database
the introspection features described at :ref:`metadata_reflection`, SQLAlchemy
makes use of a fixed mapping which links the datatype information reported by a
database server to a SQLAlchemy datatype object.   For example, if we look
inside of a PostgreSQL schema at the definition for a particular database
column, we might receive back the string ``"VARCHAR"``.  SQLAlchemy's
PostgreSQL dialect has a hardcoded mapping which links the string name
``"VARCHAR"`` to the SQLAlchemy :class:`.VARCHAR` class, and that's how when we
emit a statement like ``Table('my_table', m, autoload_with=engine)``, the
:class:`_schema.Column` object within it would have an instance of :class:`.VARCHAR`
present inside of it.

The implication of this is that if a :class:`_schema.Table` object makes use of type
objects that don't correspond directly to the database-native type name, if we
create a new :class:`_schema.Table` object against a new :class:`_schema.MetaData` collection
for this database table elsewhere using reflection, it will not have this
datatype. For example::

    >>> from sqlalchemy import Table, Column, MetaData, create_engine, PickleType, Integer
    >>> metadata = MetaData()
    >>> my_table = Table("my_table", metadata, Column('id', Integer), Column("data", PickleType))
    >>> engine = create_engine("sqlite://", echo='debug')
    >>> my_table.create(engine)
    INFO sqlalchemy.engine.base.Engine
    CREATE TABLE my_table (
        id INTEGER,
        data BLOB
    )

Above, we made use of :class:`.PickleType`, which is a :class:`.TypeDecorator`
that works on top of the :class:`.LargeBinary` datatype, which on SQLite
corresponds to the database type ``BLOB``.  In the CREATE TABLE, we see that
the ``BLOB`` datatype is used.   The SQLite database knows nothing about the
:class:`.PickleType` we've used.

If we look at the datatype of ``my_table.c.data.type``, as this is a Python
object that was created by us directly, it is :class:`.PickleType`::

    >>> my_table.c.data.type
    PickleType()

However, if we create another instance of :class:`_schema.Table` using reflection,
the use of :class:`.PickleType` is not represented in the SQLite database we've
created; we instead get back :class:`.BLOB`::

    >>> metadata_two = MetaData()
    >>> my_reflected_table = Table("my_table", metadata_two, autoload_with=engine)
    INFO sqlalchemy.engine.base.Engine PRAGMA main.table_info("my_table")
    INFO sqlalchemy.engine.base.Engine ()
    DEBUG sqlalchemy.engine.base.Engine Col ('cid', 'name', 'type', 'notnull', 'dflt_value', 'pk')
    DEBUG sqlalchemy.engine.base.Engine Row (0, 'id', 'INTEGER', 0, None, 0)
    DEBUG sqlalchemy.engine.base.Engine Row (1, 'data', 'BLOB', 0, None, 0)

    >>> my_reflected_table.c.data.type
    BLOB()

Typically, when an application defines explicit :class:`_schema.Table` metadata with
custom types, there is no need to use table reflection because the necessary
:class:`_schema.Table` metadata is already present.  However, for the case where an
application, or a combination of them, need to make use of both explicit
:class:`_schema.Table` metadata which includes custom, Python-level datatypes, as well
as :class:`_schema.Table` objects which set up their :class:`_schema.Column` objects as
reflected from the database, which nevertheless still need to exhibit the
additional Python behaviors of the custom datatypes, additional steps must be
taken to allow this.

The most straightforward is to override specific columns as described at
:ref:`reflection_overriding_columns`.  In this technique, we simply
use reflection in combination with explicit :class:`_schema.Column` objects for those
columns for which we want to use a custom or decorated datatype::

    >>> metadata_three = MetaData()
    >>> my_reflected_table = Table("my_table", metadata_three, Column("data", PickleType), autoload_with=engine)

The ``my_reflected_table`` object above is reflected, and will load the
definition of the "id" column from the SQLite database.  But for the "data"
column, we've overridden the reflected object with an explicit :class:`_schema.Column`
definition that includes our desired in-Python datatype, the
:class:`.PickleType`. The reflection process will leave this :class:`_schema.Column`
object intact::

    >>> my_reflected_table.c.data.type
    PickleType()

A more elaborate way to convert from database-native type objects to custom
datatypes is to use the :meth:`.DDLEvents.column_reflect` event handler.   If
for example we knew that we wanted all :class:`.BLOB` datatypes to in fact be
:class:`.PickleType`, we could set up a rule across the board::


    from sqlalchemy import BLOB
    from sqlalchemy import event
    from sqlalchemy import PickleType
    from sqlalchemy import Table

    @event.listens_for(Table, "column_reflect")
    def _setup_pickletype(inspector, table, column_info):
        if isinstance(column_info["type"], BLOB):
            column_info["type"] = PickleType()

When the above code is invoked *before* any table reflection occurs (note also
it should be invoked **only once** in the application, as it is a global rule),
upon reflecting any :class:`_schema.Table` that includes a column with a :class:`.BLOB`
datatype, the resulting datatype will be stored in the :class:`_schema.Column` object
as :class:`.PickleType`.

In practice, the above event-based approach would likely have additional rules
in order to affect only those columns where the datatype is important, such as
a lookup table of table names and possibly column names, or other heuristics
in order to accurately determine which columns should be established with an
in Python datatype.