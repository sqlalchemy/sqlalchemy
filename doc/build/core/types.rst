.. _types_toplevel:

Column and Data Types
=====================

.. module:: sqlalchemy.types

SQLAlchemy provides abstractions for most common database data types,
and a mechanism for specifying your own custom data types.

The methods and attributes of type objects are rarely used directly.
Type objects are supplied to :class:`~sqlalchemy.Table` definitions
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
Functions that accept a type (such as :func:`~sqlalchemy.Column`) will
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
  :show-inheritance:
  :members:

.. autoclass:: Boolean
  :show-inheritance:
  :members:

.. autoclass:: Date
 :show-inheritance:
 :members:

.. autoclass:: DateTime
   :show-inheritance:
   :members:

.. autoclass:: Enum
  :show-inheritance:
  :members: __init__, create, drop

.. autoclass:: Float
  :show-inheritance:
  :members:

.. autoclass:: Integer
  :show-inheritance:
  :members:

.. autoclass:: Interval
 :show-inheritance:
 :members:

.. autoclass:: LargeBinary
 :show-inheritance:
 :members:

.. autoclass:: Numeric
  :show-inheritance:
  :members:

.. autoclass:: PickleType
 :show-inheritance:
 :members:

.. autoclass:: SchemaType
  :show-inheritance:
  :members:
  :undoc-members:

.. autoclass:: SmallInteger
 :show-inheritance:
 :members:

.. autoclass:: String
   :show-inheritance:
   :members:

.. autoclass:: Text
   :show-inheritance:
   :members:

.. autoclass:: Time
  :show-inheritance:
  :members:

.. autoclass:: Unicode
  :show-inheritance:
  :members:

.. autoclass:: UnicodeText
   :show-inheritance:
   :members:

.. _types_sqlstandard:

SQL Standard Types
------------------

The SQL standard types always create database column types of the same
name when ``CREATE TABLE`` is issued.  Some types may not be supported
on all databases.

.. autoclass:: BIGINT
  :show-inheritance:

.. autoclass:: BINARY
  :show-inheritance:

.. autoclass:: BLOB
  :show-inheritance:

.. autoclass:: BOOLEAN
  :show-inheritance:

.. autoclass:: CHAR
  :show-inheritance:

.. autoclass:: CLOB
  :show-inheritance:

.. autoclass:: DATE
  :show-inheritance:

.. autoclass:: DATETIME
  :show-inheritance:

.. autoclass:: DECIMAL
  :show-inheritance:

.. autoclass:: FLOAT
  :show-inheritance:

.. autoclass:: INT
  :show-inheritance:

.. autoclass:: sqlalchemy.types.INTEGER
  :show-inheritance:

.. autoclass:: NCHAR
  :show-inheritance:

.. autoclass:: NVARCHAR
  :show-inheritance:

.. autoclass:: NUMERIC
  :show-inheritance:

.. autoclass:: REAL
  :show-inheritance:

.. autoclass:: SMALLINT
  :show-inheritance:

.. autoclass:: TEXT
  :show-inheritance:

.. autoclass:: TIME
  :show-inheritance:

.. autoclass:: TIMESTAMP
  :show-inheritance:

.. autoclass:: VARBINARY
  :show-inheritance:

.. autoclass:: VARCHAR
  :show-inheritance:

.. _types_vendor:

Vendor-Specific Types
---------------------

Database-specific types are also available for import from each
database's dialect module. See the :ref:`sqlalchemy.dialects_toplevel`
reference for the database you're interested in.

For example, MySQL has a ``BIGINTEGER`` type and PostgreSQL has an
``INET`` type.  To use these, import them from the module explicitly::

    from sqlalchemy.dialects import mysql

    table = Table('foo', meta,
        Column('id', mysql.BIGINTEGER),
        Column('enumerates', mysql.ENUM('a', 'b', 'c'))
    )

Or some PostgreSQL types::

    from sqlalchemy.dialects import postgresql

    table = Table('foo', meta,
        Column('ipaddress', postgresql.INET),
        Column('elements', postgresql.ARRAY(str))
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

Augmenting Existing Types
~~~~~~~~~~~~~~~~~~~~~~~~~

The :class:`.TypeDecorator` allows the creation of custom types which
add bind-parameter and result-processing behavior to an existing
type object.  It is used when additional in-Python marshalling of data
to and from the database is required.

.. autoclass:: TypeDecorator
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

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
values are ever mutated once created.  For those which do have this requirment,
support for mutability is best applied using the ``sqlalchemy.ext.mutable``
extension - see the example in :ref:`mutable_toplevel`.

Creating New Types
~~~~~~~~~~~~~~~~~~

The :class:`.UserDefinedType` class is provided as a simple base class
for defining entirely new database types:

.. autoclass:: UserDefinedType
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. _types_api:

Base Type API
--------------

.. autoclass:: AbstractType
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: TypeEngine
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: MutableType
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: Concatenable
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: NullType
   :show-inheritance:

.. autoclass:: Variant
   :show-inheritance:
   :members: with_variant, __init__
