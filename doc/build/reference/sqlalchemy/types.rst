.. _types:

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
  :members:

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

Custom Types
------------

User-defined types may be created to match special capabilities of a
particular database or simply for implementing custom processing logic
in Python.

The simplest method is implementing a :class:`TypeDecorator`, a helper
class that makes it easy to augment the bind parameter and result
processing capabilities of one of the built in types.

To build a type object from scratch, subclass `:class:UserDefinedType`.

.. autoclass:: TypeDecorator
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: UserDefinedType
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: TypeEngine
   :members:
   :undoc-members:
   :inherited-members:
   :show-inheritance:

.. autoclass:: AbstractType
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

