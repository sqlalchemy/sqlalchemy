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

.. autoclass:: MatchType
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

