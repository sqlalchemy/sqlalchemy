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

.. autoclass:: String
   :show-inheritance:

.. autoclass:: Unicode
   :show-inheritance:

.. autoclass:: Text
   :show-inheritance:

.. autoclass:: UnicodeText
   :show-inheritance:

.. autoclass:: Integer
   :show-inheritance:

.. autoclass:: SmallInteger
   :show-inheritance:

.. autoclass:: Numeric
   :show-inheritance:

.. autoclass:: Float
   :show-inheritance:

.. autoclass:: DateTime
   :show-inheritance:

.. autoclass:: Date
   :show-inheritance:

.. autoclass:: Time
   :show-inheritance:

.. autoclass:: Interval
   :show-inheritance:

.. autoclass:: Boolean
   :show-inheritance:

.. autoclass:: Binary
   :show-inheritance:

.. autoclass:: PickleType
   :show-inheritance:


SQL Standard Types
------------------

The SQL standard types always create database column types of the same
name when ``CREATE TABLE`` is issued.  Some types may not be supported
on all databases.

.. autoclass:: INT
   :show-inheritance:

.. autoclass:: sqlalchemy.types.INTEGER
   :show-inheritance:

.. autoclass:: CHAR
   :show-inheritance:

.. autoclass:: VARCHAR
   :show-inheritance:

.. autoclass:: NCHAR
   :show-inheritance:

.. autoclass:: TEXT
   :show-inheritance:

.. autoclass:: FLOAT
   :show-inheritance:

.. autoclass:: NUMERIC
   :show-inheritance:

.. autoclass:: DECIMAL
   :show-inheritance:

.. autoclass:: TIMESTAMP
   :show-inheritance:

.. autoclass:: DATETIME
   :show-inheritance:

.. autoclass:: CLOB
   :show-inheritance:

.. autoclass:: BLOB
   :show-inheritance:

.. autoclass:: BOOLEAN
   :show-inheritance:

.. autoclass:: SMALLINT
   :show-inheritance:

.. autoclass:: DATE
   :show-inheritance:

.. autoclass:: TIME
   :show-inheritance:


Vendor-Specific Types
---------------------

Database-specific types are also available for import from each
database's dialect module. See the :ref:`sqlalchemy.databases`
reference for the database you're interested in.

For example, MySQL has a ``BIGINTEGER`` type and PostgreSQL has an
``INET`` type.  To use these, import them from the module explicitly::

    from sqlalchemy.databases.mysql import MSBigInteger, MSEnum

    table = Table('foo', meta,
        Column('id', MSBigInteger),
        Column('enumerates', MSEnum('a', 'b', 'c'))
    )

Or some PostgreSQL types::

    from sqlalchemy.databases.postgres import PGInet, PGArray

    table = Table('foo', meta,
        Column('ipaddress', PGInet),
        Column('elements', PGArray(str))
        )


Custom Types
------------

User-defined types may be created to match special capabilities of a
particular database or simply for implementing custom processing logic
in Python.

The simplest method is implementing a :class:`TypeDecorator`, a helper
class that makes it easy to augment the bind parameter and result
processing capabilities of one of the built in types.

To build a type object from scratch, subclass `:class:TypeEngine`.

.. autoclass:: TypeDecorator
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

