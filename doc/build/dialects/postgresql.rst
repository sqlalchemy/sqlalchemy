.. _postgresql_toplevel:

PostgreSQL
==========

.. automodule:: sqlalchemy.dialects.postgresql.base

ARRAY Types
-----------

The PostgreSQL dialect supports arrays, both as multidimensional column types
as well as array literals:

* :class:`_postgresql.ARRAY` - ARRAY datatype

* :class:`_postgresql.array` - array literal

* :func:`_postgresql.array_agg` - ARRAY_AGG SQL function

* :class:`_postgresql.aggregate_order_by` - helper for PG's ORDER BY aggregate
  function syntax.

.. _postgresql_json_types:

JSON Types
----------

The PostgreSQL dialect supports both JSON and JSONB datatypes, including
psycopg2's native support and support for all of PostgreSQL's special
operators:

* :class:`_postgresql.JSON`

* :class:`_postgresql.JSONB`

* :class:`_postgresql.JSONPATH`

HSTORE Type
-----------

The PostgreSQL HSTORE type as well as hstore literals are supported:

* :class:`_postgresql.HSTORE` - HSTORE datatype

* :class:`_postgresql.hstore` - hstore literal

ENUM Types
----------

PostgreSQL has an independently creatable TYPE structure which is used
to implement an enumerated type.   This approach introduces significant
complexity on the SQLAlchemy side in terms of when this type should be
CREATED and DROPPED.   The type object is also an independently reflectable
entity.   The following sections should be consulted:

* :class:`_postgresql.ENUM` - DDL and typing support for ENUM.

* :meth:`.PGInspector.get_enums` - retrieve a listing of current ENUM types

* :meth:`.postgresql.ENUM.create` , :meth:`.postgresql.ENUM.drop` - individual
  CREATE and DROP commands for ENUM.

.. _postgresql_array_of_enum:

Using ENUM with ARRAY
^^^^^^^^^^^^^^^^^^^^^

The combination of ENUM and ARRAY is not directly supported by backend
DBAPIs at this time.   Prior to SQLAlchemy 1.3.17, a special workaround
was needed in order to allow this combination to work, described below.

.. versionchanged:: 1.3.17 The combination of ENUM and ARRAY is now directly
   handled by SQLAlchemy's implementation without any workarounds needed.

.. sourcecode:: python

    from sqlalchemy import TypeDecorator
    from sqlalchemy.dialects.postgresql import ARRAY


    class ArrayOfEnum(TypeDecorator):
        impl = ARRAY

        def bind_expression(self, bindvalue):
            return sa.cast(bindvalue, self)

        def result_processor(self, dialect, coltype):
            super_rp = super(ArrayOfEnum, self).result_processor(dialect, coltype)

            def handle_raw_string(value):
                inner = re.match(r"^{(.*)}$", value).group(1)
                return inner.split(",") if inner else []

            def process(value):
                if value is None:
                    return None
                return super_rp(handle_raw_string(value))

            return process

E.g.::

    Table(
        "mydata",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", ArrayOfEnum(ENUM("a", "b", "c", name="myenum"))),
    )

This type is not included as a built-in type as it would be incompatible
with a DBAPI that suddenly decides to support ARRAY of ENUM directly in
a new version.

.. _postgresql_array_of_json:

Using JSON/JSONB with ARRAY
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Similar to using ENUM, prior to SQLAlchemy 1.3.17, for an ARRAY of JSON/JSONB
we need to render the appropriate CAST.   Current psycopg2 drivers accommodate
the result set correctly without any special steps.

.. versionchanged:: 1.3.17 The combination of JSON/JSONB and ARRAY is now
   directly handled by SQLAlchemy's implementation without any workarounds
   needed.

.. sourcecode:: python

    class CastingArray(ARRAY):
        def bind_expression(self, bindvalue):
            return sa.cast(bindvalue, self)

E.g.::

    Table(
        "mydata",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("data", CastingArray(JSONB)),
    )

.. _postgresql_ranges:

Range and Multirange Types
--------------------------

PostgreSQL range and multirange types are supported for the
psycopg, pg8000 and asyncpg dialects; the psycopg2 dialect supports the
range types only.

.. versionadded:: 2.0.17 Added range and multirange support for the pg8000
   dialect.  pg8000 1.29.8 or greater is required.

Data values being passed to the database may be passed as string
values or by using the :class:`_postgresql.Range` data object.

.. versionadded:: 2.0  Added the backend-agnostic :class:`_postgresql.Range`
   object used to indicate ranges.  The ``psycopg2``-specific range classes
   are no longer exposed and are only used internally by that particular
   dialect.

E.g. an example of a fully typed model using the
:class:`_postgresql.TSRANGE` datatype::

    from datetime import datetime

    from sqlalchemy.dialects.postgresql import Range
    from sqlalchemy.dialects.postgresql import TSRANGE
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class RoomBooking(Base):
        __tablename__ = "room_booking"

        id: Mapped[int] = mapped_column(primary_key=True)
        room: Mapped[str]
        during: Mapped[Range[datetime]] = mapped_column(TSRANGE)

To represent data for the ``during`` column above, the :class:`_postgresql.Range`
type is a simple dataclass that will represent the bounds of the range.
Below illustrates an INSERT of a row into the above ``room_booking`` table::

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    engine = create_engine("postgresql+psycopg://scott:tiger@pg14/dbname")

    Base.metadata.create_all(engine)

    with Session(engine) as session:
        booking = RoomBooking(
            room="101", during=Range(datetime(2013, 3, 23), datetime(2013, 3, 25))
        )
        session.add(booking)
        session.commit()

Selecting from any range column will also return :class:`_postgresql.Range`
objects as indicated::

    from sqlalchemy import select

    with Session(engine) as session:
        for row in session.execute(select(RoomBooking.during)):
            print(row)

The available range datatypes are as follows:

* :class:`_postgresql.INT4RANGE`
* :class:`_postgresql.INT8RANGE`
* :class:`_postgresql.NUMRANGE`
* :class:`_postgresql.DATERANGE`
* :class:`_postgresql.TSRANGE`
* :class:`_postgresql.TSTZRANGE`

.. autoclass:: sqlalchemy.dialects.postgresql.Range
    :members:

Multiranges
^^^^^^^^^^^

Multiranges are supported by PostgreSQL 14 and above.  SQLAlchemy's
multirange datatypes deal in lists of :class:`_postgresql.Range` types.

Multiranges are supported on the psycopg, asyncpg, and pg8000 dialects
**only**.  The psycopg2 dialect, which is SQLAlchemy's default ``postgresql``
dialect, **does not** support multirange datatypes.

.. versionadded:: 2.0 Added support for MULTIRANGE datatypes.
   SQLAlchemy represents a multirange value as a list of
   :class:`_postgresql.Range` objects.

.. versionadded:: 2.0.17 Added multirange support for the pg8000 dialect.
   pg8000 1.29.8 or greater is required.

The example below illustrates use of the :class:`_postgresql.TSMULTIRANGE`
datatype::

    from datetime import datetime
    from typing import List

    from sqlalchemy.dialects.postgresql import Range
    from sqlalchemy.dialects.postgresql import TSMULTIRANGE
    from sqlalchemy.orm import DeclarativeBase
    from sqlalchemy.orm import Mapped
    from sqlalchemy.orm import mapped_column


    class Base(DeclarativeBase):
        pass


    class EventCalendar(Base):
        __tablename__ = "event_calendar"

        id: Mapped[int] = mapped_column(primary_key=True)
        event_name: Mapped[str]
        in_session_periods: Mapped[List[Range[datetime]]] = mapped_column(TSMULTIRANGE)

Illustrating insertion and selecting of a record::

    from sqlalchemy import create_engine
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    engine = create_engine("postgresql+psycopg://scott:tiger@pg14/test")

    Base.metadata.create_all(engine)

    with Session(engine) as session:
        calendar = EventCalendar(
            event_name="SQLAlchemy Tutorial Sessions",
            in_session_periods=[
                Range(datetime(2013, 3, 23), datetime(2013, 3, 25)),
                Range(datetime(2013, 4, 12), datetime(2013, 4, 15)),
                Range(datetime(2013, 5, 9), datetime(2013, 5, 12)),
            ],
        )
        session.add(calendar)
        session.commit()

        for multirange in session.scalars(select(EventCalendar.in_session_periods)):
            for range_ in multirange:
                print(f"Start: {range_.lower}  End: {range_.upper}")

.. note:: In the above example, the list of :class:`_postgresql.Range` types
   as handled by the ORM will not automatically detect in-place changes to
   a particular list value; to update list values with the ORM, either re-assign
   a new list to the attribute, or use the :class:`.MutableList`
   type modifier.  See the section :ref:`mutable_toplevel` for background.


The available multirange datatypes are as follows:

* :class:`_postgresql.INT4MULTIRANGE`
* :class:`_postgresql.INT8MULTIRANGE`
* :class:`_postgresql.NUMMULTIRANGE`
* :class:`_postgresql.DATEMULTIRANGE`
* :class:`_postgresql.TSMULTIRANGE`
* :class:`_postgresql.TSTZMULTIRANGE`



PostgreSQL Data Types
---------------------

As with all SQLAlchemy dialects, all UPPERCASE types that are known to be
valid with PostgreSQL are importable from the top level dialect, whether
they originate from :mod:`sqlalchemy.types` or from the local dialect::

    from sqlalchemy.dialects.postgresql import (
        ARRAY,
        BIGINT,
        BIT,
        BOOLEAN,
        BYTEA,
        CHAR,
        CIDR,
        CITEXT,
        DATE,
        DOUBLE_PRECISION,
        ENUM,
        FLOAT,
        HSTORE,
        INET,
        INTEGER,
        INTERVAL,
        JSON,
        JSONB,
        MACADDR,
        MACADDR8,
        MONEY,
        NUMERIC,
        OID,
        REAL,
        SMALLINT,
        TEXT,
        TIME,
        TIMESTAMP,
        UUID,
        VARCHAR,
        INT4RANGE,
        INT8RANGE,
        NUMRANGE,
        DATERANGE,
        TSRANGE,
        TSTZRANGE,
        REGCONFIG,
        REGCLASS,
        TSQUERY,
        TSVECTOR,
    )

Types which are specific to PostgreSQL, or have PostgreSQL-specific
construction arguments, are as follows:

.. note: where :noindex: is used, indicates a type that is not redefined
   in the dialect module, just imported from sqltypes.  this avoids warnings
   in the sphinx build

.. currentmodule:: sqlalchemy.dialects.postgresql

.. autoclass:: sqlalchemy.dialects.postgresql.AbstractRange
    :members: comparator_factory

.. autoclass:: sqlalchemy.dialects.postgresql.AbstractMultiRange


.. autoclass:: ARRAY
    :members: __init__, Comparator


.. autoclass:: BIT

.. autoclass:: BYTEA
    :members: __init__

.. autoclass:: CIDR

.. autoclass:: CITEXT

.. autoclass:: DOMAIN
    :members: __init__, create, drop

.. autoclass:: DOUBLE_PRECISION
    :members: __init__
    :noindex:


.. autoclass:: ENUM
    :members: __init__, create, drop


.. autoclass:: HSTORE
    :members:


.. autoclass:: INET

.. autoclass:: INTERVAL
    :members: __init__

.. autoclass:: JSON
    :members:

.. autoclass:: JSONB
    :members:

.. autoclass:: JSONPATH

.. autoclass:: MACADDR

.. autoclass:: MACADDR8

.. autoclass:: MONEY

.. autoclass:: OID

.. autoclass:: REAL
    :members: __init__
    :noindex:


.. autoclass:: REGCONFIG

.. autoclass:: REGCLASS

.. autoclass:: TIMESTAMP
    :members: __init__

.. autoclass:: TIME
    :members: __init__

.. autoclass:: TSQUERY

.. autoclass:: TSVECTOR

.. autoclass:: UUID
    :members: __init__
    :noindex:


.. autoclass:: INT4RANGE


.. autoclass:: INT8RANGE


.. autoclass:: NUMRANGE


.. autoclass:: DATERANGE


.. autoclass:: TSRANGE


.. autoclass:: TSTZRANGE


.. autoclass:: INT4MULTIRANGE


.. autoclass:: INT8MULTIRANGE


.. autoclass:: NUMMULTIRANGE


.. autoclass:: DATEMULTIRANGE


.. autoclass:: TSMULTIRANGE


.. autoclass:: TSTZMULTIRANGE


PostgreSQL SQL Elements and Functions
--------------------------------------

.. autoclass:: aggregate_order_by

.. autoclass:: array

.. autofunction:: array_agg

.. autofunction:: Any

.. autofunction:: All

.. autoclass:: hstore
    :members:

.. autoclass:: to_tsvector

.. autoclass:: to_tsquery

.. autoclass:: plainto_tsquery

.. autoclass:: phraseto_tsquery

.. autoclass:: websearch_to_tsquery

.. autoclass:: ts_headline

PostgreSQL Constraint Types
---------------------------

SQLAlchemy supports PostgreSQL EXCLUDE constraints via the
:class:`ExcludeConstraint` class:

.. autoclass:: ExcludeConstraint
   :members: __init__

For example::

    from sqlalchemy.dialects.postgresql import ExcludeConstraint, TSRANGE


    class RoomBooking(Base):
        __tablename__ = "room_booking"

        room = Column(Integer(), primary_key=True)
        during = Column(TSRANGE())

        __table_args__ = (ExcludeConstraint(("room", "="), ("during", "&&")),)

PostgreSQL DML Constructs
-------------------------

.. autofunction:: sqlalchemy.dialects.postgresql.insert

.. autoclass:: sqlalchemy.dialects.postgresql.Insert
  :members:

.. _postgresql_psycopg2:

psycopg2
--------

.. automodule:: sqlalchemy.dialects.postgresql.psycopg2

.. _postgresql_psycopg:

psycopg
--------

.. automodule:: sqlalchemy.dialects.postgresql.psycopg

pg8000
------

.. automodule:: sqlalchemy.dialects.postgresql.pg8000

.. _dialect-postgresql-asyncpg:

asyncpg
-------

.. automodule:: sqlalchemy.dialects.postgresql.asyncpg

psycopg2cffi
------------

.. automodule:: sqlalchemy.dialects.postgresql.psycopg2cffi
