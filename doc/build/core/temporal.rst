.. _metadata_temporal:

.. currentmodule:: sqlalchemy.schema

==========================================================
Temporal Constructions (System and Application versioning)
==========================================================

This section will discuss temporal features added in SQL:2011, which include DDL
constructs ``PERIOD FOR``, ``PERIOD FOR SYSTEM_TIME``, ``WITH SYSTEM
VERSIONING``, and ``WITHOUT OVERLAPS``. In SQLAlchemy these constructs can be
represented using :class:`_schema.Period` and :class:`.SystemTimePeriod`
objects.


Context
-------
The 2011 release of ISO SQL added support for temporal tables, i.e schemas where
rows are associated with one or more time periods. This page attemps to give
only a brief introduction to these new temporal features; for a more complete
explanation, refer to `TF`_. These temporal features are split into system
versioning and application versioning.

.. _TF: https://cs.ulb.ac.be/public/_media/teaching/infoh415/tempfeaturessql2011.pdf

System versioning describes row history logging that is handled automatically by
the backend, useful for producing auditable tables and storing history without
triggers. Historical rows are (usually) held in the same table, and are hidden
unless explicitly requested using ``FOR SYSTEM TIME`` in ``SELECT`` statements.

Application versioning describes row with a time period indicating its valid
time, manually managed by the application rather than the by the backend. This
is useful for situations where something might be valid for only a specific time
period, e.g. insurance policies or a period of employment. Tables with
application versioning are sometimes called application-time period tables.

System versioning is currently supported only by MariaDB, Microsoft SQL Server,
IBM Db2, and partially in Oracle (Oracle's implementation is not in accordance
with the ISO SQL specification). Application versioning is only supported by
MariaDB and Db2.

At the moment, SQLAlchemy supports all temporal DML (``CREATE TABLE``)
constructs for MariaDB and SQL Server; however, it does not support DML
constucts. These include  ``SELECT [...] FOR SYSTEM_TIME [...]`` for system
versioning and ``SELECT [...] WHERE my_period CONTAINS [...]`` or ``FOR PORTION
OF`` with application versioning - these actions can mostly be accomplished
using :meth:`_expression.Select.with_hint` for system versioning, and text
:meth:`_orm.Query.where`/ :meth:`_orm.Query.filter` clauses for application
versioning.


Working with Application-Time Periods
-------------------------------------
Application-time period schemas can be represented in SQLAlchemy using the
:class:`Period` schema item. All that is required in SQL DDL to add application
versioning to a table is to include a named ``PERIOD FOR`` construct. This is
described similarly in SQLAlchemy, using something like the following:

.. code-block:: python

    t = Table(
        "t",
        metadata,
        Column("id", Integer),
        Column("start_ts", TIMESTAMP),
        Column("end_ts", TIMESTAMP),
        Period("my_period", "start_ts", "end_ts"),
    )

The above will compile to:

.. code-block:: sql

    CREATE TABLE t (
        id INTEGER,
        start_ts TIMESTAMP,
        end_ts TIMESTAMP,
        PERIOD FOR my_period (start_ts, end_ts)
    )

Once created, a ``Period`` object is accessible via the tables's
``Table.periods`` collection. (e.g. ``t.periods.my_period``.)


Using Application-Time Periods in Primary Keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SQL:2011 allows for including periods in composite primary or unique keys using
the ``WITHOUT OVERLAPS`` clause. This can be acheived in SQLAlchemy either with
the ``primary_key`` argument to :class:`Period`, or with a
:class:`PrimaryKeyConstraint` or :class:`UniqueConstraint` table argument. The
below is an example:


.. code-block:: python

    from sqlalchemy import Period, UniqueConstraint

    t = Table(
        "t",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(30)),
        Column("tstart", DATE),
        Column("tend", DATE),
        Period("my_period", "tstart", "tend", primary_key=True),
        UniqueConstraint("name", "my_period")
    )

Generated DDL:

.. code-block:: sql

    CREATE TABLE t (
        id INTEGER,
        name VARCHAR(30),
        tstart DATE,
        tend DATE,
        PERIOD FOR my_period (tstart, tend),
        PRIMARY KEY (id, my_period WITHOUT OVERLAPS),
        UNIQUE (name, my_period WITHOUT OVERLAPS)
    )

Nothing special is needed to specify `WITHOUT OVERLAPS`, it is added
automatically if there is a period as a primary key.

Working with System-Time Periods
--------------------------------

System time periods are represented in DDL by adding ``WITH SYSTEM VERSIONING``
to a table definition and by creating a ``PERIOD FOR SYSTEM_TIME(...)``
construct. Some backends may allow omitting one of these in some cases.

This can be accomplished in SQLAlchemy using the :class:`SystemTimePeriod`
schema item, as below.

.. code-block:: python

    from sqlalchemy import SystemTimePeriod

    t = Table(
        "t",
        metadata,
        Column("id", Integer),
        Column("name", String(30)),
        Column("bigblob", BLOB, system_versioning=False),
        Column("sys_start", TIMESTAMP),
        Column("sys_end", TIMESTAMP),
        SystemTimePeriod("sys_start", "sys_end"),
    )

SQLAlchemy performs the following steps to create the DDL:

#. Look for a ``SystemTimePeriod`` object. If present, add ``WITH SYSTEM
   VERSIONING`` to the table's ``CREATE`` statement.
#. Locate the columns specified for the ``SystemTimePeriod()`` ``start`` and
   ``end`` arguments (first two positional arguments). For each of these,
   perform the following:

    * Check if the column has a :class:`Computed` object. If so, no further
      action is taken.
    * If not, add a :class:`Computed` object that will render as ``GENERATED
      ALWAYS AS ROW START`` or ``GENERATED ALWAYS AS ROW END``.
  
  If no column arguments are provided to ``SystemTimePeriod()``, ``WITH SYSTEM
  VERSIONING`` will be added to the table but no ``GENERATED`` columns will be
  specified. This is useful if the backend supports implicit system versioning
  columns (e.g. MariaDB).

#. Look for any columns with a ``system_versioning`` argument. This will add
   ``WITH SYSTEM VERSIONING`` or ``WITHOUT SYSTEM VERSIONING`` to that column.
   Not all backends support individual column opting in/out.

The resulting DDL is below:

.. code-block:: sql
    
    CREATE TABLE t (
        id INTEGER,
        name VARCHAR(30),
        bigblob BLOB WITHOUT SYSTEM VERSIONING,
        sys_start TIMESTAMP GENERATED ALWAYS AS ROW START,
        sys_end TIMESTAMP GENERATED ALWAYS AS ROW END,
        PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
    ) WITH SYSTEM VERSIONING

As mentioned above, some dialects may also support specifically opting a column
in or out of versioning, which is useful to help minimize storage requirements
with frequently-updating rows. This result can be acheived with the following
syntax:

.. code-block:: python

    t = Table(
        "t",
        metadata,
        Column("x", Integer),
        Column("y", Integer, system_versioning=True),
        Column("z", BLOB, system_versioning=False,
        SystemTimePeriod(),
    )

.. code-block:: sql

    CREATE TABLE t (
        x INTEGER,
        y INTEGER WITH SYSTEM VERSIONING,
        z BLOB WITHOUT SYSTEM VERSIONING
    ) WITH SYSTEM VERSIONING

Backend-Specific Constructs
----------------------------

Microsoft SQL Server
~~~~~~~~~~~~~~~~~~~~

Microsoft SQL server requires a slightly different system versioning syntax,
namely ``WITH (SYSTEM_VERSIONING = ON)`` or ``WITH (SYSTEM_VERSIONING = ON
(HISTORY_TABLE = schema.TableHistory))``. No action is needed for the first
construct, referred to as "anonymous" history table. The second construct Can be
created by passing a table name (with schema) or table object to the
``SystemTimePeriod``'s ``history_table`` constructor argument.

.. code-block:: python

    table_history = Table("TableHistory", ..., schema="dbo")

    # Option for passing a table object
    SystemTimePeriod("validfrom", "validto", history_table=table_history)

    # Alternative, option for specifying a table by name
    SystemTimePeriod("validfrom", "validto", history_table="dbo.TableHistory")

.. note::

    SQL Server's syntax explicitly requires specifying the table schema for the
    ``HISTORY_TABLE`` option for DDL creation. Ensure that it is specified when
    creating the history table object.

    If a string arument is passed to ``history_table``, *No checks will be done
    to verify the table exists*. This is intentional, as SQL Server will create
    the history table automatically if it does not exist. If strict checking is
    preferred, just pass the option ``_validate_str_tables=True``to the
    ``SystemTimePeriod`` constructor.

MariaDB
~~~~~~~

MariaDB allows for implicit system versioning columns

Other Backends
~~~~~~~~~~~~~~

If a backend needs an alternative system time period name instead of
``SYSTEM_TIME`` (e.g. Oracle), set it by subclassing :class:`SystemTimePeriod`
and overriding the _period_name = "SYSTEM_TIME" class attribute.

Bitemporal Tables
-----------------

Bitemporal tables are those that have both system and application versioning
enabled. This can be done easily by providing both a :class:`Period` and a
:class:`SystemTimePeriod`.

Temporal API
------------
.. autoclass:: Period
    :members:
    :inherited-members:

.. autoclass:: SystemTimePeriod
    :members:
    :inherited-members:
