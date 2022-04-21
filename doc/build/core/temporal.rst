.. _metadata_temporal:

.. currentmodule:: sqlalchemy.schema

==========================================================
Temporal Constructions (System and Application versioning)
==========================================================

This section will discuss temporal features added in SQL:2011, which include DDL
constructs ``PERIOD FOR``, ``PERIOD FOR SYSTEM_TIME``, ``WITH SYSTEM
VERSIONING``, and ``WITHOUT OVERLAPS``. In SQLAlchemy these constructs can be
represented using :class:`_schema.Period`, :class:`ApplicationTimePeriod` and
:class:`.SystemTimePeriod` objects.


Context
-------
The 2011 release of ISO SQL added support for temporal tables, i.e schemas where
rows are associated with one or more time periods. This page attemps to give
only a brief introduction to these new temporal features; for a more complete
explanation, refer to `TF`_.

.. _TF: https://cs.ulb.ac.be/public/_media/teaching/infoh415/tempfeaturessql2011.pdf

These temporal features are split into system versioning and application
versioning. System versioning describes row history logging that is handled
automatically by the backend, useful for producing auditable tables and storing
history without triggers. Historical rows are (usually) held in the same table,
and are hidden unless explicitly requested using ``FOR SYSTEM TIME`` in
``SELECT`` statements.

Application versioning describes roww with a time period indicating its valid
time, manually managed by the application rather than the by the backend. This
is useful for situations where something might be valid for only a specific time
period, e.g. insurance policies or a period of employment.

System versioning is currently supported only by MariaDB and Microsoft SQL
Server. Application versioning is only supported by MariaDB.

At the moment, SQLAlchemy supports all temporal DML (``CREATE TABLE``)
constructs for all database backends that implement it; however, it does not
support DML constucts. These include  ``SELECT [...] FOR SYSTEM_TIME [...]`` for
system versioning and ``SELECT [...] WHERE my_period CONTAINS [...]`` with
application versioning - these actions can mostly be accomplished using
:meth:`_expression.Select.with_hint` for system versioning, and and text
:meth:`_orm.Query.where`/ :meth:`_orm.Query.filter` clauses for application
versioning.


Working with Application-Time Periods
-------------------------------------
Application-time period schemas can be represented in SQLAlchemy using the
:class:`Period` schema item, or its alias :class:`ApplicationTimePeriod`. All
that is required in SQL DDL to add application versioning to a table is to
include a named ``PERIOD FOR`` construct. This is described similarly in
SQLAlchemy, using something like the following:

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
the ``WITHOUT OVERLAPS`` clause. This can be acheived in SQLAlchemy in the same
way as usual: either with the ``primary_key`` argument to :class:`Column`, or
with a :class:`PrimaryKeyConstraint` or :class:`UniqueConstraint` table
argument. The below is an example:


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

Backend-Specific System Versioning Constructs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    preferred, just pass the option ``_validate_tables=True``to the
    ``SystemTimePeriod`` constructor.



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

.. autoclass:: ApplicationTimePeriod
    :members:
    :inherited-members:

.. autoclass:: SystemTimePeriod
    :members:
    :inherited-members:
