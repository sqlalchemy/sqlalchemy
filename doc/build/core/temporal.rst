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
The 2011 release of the ISO SQL standard added support for temporal tables,
i.e., schemas where rows are associated with one or more time periods. This page
attempts to give only a brief introduction to these new features; for a more
complete explanation, refer to `Temporal Features in SQL:2011
<temporal_features_>`_ [#tempfeatures]_. Temporal features are split into two
classes: system versioning and application versioning.

System versioning describes row history logging that is handled automatically by
the backend, useful for producing auditable tables and storing history without
triggers. Historical rows are (usually) held in the same table, but are hidden
to all DML operations unless explicitly requested using ``FOR SYSTEM TIME`` in
``SELECT`` statements. System versioning is transparent; no user interaction is
needed.

Application versioning describes rows containing a time period that is managed
manually by the application, rather than by the backend. Typically, this is used
to represent the time of validity for the row, e.g., an identity document's
valid date range, or a period of employment. These application-time periods can
be added to primary and unique constraints in order to enforce integrity that
would be expected in such situations. Tables with application versioning are
sometimes referred to as application-time period tables.

System versioning is currently supported only by MariaDB, Microsoft SQL Server,
IBM Db2, and partially by Oracle (Oracle's implementation is not in accordance
with the ISO SQL specification). Application versioning is only supported by
MariaDB and Db2.

Currently, SQLAlchemy supports all temporal DML (``CREATE TABLE``) constructs
for MariaDB and SQL Server; however, it does not yet support DML constructs.
These include  ``SELECT [...] FOR SYSTEM_TIME [...]`` for system versioning and
``SELECT [...] WHERE my_period CONTAINS [...]`` or ``SELECT FOR PORTION OF``
with application versioning. While not natively supported, these actions can
mostly be accomplished using :meth:`_expression.Select.with_hint` for system
versioning, and text :meth:`_orm.Query.where`/ :meth:`_orm.Query.filter` clauses
for application versioning.



Working with Application-Time Periods
-------------------------------------
Application-time period schemas can be represented in SQLAlchemy using
:class:`Period`. All that is required in SQL DDL to add application versioning
to a table is to include a named ``PERIOD FOR`` construct. This is described
similarly in SQLAlchemy, using something like the following:

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

Usually application period columns are allowed to be ``DATE``, ``DATETIME``, or
``TIMESTAMP``, but be sure to verify what is accepted with your backend. Once
created, a ``Period`` object is accessible via the table's ``Table.periods``
collection. (e.g., ``t.periods.my_period``.)


Using Application-Time Periods in Constraints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Nothing special is needed to specify ``WITHOUT OVERLAPS``, it is added
automatically if there is a period in the constraint item list.



Working with System-Time Periods
--------------------------------

System time periods are represented in DDL by adding ``WITH SYSTEM VERSIONING``
to a table definition, and by creating a period named ``SYSTEM_TIME`` (using the
same construct as with application versioning, ``PERIOD FOR SYSTEM_TIME(...)``).
Some backends may allow enabling system versioning with only one of these two
statements, to simplify DDL.

Adding the above-mentioned constructs is represented in SQLAlchemy using the
:class:`SystemTimePeriod` schema item, as below.

.. code-block:: python

    from sqlalchemy import SystemTimePeriod

    t = Table(
        "t",
        metadata,
        Column("id", Integer),
        Column("name", String(30)),
        Column("sys_start", TIMESTAMP),
        Column("sys_end", TIMESTAMP),
        SystemTimePeriod("sys_start", "sys_end"),
    )

SQLAlchemy performs the following steps to create the DDL:

1. Look for a ``SystemTimePeriod`` object. If present, add ``WITH SYSTEM
   VERSIONING`` to the table's ``CREATE`` statement.
2. Locate the columns specified for the ``SystemTimePeriod()`` ``start`` and
   ``end`` arguments (first two positional arguments). For each of these,
   perform the following:

    * Check if the column has a :class:`Computed` object. If so, no further
      action is taken.
    * If not, add a :class:`Computed` object that will render as ``GENERATED
      ALWAYS AS ROW START`` or ``GENERATED ALWAYS AS ROW END``.
  
  If no column arguments are provided to ``SystemTimePeriod()``, ``WITH SYSTEM
  VERSIONING`` will be added to the table but no ``GENERATED`` columns will be
  specified. This is useful if the backend supports implicit system versioning
  columns (e.g., MariaDB).

3. Look for any columns with a ``system_versioning`` argument. This will add
   ``WITH SYSTEM VERSIONING`` or ``WITHOUT SYSTEM VERSIONING`` to that column.
   Not all backends support individual column opting in/out.

The resulting DDL is below:

.. code-block:: sql
    
    CREATE TABLE t (
        id INTEGER,
        name VARCHAR(30),
        sys_start TIMESTAMP GENERATED ALWAYS AS ROW START,
        sys_end TIMESTAMP GENERATED ALWAYS AS ROW END,
        PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
    ) WITH SYSTEM VERSIONING

As mentioned above, some dialects support specifically opting a column in or out
of versioning, which is useful to help minimize storage requirements with
frequently updating rows. This result can be achieved with the following syntax:

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
construct, referred to as an "anonymous" history table. The second construct can
be created by passing a table name (with schema) or table object to the
``SystemTimePeriod``'s ``history_table`` constructor argument.

.. code-block:: python

    table_history = Table("TableHistory", ..., schema="dbo")

    # Option for passing a table object
    SystemTimePeriod("validfrom", "validto", history_table=table_history)

    # Alternative, option for specifying a table by name
    SystemTimePeriod("validfrom", "validto", history_table="dbo.OtherTable")

.. note::

    SQL Server's syntax explicitly requires specifying the table schema for the
    ``HISTORY_TABLE`` option if given. Ensure that it is specified if passing a
    SQLAlchemy :class:`Table` object, as shown above.

    If a string argument is passed to ``history_table``, *no checks will be done
    to verify the table exists*. This is intentional, as SQL Server will create
    the history table automatically if it is not already present in the schema.
    If you would like SQLAlchemy to validate that the table is present in its
    metadata, just enable strict checking by passing
    ``_validate_str_tables=True`` to the ``SystemTimePeriod`` constructor.


For more information on MSSQL-specific syntax, refer to their `documentation on
system versioning <mssql_sv>`_.


MariaDB
~~~~~~~

MariaDB allows a simplified syntax for creating system versioned tables,
where ``ROW START`` and ``ROW END`` columns, and ``SYSTEM_TIME`` period do not
need to be specified. This is easily implemented:

.. code-block:: python

    t = Table("t", metadata, Column("x", Integer), SystemTimePeriod())

.. code-block:: sql

   CREATE TABLE t (
      x INTEGER
   ) WITH SYSTEM VERSIONING;

In this case, columns ``ROW_START`` and ``ROW_END`` will be implicitly added to
the table but be made invisible (but remain selectable). If this syntax is used
and it is desired that the columns be accessible via SQLAlchemy, create start
and end columns using ``system=True``.

.. code-block:: python

    t = Table(
        "t",
        metadata,
        Column("x", Integer),
        Column('row_start', TIMESTAMP, system=True),
        Column('row_end', TIMESTAMP, system=True),
        SystemTimePeriod(),
    )

The above will render exactly the same as the previous example's DDL (with no
start or end columns) but give SQLAlchemy knowledge of the implicit columns.

For more information on MariaDB-specific syntax, refer to their `documentation
on system versioning <mariadb_sv_>`_ or on `application versioning
<mariadb_av_>`_.


Other Backends
~~~~~~~~~~~~~~

If a backend not currently supported by SQLAlchemy needs an alternative system
time period name instead of ``SYSTEM_TIME`` (e.g., Oracle), set it by
subclassing :class:`SystemTimePeriod` and overriding the ``_period_name =
"SYSTEM_TIME"`` class attribute.


Bitemporal Tables
-----------------

Bitemporal tables are those that have both system and application versioning
enabled. This can be done easily by providing both a :class:`Period` and a
:class:`SystemTimePeriod`.

.. code-block:: python

    t = Table(
        "t",
        metadata,
        Column("id", Integer),
        Column("tstart", DATE),
        Column("tend", DATE),
        Column("sys_start", TIMESTAMP),
        Column("sys_end", TIMESTAMP),
        Period("my_period", "tstart", "tend"),
        SystemTimePeriod("sys_start", "sys_end"),
        PrimaryKeyConstraint("id", "my_period"),
    )

.. code-block:: sql

    CREATE TABLE emp (
        id INTEGER NOT NULL, 
        tstart DATE, 
        tend DATE, 
        sys_start TIMESTAMP GENERATED ALWAYS AS ROW START, 
        sys_end TIMESTAMP GENERATED ALWAYS AS ROW END, 
        PERIOD FOR my_period (tstart, tend), 
        PERIOD FOR SYSTEM_TIME (sys_start, sys_end), 
        PRIMARY KEY (id, my_period WITHOUT OVERLAPS)
    ) WITH SYSTEM VERSIONING"



Temporal API
------------
.. autoclass:: Period
    :members:
    :inherited-members:

.. autoclass:: SystemTimePeriod
    :members:
    :inherited-members:



References
----------

.. [#tempfeatures] K Kulkarni, and  J.E. Michels, "Temporal Features in SQL:2011,"
    *Sigmoid Record*, vol. 41, no. 3, September 2012. Available:
    `<https://sigmodrecord.org/publications/sigmodRecord/1209/pdfs/07.industry.
    kulkarni.pdf>`_


.. Links (not displayed)
.. _temporal_features: https://sigmodrecord.org/publications/sigmodRecord/1209
    /pdfs/07.industry.kulkarni.pdf
.. _mariadb_sv: https://mariadb.com/kb/en/system-versioned-tables/
.. _mariadb_av: https://mariadb.com/kb/en/application-time-periods/
.. _mssql_sv: https://docs.microsoft.com/en-us/sql/relational-databases/tables
    /temporal-tables?view=sql-server-ver15
