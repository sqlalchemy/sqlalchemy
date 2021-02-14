=================================
Additional Persistence Techniques
=================================



.. _flush_embedded_sql_expressions:

Embedding SQL Insert/Update Expressions into a Flush
====================================================

This feature allows the value of a database column to be set to a SQL
expression instead of a literal value. It's especially useful for atomic
updates, calling stored procedures, etc. All you do is assign an expression to
an attribute::

    class SomeClass(Base):
        __tablename__ = "some_table"

        # ...

        value = Column(Integer)

    someobject = session.query(SomeClass).get(5)

    # set 'value' attribute to a SQL expression adding one
    someobject.value = SomeClass.value + 1

    # issues "UPDATE some_table SET value=value+1"
    session.commit()

This technique works both for INSERT and UPDATE statements. After the
flush/commit operation, the ``value`` attribute on ``someobject`` above is
expired, so that when next accessed the newly generated value will be loaded
from the database.

The feature also has conditional support to work in conjunction with
primary key columns.  A database that supports RETURNING, e.g. PostgreSQL,
Oracle, or SQL Server, or as a special case when using SQLite with the pysqlite
driver and a single auto-increment column, a SQL expression may be assigned
to a primary key column as well.  This allows both the SQL expression to
be evaluated, as well as allows any server side triggers that modify the
primary key value on INSERT, to be successfully retrieved by the ORM as
part of the object's primary key::


    class Foo(Base):
        __tablename__ = 'foo'
        pk = Column(Integer, primary_key=True)
        bar = Column(Integer)

    e = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    Base.metadata.create_all(e)

    session = Session(e)

    foo = Foo(pk=sql.select(sql.func.coalesce(sql.func.max(Foo.pk) + 1, 1))
    session.add(foo)
    session.commit()

On PostgreSQL, the above :class:`.Session` will emit the following INSERT:

.. sourcecode:: sql

    INSERT INTO foo (foopk, bar) VALUES
    ((SELECT coalesce(max(foo.foopk) + %(max_1)s, %(coalesce_2)s) AS coalesce_1
    FROM foo), %(bar)s) RETURNING foo.foopk

.. versionadded:: 1.3
    SQL expressions can now be passed to a primary key column during an ORM
    flush; if the database supports RETURNING, or if pysqlite is in use, the
    ORM will be able to retrieve the server-generated value as the value
    of the primary key attribute.

.. _session_sql_expressions:

Using SQL Expressions with Sessions
===================================

SQL expressions and strings can be executed via the
:class:`~sqlalchemy.orm.session.Session` within its transactional context.
This is most easily accomplished using the
:meth:`~.Session.execute` method, which returns a
:class:`~sqlalchemy.engine.CursorResult` in the same manner as an
:class:`~sqlalchemy.engine.Engine` or
:class:`~sqlalchemy.engine.Connection`::

    Session = sessionmaker(bind=engine)
    session = Session()

    # execute a string statement
    result = session.execute("select * from table where id=:id", {'id':7})

    # execute a SQL expression construct
    result = session.execute(select(mytable).where(mytable.c.id==7))

The current :class:`~sqlalchemy.engine.Connection` held by the
:class:`~sqlalchemy.orm.session.Session` is accessible using the
:meth:`~.Session.connection` method::

    connection = session.connection()

The examples above deal with a :class:`_orm.Session` that's
bound to a single :class:`_engine.Engine` or
:class:`_engine.Connection`. To execute statements using a
:class:`_orm.Session` which is bound either to multiple
engines, or none at all (i.e. relies upon bound metadata), both
:meth:`_orm.Session.execute` and
:meth:`_orm.Session.connection` accept a dictionary of bind arguments
:paramref:`_orm.Session.execute.bind_arguments` which may include "mapper"
which is passed a mapped class or
:class:`_orm.Mapper` instance, which is used to locate the
proper context for the desired engine::

    Session = sessionmaker()
    session = Session()

    # need to specify mapper or class when executing
    result = session.execute(
        text("select * from table where id=:id"),
        {'id':7},
        bind_arguments={'mapper': MyMappedClass}
    )

    result = session.execute(
        select(mytable).where(mytable.c.id==7),
        bind_arguments={'mapper': MyMappedClass}
    )

    connection = session.connection(MyMappedClass)

.. versionchanged:: 1.4 the ``mapper`` and ``clause`` arguments to
   :meth:`_orm.Session.execute` are now passed as part of a dictionary
   sent as the :paramref:`_orm.Session.execute.bind_arguments` parameter.
   The previous arguments are still accepted however this usage is
   deprecated.

.. _session_forcing_null:

Forcing NULL on a column with a default
=======================================

The ORM considers any attribute that was never set on an object as a
"default" case; the attribute will be omitted from the INSERT statement::

    class MyObject(Base):
        __tablename__ = 'my_table'
        id = Column(Integer, primary_key=True)
        data = Column(String(50), nullable=True)

    obj = MyObject(id=1)
    session.add(obj)
    session.commit()  # INSERT with the 'data' column omitted; the database
                      # itself will persist this as the NULL value

Omitting a column from the INSERT means that the column will
have the NULL value set, *unless* the column has a default set up,
in which case the default value will be persisted.   This holds true
both from a pure SQL perspective with server-side defaults, as well as the
behavior of SQLAlchemy's insert behavior with both client-side and server-side
defaults::

    class MyObject(Base):
        __tablename__ = 'my_table'
        id = Column(Integer, primary_key=True)
        data = Column(String(50), nullable=True, server_default="default")

    obj = MyObject(id=1)
    session.add(obj)
    session.commit()  # INSERT with the 'data' column omitted; the database
                      # itself will persist this as the value 'default'

However, in the ORM, even if one assigns the Python value ``None`` explicitly
to the object, this is treated the **same** as though the value were never
assigned::

    class MyObject(Base):
        __tablename__ = 'my_table'
        id = Column(Integer, primary_key=True)
        data = Column(String(50), nullable=True, server_default="default")

    obj = MyObject(id=1, data=None)
    session.add(obj)
    session.commit()  # INSERT with the 'data' column explicitly set to None;
                      # the ORM still omits it from the statement and the
                      # database will still persist this as the value 'default'

The above operation will persist into the ``data`` column the
server default value of ``"default"`` and not SQL NULL, even though ``None``
was passed; this is a long-standing behavior of the ORM that many applications
hold as an assumption.

So what if we want to actually put NULL into this column, even though the
column has a default value?  There are two approaches.  One is that
on a per-instance level, we assign the attribute using the
:obj:`_expression.null` SQL construct::

    from sqlalchemy import null

    obj = MyObject(id=1, data=null())
    session.add(obj)
    session.commit()  # INSERT with the 'data' column explicitly set as null();
                      # the ORM uses this directly, bypassing all client-
                      # and server-side defaults, and the database will
                      # persist this as the NULL value

The :obj:`_expression.null` SQL construct always translates into the SQL
NULL value being directly present in the target INSERT statement.

If we'd like to be able to use the Python value ``None`` and have this
also be persisted as NULL despite the presence of column defaults,
we can configure this for the ORM using a Core-level modifier
:meth:`.TypeEngine.evaluates_none`, which indicates
a type where the ORM should treat the value ``None`` the same as any other
value and pass it through, rather than omitting it as a "missing" value::

    class MyObject(Base):
        __tablename__ = 'my_table'
        id = Column(Integer, primary_key=True)
        data = Column(
          String(50).evaluates_none(),  # indicate that None should always be passed
          nullable=True, server_default="default")

    obj = MyObject(id=1, data=None)
    session.add(obj)
    session.commit()  # INSERT with the 'data' column explicitly set to None;
                      # the ORM uses this directly, bypassing all client-
                      # and server-side defaults, and the database will
                      # persist this as the NULL value

.. topic:: Evaluating None

  The :meth:`.TypeEngine.evaluates_none` modifier is primarily intended to
  signal a type where the Python value "None" is significant, the primary
  example being a JSON type which may want to persist the JSON ``null`` value
  rather than SQL NULL.  We are slightly repurposing it here in order to
  signal to the ORM that we'd like ``None`` to be passed into the type whenever
  present, even though no special type-level behaviors are assigned to it.

.. versionadded:: 1.1 added the :meth:`.TypeEngine.evaluates_none` method
   in order to indicate that a "None" value should be treated as significant.

.. _orm_server_defaults:

Fetching Server-Generated Defaults
===================================

As introduced in the sections :ref:`server_defaults` and :ref:`triggered_columns`,
the Core supports the notion of database columns for which the database
itself generates a value upon INSERT and in less common cases upon UPDATE
statements.  The ORM features support for such columns regarding being
able to fetch these newly generated values upon flush.   This behavior is
required in the case of primary key columns that are generated by the server,
since the ORM has to know the primary key of an object once it is persisted.

In the vast majority of cases, primary key columns that have their value
generated automatically by the database are  simple integer columns, which are
implemented by the database as either a so-called "autoincrement" column, or
from a sequence associated with the column.   Every database dialect within
SQLAlchemy Core supports a method of retrieving these primary key values which
is often native to the Python DBAPI, and in general this process is automatic,
with the exception of a database like Oracle that requires us to specify a
:class:`.Sequence` explicitly.   There is more documentation regarding this
at :paramref:`_schema.Column.autoincrement`.

For server-generating columns that are not primary key columns or that are not
simple autoincrementing integer columns, the ORM requires that these columns
are marked with an appropriate ``server_default`` directive that allows the ORM to
retrieve this value.   Not all methods are supported on all backends, however,
so care must be taken to use the appropriate method. The two questions to be
answered are, 1. is this column part of the primary key or not, and 2. does the
database support RETURNING or an equivalent, such as "OUTPUT inserted"; these
are SQL phrases which return a server-generated value at the same time as the
INSERT or UPDATE statement is invoked. Databases that support RETURNING or
equivalent include PostgreSQL, Oracle, and SQL Server.  Databases that do not
include SQLite and MySQL.

Case 1: non primary key, RETURNING or equivalent is supported
-------------------------------------------------------------

In this case, columns should be marked as :class:`.FetchedValue` or with an
explicit :paramref:`_schema.Column.server_default`.   The
:paramref:`.orm.mapper.eager_defaults` flag may be used to indicate that these
columns should be fetched immediately upon INSERT and sometimes UPDATE::


    class MyModel(Base):
        __tablename__ = 'my_table'

        id = Column(Integer, primary_key=True)
        timestamp = Column(DateTime(), server_default=func.now())

        # assume a database trigger populates a value into this column
        # during INSERT
        special_identifier = Column(String(50), server_default=FetchedValue())

        __mapper_args__ = {"eager_defaults": True}

Above, an INSERT statement that does not specify explicit values for
"timestamp" or "special_identifier" from the client side will include the
"timestamp" and "special_identifier" columns within the RETURNING clause so
they are available immediately. On the PostgreSQL database, an INSERT for the
above table will look like:

.. sourcecode:: sql

   INSERT INTO my_table DEFAULT VALUES RETURNING my_table.id, my_table.timestamp, my_table.special_identifier

Case 2: non primary key, RETURNING or equivalent is not supported or not needed
--------------------------------------------------------------------------------

This case is the same as case 1 above, except we don't specify
:paramref:`.orm.mapper.eager_defaults`::

    class MyModel(Base):
        __tablename__ = 'my_table'

        id = Column(Integer, primary_key=True)
        timestamp = Column(DateTime(), server_default=func.now())

        # assume a database trigger populates a value into this column
        # during INSERT
        special_identifier = Column(String(50), server_default=FetchedValue())

After a record with the above mapping is INSERTed, the "timestamp" and
"special_identifier" columns will remain empty, and will be fetched via
a second SELECT statement when they are first accessed after the flush, e.g.
they are marked as "expired".

If the :paramref:`.orm.mapper.eager_defaults` is still used, and the backend
database does not support RETURNING or an equivalent, the ORM will emit this
SELECT statement immediately following the INSERT statement.   This is often
undesirable as it adds additional SELECT statements to the flush process that
may not be needed.  Using the above mapping with the
:paramref:`.orm.mapper.eager_defaults` flag set to True against MySQL results
in SQL like this upon flush (minus the comment, which is for clarification only):

.. sourcecode:: sql

    INSERT INTO my_table () VALUES ()

    -- when eager_defaults **is** used, but RETURNING is not supported
    SELECT my_table.timestamp AS my_table_timestamp, my_table.special_identifier AS my_table_special_identifier
    FROM my_table WHERE my_table.id = %s

Case 3: primary key, RETURNING or equivalent is supported
----------------------------------------------------------

A primary key column with a server-generated value must be fetched immediately
upon INSERT; the ORM can only access rows for which it has a primary key value,
so if the primary key is generated by the server, the ORM needs a way for the
database to give us that new value immediately upon INSERT.

As mentioned above, for integer "autoincrement" columns as well as
PostgreSQL SERIAL, these types are handled automatically by the Core; databases
include functions for fetching the "last inserted id" where RETURNING
is not supported, and where RETURNING is supported SQLAlchemy will use that.

However, for non-integer values, as well as for integer values that must be
explicitly linked to a sequence or other triggered routine,  the server default
generation must be marked in the table metadata.

For an explicit sequence as we use with Oracle, this just means we are using
the :class:`.Sequence` construct::

    class MyOracleModel(Base):
        __tablename__ = 'my_table'

        id = Column(Integer, Sequence("my_sequence"), primary_key=True)
        data = Column(String(50))

The INSERT for a model as above on Oracle looks like:

.. sourcecode:: sql

    INSERT INTO my_table (id, data) VALUES (my_sequence.nextval, :data) RETURNING my_table.id INTO :ret_0

Where above, SQLAlchemy renders ``my_sequence.nextval`` for the primary key column
and also uses RETURNING to get the new value back immediately.

For datatypes that generate values automatically, or columns that are populated
by a trigger, we use :class:`.FetchedValue`.  Below is a model that uses a
SQL Server TIMESTAMP column as the primary key, which generates values automatically::

    class MyModel(Base):
        __tablename__ = 'my_table'

        timestamp = Column(TIMESTAMP(), server_default=FetchedValue(), primary_key=True)

An INSERT for the above table on SQL Server looks like:

.. sourcecode:: sql

    INSERT INTO my_table OUTPUT inserted.timestamp DEFAULT VALUES

Case 4: primary key, RETURNING or equivalent is not supported
--------------------------------------------------------------

In this area we are generating rows for a database such as SQLite or MySQL
where some means of generating a default is occurring on the server, but is
outside of the database's usual autoincrement routine. In this case, we have to
make sure SQLAlchemy can "pre-execute" the default, which means it has to be an
explicit SQL expression.

.. note::  This section will illustrate multiple recipes involving
   datetime values for MySQL and SQLite, since the datetime datatypes on these
   two  backends have additional idiosyncratic requirements that are useful to
   illustrate.  Keep in mind however that SQLite and MySQL require an explicit
   "pre-executed" default generator for *any* auto-generated datatype used as
   the primary key other than the usual single-column autoincrementing integer
   value.

MySQL with DateTime primary key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using the example of a :class:`.DateTime` column for MySQL, we add an explicit
pre-execute-supported default using the "NOW()" SQL function::

    class MyModel(Base):
        __tablename__ = 'my_table'

        timestamp = Column(DateTime(), default=func.now(), primary_key=True)

Where above, we select the "NOW()" function to deliver a datetime value
to the column.  The SQL generated by the above is:

.. sourcecode:: sql

    SELECT now() AS anon_1
    INSERT INTO my_table (timestamp) VALUES (%s)
    ('2018-08-09 13:08:46',)

MySQL with TIMESTAMP primary key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using the :class:`_types.TIMESTAMP` datatype with MySQL, MySQL ordinarily
associates a server-side default with this datatype automatically.  However
when we use one as a primary key, the Core cannot retrieve the newly generated
value unless we execute the function ourselves.  As :class:`_types.TIMESTAMP` on
MySQL actually stores a binary value, we need to add an additional "CAST" to our
usage of "NOW()" so that we retrieve a binary value that can be persisted
into the column::

    from sqlalchemy import cast, Binary

    class MyModel(Base):
        __tablename__ = 'my_table'

        timestamp = Column(
            TIMESTAMP(),
            default=cast(func.now(), Binary),
            primary_key=True)

Above, in addition to selecting the "NOW()" function, we additionally make
use of the :class:`.Binary` datatype in conjunction with :func:`.cast` so that
the returned value is binary.  SQL rendered from the above within an
INSERT looks like:

.. sourcecode:: sql

    SELECT CAST(now() AS BINARY) AS anon_1
    INSERT INTO my_table (timestamp) VALUES (%s)
    (b'2018-08-09 13:08:46',)

SQLite with DateTime primary key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For SQLite, new timestamps can be generated using the SQL function
``datetime('now', 'localtime')`` (or specify ``'utc'`` for UTC),
however making things more complicated is that this returns a string
value, which is then incompatible with SQLAlchemy's :class:`.DateTime`
datatype (even though the datatype converts the information back into a
string for the SQLite backend, it must be passed through as a Python datetime).
We therefore must also specify that we'd like to coerce the return value to
:class:`.DateTime` when it is returned from the function, which we achieve
by passing this as the ``type_`` parameter::

    class MyModel(Base):
        __tablename__ = 'my_table'

        timestamp = Column(
            DateTime,
            default=func.datetime('now', 'localtime', type_=DateTime),
            primary_key=True)

The above mapping upon INSERT will look like:

.. sourcecode:: sql

    SELECT datetime(?, ?) AS datetime_1
    ('now', 'localtime')
    INSERT INTO my_table (timestamp) VALUES (?)
    ('2018-10-02 13:37:33.000000',)


.. seealso::

    :ref:`metadata_defaults_toplevel`


.. _session_partitioning:

Partitioning Strategies (e.g. multiple database backends per Session)
=====================================================================

Simple Vertical Partitioning
----------------------------

Vertical partitioning places different classes, class hierarchies,
or mapped tables, across multiple databases, by configuring the
:class:`.Session` with the :paramref:`.Session.binds` argument. This
argument receives a dictionary that contains any combination of
ORM-mapped classes, arbitrary classes within a mapped hierarchy (such
as declarative base classes or mixins), :class:`_schema.Table` objects,
and :class:`_orm.Mapper` objects as keys, which then refer typically to
:class:`_engine.Engine` or less typically :class:`_engine.Connection` objects as targets.
The dictionary is consulted whenever the :class:`.Session` needs to
emit SQL on behalf of a particular kind of mapped class in order to locate
the appropriate source of database connectivity::

    engine1 = create_engine('postgresql://db1')
    engine2 = create_engine('postgresql://db2')

    Session = sessionmaker()

    # bind User operations to engine 1, Account operations to engine 2
    Session.configure(binds={User:engine1, Account:engine2})

    session = Session()

Above, SQL operations against either class will make usage of the :class:`_engine.Engine`
linked to that class.     The functionality is comprehensive across both
read and write operations; a :class:`_query.Query` that is against entities
mapped to ``engine1`` (determined by looking at the first entity in the
list of items requested) will make use of ``engine1`` to run the query.   A
flush operation will make use of **both** engines on a per-class basis as it
flushes objects of type ``User`` and ``Account``.

In the more common case, there are typically base or mixin classes that  can be
used to distinguish between operations that are destined for different database
connections.  The :paramref:`.Session.binds` argument can accommodate any
arbitrary Python class as a key, which will be used if it is found to be in the
``__mro__`` (Python method resolution order) for a particular  mapped class.
Supposing two declarative bases are representing two different database
connections::

    BaseA = declarative_base()

    BaseB = declarative_base()

    class User(BaseA):
        # ...

    class Address(BaseA):
        # ...


    class GameInfo(BaseB):
        # ...

    class GameStats(BaseB):
        # ...


    Session = sessionmaker()

    # all User/Address operations will be on engine 1, all
    # Game operations will be on engine 2
    Session.configure(binds={BaseA:engine1, BaseB:engine2})

Above, classes which descend from ``BaseA`` and ``BaseB`` will have their
SQL operations routed to one of two engines based on which superclass
they descend from, if any.   In the case of a class that descends from more
than one "bound" superclass, the superclass that is highest in the target
class' hierarchy will be chosen to represent which engine should be used.

.. seealso::

    :paramref:`.Session.binds`


Coordination of Transactions for a multiple-engine Session
----------------------------------------------------------

One caveat to using multiple bound engines is in the case where a commit
operation may fail on one backend after the commit has succeeded on another.
This is an inconsistency problem that in relational databases is solved
using a "two phase transaction", which adds an additional "prepare" step
to the commit sequence that allows for multiple databases to agree to commit
before actually completing the transaction.

Due to limited support within DBAPIs,  SQLAlchemy has limited support for two-
phase transactions across backends.  Most typically, it is known to work well
with the PostgreSQL backend and to  a lesser extent with the MySQL backend.
However, the :class:`.Session` is fully capable of taking advantage of the two
phase transaction feature when the backend supports it, by setting the
:paramref:`.Session.use_twophase` flag within :class:`.sessionmaker` or
:class:`.Session`.  See :ref:`session_twophase` for an example.


.. _session_custom_partitioning:

Custom Vertical Partitioning
----------------------------

More comprehensive rule-based class-level partitioning can be built by
overriding the :meth:`.Session.get_bind` method.   Below we illustrate
a custom :class:`.Session` which delivers the following rules:

1. Flush operations, as well as bulk "update" and "delete" operations,
   are delivered to the engine named ``leader``.

2. Operations on objects that subclass ``MyOtherClass`` all
   occur on the ``other`` engine.

3. Read operations for all other classes occur on a random
   choice of the ``follower1`` or ``follower2`` database.

::

    engines = {
        'leader':create_engine("sqlite:///leader.db"),
        'other':create_engine("sqlite:///other.db"),
        'follower1':create_engine("sqlite:///follower1.db"),
        'follower2':create_engine("sqlite:///follower2.db"),
    }

    from sqlalchemy.sql import Update, Delete
    from sqlalchemy.orm import Session, sessionmaker
    import random

    class RoutingSession(Session):
        def get_bind(self, mapper=None, clause=None):
            if mapper and issubclass(mapper.class_, MyOtherClass):
                return engines['other']
            elif self._flushing or isinstance(clause, (Update, Delete)):
                return engines['leader']
            else:
                return engines[
                    random.choice(['follower1','follower2'])
                ]

The above :class:`.Session` class is plugged in using the ``class_``
argument to :class:`.sessionmaker`::

    Session = sessionmaker(class_=RoutingSession)

This approach can be combined with multiple :class:`_schema.MetaData` objects,
using an approach such as that of using the declarative ``__abstract__``
keyword, described at :ref:`declarative_abstract`.

.. seealso::

    `Django-style Database Routers in SQLAlchemy <http://techspot.zzzeek.org/2012/01/11/django-style-database-routers-in-sqlalchemy/>`_  - blog post on a more comprehensive example of :meth:`.Session.get_bind`

Horizontal Partitioning
-----------------------

Horizontal partitioning partitions the rows of a single table (or a set of
tables) across multiple databases.    The SQLAlchemy :class:`.Session`
contains support for this concept, however to use it fully requires that
:class:`.Session` and :class:`_query.Query` subclasses are used.  A basic version
of these subclasses are available in the :ref:`horizontal_sharding_toplevel`
ORM extension.   An example of use is at: :ref:`examples_sharding`.

.. _bulk_operations:

Bulk Operations
===============

.. deepalchemy:: Bulk operations are essentially lower-functionality versions
   of the Unit of Work's facilities for emitting INSERT and UPDATE statements
   on primary key targeted rows.   These routines were added to suit some
   cases where many rows being inserted or updated could be run into the
   database without as much of the usual unit of work overhead, in that
   most unit of work features are **disabled**.

   There is **usually no need to use these routines, and they are not easy
   to use as there are many missing behaviors that are usually expected when
   using ORM objects**; for efficient
   bulk inserts, it's better to use the Core :class:`_sql.Insert` construct
   directly.   Please read all caveats at :ref:`bulk_operations_caveats`.

.. note:: Bulk INSERT and UPDATE should not be confused with the
   more common feature known as :ref:`orm_expression_update_delete`.   This
   feature allows a single UPDATE or DELETE statement with arbitrary WHERE
   criteria to be emitted.

.. versionadded:: 1.0.0

Bulk INSERT/per-row UPDATE operations on the :class:`.Session` include
:meth:`.Session.bulk_save_objects`, :meth:`.Session.bulk_insert_mappings`, and
:meth:`.Session.bulk_update_mappings`. The purpose of these methods is to
directly expose internal elements of the unit of work system, such that
facilities for emitting INSERT and UPDATE statements given dictionaries or
object states can be utilized alone, bypassing the normal unit of work
mechanics of state, relationship and attribute management.   The advantages to
this approach is strictly one of reduced Python overhead:

* The flush() process, including the survey of all objects, their state,
  their cascade status, the status of all objects associated with them
  via :func:`_orm.relationship`, and the topological sort of all operations to
  be performed is completely bypassed.  This reduces a great amount of
  Python overhead.

* The objects as given have no defined relationship to the target
  :class:`.Session`, even when the operation is complete, meaning there's no
  overhead in attaching them or managing their state in terms of the identity
  map or session.

* The :meth:`.Session.bulk_insert_mappings` and :meth:`.Session.bulk_update_mappings`
  methods accept lists of plain Python dictionaries, not objects; this further
  reduces a large amount of overhead associated with instantiating mapped
  objects and assigning state to them, which normally is also subject to
  expensive tracking of history on a per-attribute basis.

* The set of objects passed to all bulk methods are processed
  in the order they are received.   In the case of
  :meth:`.Session.bulk_save_objects`, when objects of different types are passed,
  the INSERT and UPDATE statements are necessarily broken up into per-type
  groups.  In order to reduce the number of batch INSERT or UPDATE statements
  passed to the DBAPI, ensure that the incoming list of objects
  are grouped by type.

* The process of fetching primary keys after an INSERT also is disabled by
  default.   When performed correctly, INSERT statements can now more readily
  be batched by the unit of work process into ``executemany()`` blocks, which
  perform vastly better than individual statement invocations.

* UPDATE statements can similarly be tailored such that all attributes
  are subject to the SET clause unconditionally, again making it much more
  likely that ``executemany()`` blocks can be used.

The performance behavior of the bulk routines should be studied using the
:ref:`examples_performance` example suite.  This is a series of example
scripts which illustrate Python call-counts across a variety of scenarios,
including bulk insert and update scenarios.

.. seealso::

  :ref:`examples_performance` - includes detailed examples of bulk operations
  contrasted against traditional Core and ORM methods, including performance
  metrics.

Usage
-----

The methods each work in the context of the :class:`.Session` object's
transaction, like any other::

    s = Session()
    objects = [
        User(name="u1"),
        User(name="u2"),
        User(name="u3")
    ]
    s.bulk_save_objects(objects)

For :meth:`.Session.bulk_insert_mappings`, and :meth:`.Session.bulk_update_mappings`,
dictionaries are passed::

    s.bulk_insert_mappings(User,
      [dict(name="u1"), dict(name="u2"), dict(name="u3")]
    )

.. seealso::

    :meth:`.Session.bulk_save_objects`

    :meth:`.Session.bulk_insert_mappings`

    :meth:`.Session.bulk_update_mappings`


Comparison to Core Insert / Update Constructs
---------------------------------------------

The bulk methods offer performance that under particular circumstances
can be close to that of using the core :class:`_expression.Insert` and
:class:`_expression.Update` constructs in an "executemany" context (for a description
of "executemany", see :ref:`execute_multiple` in the Core tutorial).
In order to achieve this, the
:paramref:`.Session.bulk_insert_mappings.return_defaults`
flag should be disabled so that rows can be batched together.   The example
suite in :ref:`examples_performance` should be carefully studied in order
to gain familiarity with how fast bulk performance can be achieved.

.. _bulk_operations_caveats:

ORM Compatibility / Caveats
----------------------------

.. warning::  Be sure to familiarize with these limitations before using the
   bulk routines.

The bulk insert / update methods lose a significant amount of functionality
versus traditional ORM use.   The following is a listing of features that
are **not available** when using these methods:

* persistence along :func:`_orm.relationship` linkages

* sorting of rows within order of dependency; rows are inserted or updated
  directly in the order in which they are passed to the methods

* Session-management on the given objects, including attachment to the
  session, identity map management.

* Functionality related to primary key mutation, ON UPDATE cascade -
  **mutation of primary key columns will not work** - as the original PK
  value of each row is not available, so the WHERE criteria cannot be
  generated.

* SQL expression inserts / updates (e.g. :ref:`flush_embedded_sql_expressions`) -
  having to evaluate these would prevent INSERT and UPDATE statements from
  being batched together in a straightforward way for a single executemany()
  call as they alter the SQL compilation of the statement itself.

* ORM events such as :meth:`.MapperEvents.before_insert`, etc.  The bulk
  session methods have no event support.

Features that **are available** include:

* INSERTs and UPDATEs of mapped objects

* Version identifier support

* Multi-table mappings, such as joined-inheritance - however, an object
  to be inserted across multiple tables either needs to have primary key
  identifiers fully populated ahead of time, else the
  :paramref:`.Session.bulk_save_objects.return_defaults` flag must be used,
  which will greatly reduce the performance benefits


