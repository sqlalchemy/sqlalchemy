:orphan:

.. _faq_toplevel:

============================
Frequently Asked Questions
============================

.. contents::
    :local:
    :class: faq
    :backlinks: none


Connections / Engines
=====================

How do I configure logging?
---------------------------

See :ref:`dbengine_logging`.

How do I pool database connections?   Are my connections pooled?
----------------------------------------------------------------

SQLAlchemy performs application-level connection pooling automatically
in most cases.  With the exception of SQLite, a :class:`.Engine` object
refers to a :class:`.QueuePool` as a source of connectivity.

For more detail, see :ref:`engines_toplevel` and :ref:`pooling_toplevel`.

How do I pass custom connect arguments to my database API?
-----------------------------------------------------------

The :func:`.create_engine` call accepts additional arguments either
directly via the ``connect_args`` keyword argument::

    e = create_engine("mysql://scott:tiger@localhost/test",
                        connect_args={"encoding": "utf8"})

Or for basic string and integer arguments, they can usually be specified
in the query string of the URL::

    e = create_engine("mysql://scott:tiger@localhost/test?encoding=utf8")

.. seealso::

    :ref:`custom_dbapi_args`

"MySQL Server has gone away"
----------------------------

There are two major causes for this error:

1. The MySQL client closes connections which have been idle for a set period
of time, defaulting to eight hours.   This can be avoided by using the ``pool_recycle``
setting with :func:`.create_engine`, described at :ref:`mysql_connection_timeouts`.

2. Usage of the MySQLdb :term:`DBAPI`, or a similar DBAPI, in a non-threadsafe manner, or in an otherwise
inappropriate way.   The MySQLdb connection object is not threadsafe - this expands
out to any SQLAlchemy system that links to a single connection, which includes the ORM
:class:`.Session`.  For background
on how :class:`.Session` should be used in a multithreaded environment,
see :ref:`session_faq_threadsafe`.

Why does SQLAlchemy issue so many ROLLBACKs?
---------------------------------------------

SQLAlchemy currently assumes DBAPI connections are in "non-autocommit" mode -
this is the default behavior of the Python database API, meaning it
must be assumed that a transaction is always in progress. The
connection pool issues ``connection.rollback()`` when a connection is returned.
This is so that any transactional resources remaining on the connection are
released. On a database like Postgresql or MSSQL where table resources are
aggressively locked, this is critical so that rows and tables don't remain
locked within connections that are no longer in use. An application can
otherwise hang. It's not just for locks, however, and is equally critical on
any database that has any kind of transaction isolation, including MySQL with
InnoDB. Any connection that is still inside an old transaction will return
stale data, if that data was already queried on that connection within
isolation. For background on why you might see stale data even on MySQL, see
http://dev.mysql.com/doc/refman/5.1/en/innodb-transaction-model.html

I'm on MyISAM - how do I turn it off?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The behavior of the connection pool's connection return behavior can be
configured using ``reset_on_return``::

    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    engine = create_engine('mysql://scott:tiger@localhost/myisam_database', pool=QueuePool(reset_on_return=False))

I'm on SQL Server - how do I turn those ROLLBACKs into COMMITs?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``reset_on_return`` accepts the values ``commit``, ``rollback`` in addition
to ``True``, ``False``, and ``None``.   Setting to ``commit`` will cause
a COMMIT as any connection is returned to the pool::

    engine = create_engine('mssql://scott:tiger@mydsn', pool=QueuePool(reset_on_return='commit'))


I am using multiple connections with a SQLite database (typically to test transaction operation), and my test program is not working!
----------------------------------------------------------------------------------------------------------------------------------------------------------

If using a SQLite ``:memory:`` database, or a version of SQLAlchemy prior
to version 0.7, the default connection pool is the :class:`.SingletonThreadPool`,
which maintains exactly one SQLite connection per thread.  So two
connections in use in the same thread will actually be the same SQLite
connection.   Make sure you're not using a :memory: database and
use :class:`.NullPool`, which is the default for non-memory databases in
current SQLAlchemy versions.

.. seealso::

    :ref:`pysqlite_threading_pooling` - info on PySQLite's behavior.

How do I get at the raw DBAPI connection when using an Engine?
--------------------------------------------------------------

With a regular SA engine-level Connection, you can get at a pool-proxied
version of the DBAPI connection via the :attr:`.Connection.connection` attribute on
:class:`.Connection`, and for the really-real DBAPI connection you can call the
:attr:`.ConnectionFairy.connection` attribute on that - but there should never be any need to access
the non-pool-proxied DBAPI connection, as all methods are proxied through::

    engine = create_engine(...)
    conn = engine.connect()
    conn.connection.<do DBAPI things>
    cursor = conn.connection.cursor(<DBAPI specific arguments..>)

You must ensure that you revert any isolation level settings or other
operation-specific settings on the connection back to normal before returning
it to the pool.

As an alternative to reverting settings, you can call the :meth:`.Connection.detach` method on
either :class:`.Connection` or the proxied connection, which will de-associate
the connection from the pool such that it will be closed and discarded
when :meth:`.Connection.close` is called::

    conn = engine.connect()
    conn.detach()  # detaches the DBAPI connection from the connection pool
    conn.connection.<go nuts>
    conn.close()  # connection is closed for real, the pool replaces it with a new connection

MetaData / Schema
==================

My program is hanging when I say ``table.drop()`` / ``metadata.drop_all()``
----------------------------------------------------------------------------

This usually corresponds to two conditions: 1. using PostgreSQL, which is really
strict about table locks, and 2. you have a connection still open which
contains locks on the table and is distinct from the connection being used for
the DROP statement.  Heres the most minimal version of the pattern::

    connection = engine.connect()
    result = connection.execute(mytable.select())

    mytable.drop(engine)

Above, a connection pool connection is still checked out; furthermore, the
result object above also maintains a link to this connection.  If
"implicit execution" is used, the result will hold this connection opened until
the result object is closed or all rows are exhausted.

The call to ``mytable.drop(engine)`` attempts to emit DROP TABLE on a second
connection procured from the :class:`.Engine` which will lock.

The solution is to close out all connections before emitting DROP TABLE::

    connection = engine.connect()
    result = connection.execute(mytable.select())

    # fully read result sets
    result.fetchall()

    # close connections
    connection.close()

    # now locks are removed
    mytable.drop(engine)

Does SQLAlchemy support ALTER TABLE, CREATE VIEW, CREATE TRIGGER, Schema Upgrade Functionality?
-----------------------------------------------------------------------------------------------

General ALTER support isn't present in SQLAlchemy directly.  For special DDL
on an ad-hoc basis, the :class:`.DDL` and related constructs can be used.
See :doc:`core/ddl` for a discussion on this subject.

A more comprehensive option is to use schema migration tools, such as Alembic
or SQLAlchemy-Migrate; see :ref:`schema_migrations` for discussion on this.

How can I sort Table objects in order of their dependency?
-----------------------------------------------------------

This is available via the :attr:`.MetaData.sorted_tables` function::

    metadata = MetaData()
    # ... add Table objects to metadata
    ti = metadata.sorted_tables:
    for t in ti:
        print t

How can I get the CREATE TABLE/ DROP TABLE output as a string?
---------------------------------------------------------------

Modern SQLAlchemy has clause constructs which represent DDL operations. These
can be rendered to strings like any other SQL expression::

    from sqlalchemy.schema import CreateTable

    print CreateTable(mytable)

To get the string specific to a certain engine::

    print CreateTable(mytable).compile(engine)

There's also a special form of :class:`.Engine` that can let you dump an entire
metadata creation sequence, using this recipe::

    def dump(sql, *multiparams, **params):
        print sql.compile(dialect=engine.dialect)
    engine = create_engine('postgresql://', strategy='mock', executor=dump)
    metadata.create_all(engine, checkfirst=False)

The `Alembic <https://bitbucket.org/zzzeek/alembic>`_ tool also supports
an "offline" SQL generation mode that renders database migrations as SQL scripts.

How can I subclass Table/Column to provide certain behaviors/configurations?
------------------------------------------------------------------------------

:class:`.Table` and :class:`.Column` are not good targets for direct subclassing.
However, there are simple ways to get on-construction behaviors using creation
functions, and behaviors related to the linkages between schema objects such as
constraint conventions or naming conventions using attachment events.
An example of many of these
techniques can be seen at `Naming Conventions <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/NamingConventions>`_.


SQL Expressions
=================

.. _faq_sql_expression_string:

How do I render SQL expressions as strings, possibly with bound parameters inlined?
------------------------------------------------------------------------------------

The "stringification" of a SQLAlchemy statement or Query in the vast majority
of cases is as simple as::

    print(str(statement))

this applies both to an ORM :class:`~.orm.query.Query` as well as any :func:`.select` or other
statement.   Additionally, to get the statement as compiled to a
specific dialect or engine, if the statement itself is not already
bound to one you can pass this in to :meth:`.ClauseElement.compile`::

    print(statement.compile(someengine))

or without an :class:`.Engine`::

    from sqlalchemy.dialects import postgresql
    print(statement.compile(dialect=postgresql.dialect()))

When given an ORM :class:`~.orm.query.Query` object, in order to get at the
:meth:`.ClauseElement.compile`
method we only need access the :attr:`~.orm.query.Query.statement`
accessor first::

    statement = query.statement
    print(statement.compile(someengine))

The above forms will render the SQL statement as it is passed to the Python
:term:`DBAPI`, which includes that bound parameters are not rendered inline.
SQLAlchemy normally does not stringify bound parameters, as this is handled
appropriately by the Python DBAPI, not to mention bypassing bound
parameters is probably the most widely exploited security hole in
modern web applications.   SQLAlchemy has limited ability to do this
stringification in certain circumstances such as that of emitting DDL.
In order to access this functionality one can use the ``literal_binds``
flag, passed to ``compile_kwargs``::

    from sqlalchemy.sql import table, column, select

    t = table('t', column('x'))

    s = select([t]).where(t.c.x == 5)

    print(s.compile(compile_kwargs={"literal_binds": True}))

the above approach has the caveats that it is only supported for basic
types, such as ints and strings, and furthermore if a :func:`.bindparam`
without a pre-set value is used directly, it won't be able to
stringify that either.

To support inline literal rendering for types not supported, implement
a :class:`.TypeDecorator` for the target type which includes a
:meth:`.TypeDecorator.process_literal_param` method::

    from sqlalchemy import TypeDecorator, Integer


    class MyFancyType(TypeDecorator):
        impl = Integer

        def process_literal_param(self, value, dialect):
            return "my_fancy_formatting(%s)" % value

    from sqlalchemy import Table, Column, MetaData

    tab = Table('mytable', MetaData(), Column('x', MyFancyType()))

    print(
        tab.select().where(tab.c.x > 5).compile(
            compile_kwargs={"literal_binds": True})
    )

producing output like::

    SELECT mytable.x
    FROM mytable
    WHERE mytable.x > my_fancy_formatting(5)


Why does ``.col.in_([])`` Produce ``col != col``? Why not ``1=0``?
-------------------------------------------------------------------

A little introduction to the issue. The IN operator in SQL, given a list of
elements to compare against a column, generally does not accept an empty list,
that is while it is valid to say::

    column IN (1, 2, 3)

it's not valid to say::

    column IN ()

SQLAlchemy's :meth:`.Operators.in_` operator, when given an empty list, produces this
expression::

    column != column

As of version 0.6, it also produces a warning stating that a less efficient
comparison operation will be rendered. This expression is the only one that is
both database agnostic and produces correct results.

For example, the naive approach of "just evaluate to false, by comparing 1=0
or 1!=1", does not handle nulls properly. An expression like::

    NOT column != column

will not return a row when "column" is null, but an expression which does not
take the column into account::

    NOT 1=0

will.

Closer to the mark is the following CASE expression::

    CASE WHEN column IS NOT NULL THEN 1=0 ELSE NULL END

We don't use this expression due to its verbosity, and its also not
typically accepted by Oracle within a WHERE clause - depending
on how you phrase it, you'll either get "ORA-00905: missing keyword" or
"ORA-00920: invalid relational operator". It's also still less efficient than
just rendering SQL without the clause altogether (or not issuing the SQL at
all, if the statement is just a simple search).

The best approach therefore is to avoid the usage of IN given an argument list
of zero length.  Instead, don't emit the Query in the first place, if no rows
should be returned.  The warning is best promoted to a full error condition
using the Python warnings filter (see http://docs.python.org/library/warnings.html).

ORM Configuration
==================

.. _faq_mapper_primary_key:

How do I map a table that has no primary key?
---------------------------------------------

The SQLAlchemy ORM, in order to map to a particular table, needs there to be
at least one column denoted as a primary key column; multiple-column,
i.e. composite, primary keys are of course entirely feasible as well.  These
columns do **not** need to be actually known to the database as primary key
columns, though it's a good idea that they are.  It's only necessary that the columns
*behave* as a primary key does, e.g. as a unique and not nullable identifier
for a row.

Most ORMs require that objects have some kind of primary key defined
because the object in memory must correspond to a uniquely identifiable
row in the database table; at the very least, this allows the
object can be targeted for UPDATE and DELETE statements which will affect only
that object's row and no other.   However, the importance of the primary key
goes far beyond that.  In SQLAlchemy, all ORM-mapped objects are at all times
linked uniquely within a :class:`.Session`
to their specific database row using a pattern called the :term:`identity map`,
a pattern that's central to the unit of work system employed by SQLAlchemy,
and is also key to the most common (and not-so-common) patterns of ORM usage.


.. note::

    It's important to note that we're only talking about the SQLAlchemy ORM; an
    application which builds on Core and deals only with :class:`.Table` objects,
    :func:`.select` constructs and the like, **does not** need any primary key
    to be present on or associated with a table in any way (though again, in SQL, all tables
    should really have some kind of primary key, lest you need to actually
    update or delete specific rows).

In almost all cases, a table does have a so-called :term:`candidate key`, which is a column or series
of columns that uniquely identify a row.  If a table truly doesn't have this, and has actual
fully duplicate rows, the table is not corresponding to `first normal form <http://en.wikipedia.org/wiki/First_normal_form>`_ and cannot be mapped.   Otherwise, whatever columns comprise the best candidate key can be
applied directly to the mapper::

    class SomeClass(Base):
        __table__ = some_table_with_no_pk
        __mapper_args__ = {
            'primary_key':[some_table_with_no_pk.c.uid, some_table_with_no_pk.c.bar]
        }

Better yet is when using fully declared table metadata, use the ``primary_key=True``
flag on those columns::

    class SomeClass(Base):
        __tablename__ = "some_table_with_no_pk"

        uid = Column(Integer, primary_key=True)
        bar = Column(String, primary_key=True)

All tables in a relational database should have primary keys.   Even a many-to-many
association table - the primary key would be the composite of the two association
columns::

    CREATE TABLE my_association (
      user_id INTEGER REFERENCES user(id),
      account_id INTEGER REFERENCES account(id),
      PRIMARY KEY (user_id, account_id)
    )


How do I configure a Column that is a Python reserved word or similar?
----------------------------------------------------------------------------

Column-based attributes can be given any name desired in the mapping. See
:ref:`mapper_column_distinct_names`.

How do I get a list of all columns, relationships, mapped attributes, etc. given a mapped class?
-------------------------------------------------------------------------------------------------

This information is all available from the :class:`.Mapper` object.

To get at the :class:`.Mapper` for a particular mapped class, call the
:func:`.inspect` function on it::

    from sqlalchemy import inspect

    mapper = inspect(MyClass)

From there, all information about the class can be acquired using such methods as:

* :attr:`.Mapper.attrs` - a namespace of all mapped attributes.  The attributes
  themselves are instances of :class:`.MapperProperty`, which contain additional
  attributes that can lead to the mapped SQL expression or column, if applicable.

* :attr:`.Mapper.column_attrs` - the mapped attribute namespace
  limited to column and SQL expression attributes.   You might want to use
  :attr:`.Mapper.columns` to get at the :class:`.Column` objects directly.

* :attr:`.Mapper.relationships` - namespace of all :class:`.RelationshipProperty` attributes.

* :attr:`.Mapper.all_orm_descriptors` - namespace of all mapped attributes, plus user-defined
  attributes defined using systems such as :class:`.hybrid_property`, :class:`.AssociationProxy` and others.

* :attr:`.Mapper.columns` - A namespace of :class:`.Column` objects and other named
  SQL expressions associated with the mapping.

* :attr:`.Mapper.mapped_table` - The :class:`.Table` or other selectable to which
  this mapper is mapped.

* :attr:`.Mapper.local_table` - The :class:`.Table` that is "local" to this mapper;
  this differs from :attr:`.Mapper.mapped_table` in the case of a mapper mapped
  using inheritance to a composed selectable.

.. _faq_combining_columns:

I'm getting a warning or error about "Implicitly combining column X under attribute Y"
--------------------------------------------------------------------------------------

This condition refers to when a mapping contains two columns that are being
mapped under the same attribute name due to their name, but there's no indication
that this is intentional.  A mapped class needs to have explicit names for
every attribute that is to store an independent value; when two columns have the
same name and aren't disambiguated, they fall under the same attribute and
the effect is that the value from one column is **copied** into the other, based
on which column was assigned to the attribute first.

This behavior is often desirable and is allowed without warning in the case
where the two columns are linked together via a foreign key relationship
within an inheritance mapping.   When the warning or exception occurs, the
issue can be resolved by either assigning the columns to differently-named
attributes, or if combining them together is desired, by using
:func:`.column_property` to make this explicit.

Given the example as follows::

    from sqlalchemy import Integer, Column, ForeignKey
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

    class B(A):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey('a.id'))

As of SQLAlchemy version 0.9.5, the above condition is detected, and will
warn that the ``id`` column of ``A`` and ``B`` is being combined under
the same-named attribute ``id``, which above is a serious issue since it means
that a ``B`` object's primary key will always mirror that of its ``A``.

A mapping which resolves this is as follows::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

    class B(A):
        __tablename__ = 'b'

        b_id = Column('id', Integer, primary_key=True)
        a_id = Column(Integer, ForeignKey('a.id'))

Suppose we did want ``A.id`` and ``B.id`` to be mirrors of each other, despite
the fact that ``B.a_id`` is where ``A.id`` is related.  We could combine
them together using :func:`.column_property`::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

    class B(A):
        __tablename__ = 'b'

        # probably not what you want, but this is a demonstration
        id = column_property(Column(Integer, primary_key=True), A.id)
        a_id = Column(Integer, ForeignKey('a.id'))



I'm using Declarative and setting primaryjoin/secondaryjoin using an ``and_()`` or ``or_()``, and I am getting an error message about foreign keys.
------------------------------------------------------------------------------------------------------------------------------------------------------------------

Are you doing this?::

    class MyClass(Base):
        # ....

        foo = relationship("Dest", primaryjoin=and_("MyClass.id==Dest.foo_id", "MyClass.foo==Dest.bar"))

That's an ``and_()`` of two string expressions, which SQLAlchemy cannot apply any mapping towards.  Declarative allows :func:`.relationship` arguments to be specified as strings, which are converted into expression objects using ``eval()``.   But this doesn't occur inside of an ``and_()`` expression - it's a special operation declarative applies only to the *entirety* of what's passed to primaryjoin or other arguments as a string::

    class MyClass(Base):
        # ....

        foo = relationship("Dest", primaryjoin="and_(MyClass.id==Dest.foo_id, MyClass.foo==Dest.bar)")

Or if the objects you need are already available, skip the strings::

    class MyClass(Base):
        # ....

        foo = relationship(Dest, primaryjoin=and_(MyClass.id==Dest.foo_id, MyClass.foo==Dest.bar))

The same idea applies to all the other arguments, such as ``foreign_keys``::

    # wrong !
    foo = relationship(Dest, foreign_keys=["Dest.foo_id", "Dest.bar_id"])

    # correct !
    foo = relationship(Dest, foreign_keys="[Dest.foo_id, Dest.bar_id]")

    # also correct !
    foo = relationship(Dest, foreign_keys=[Dest.foo_id, Dest.bar_id])

    # if you're using columns from the class that you're inside of, just use the column objects !
    class MyClass(Base):
        foo_id = Column(...)
        bar_id = Column(...)
        # ...

        foo = relationship(Dest, foreign_keys=[foo_id, bar_id])

Performance
===========

How can I profile a SQLAlchemy powered application?
---------------------------------------------------

Looking for performance issues typically involves two stratgies.  One
is query profiling, and the other is code profiling.

Query Profiling
^^^^^^^^^^^^^^^^

Sometimes just plain SQL logging (enabled via python's logging module
or via the ``echo=True`` argument on :func:`.create_engine`) can give an
idea how long things are taking.  For example, if you log something
right after a SQL operation, you'd see something like this in your
log::

    17:37:48,325 INFO  [sqlalchemy.engine.base.Engine.0x...048c] SELECT ...
    17:37:48,326 INFO  [sqlalchemy.engine.base.Engine.0x...048c] {<params>}
    17:37:48,660 DEBUG [myapp.somemessage]

if you logged ``myapp.somemessage`` right after the operation, you know
it took 334ms to complete the SQL part of things.

Logging SQL will also illustrate if dozens/hundreds of queries are
being issued which could be better organized into much fewer queries.
When using the SQLAlchemy ORM, the "eager loading"
feature is provided to partially (:func:`.contains_eager()`) or fully
(:func:`.joinedload()`, :func:`.subqueryload()`)
automate this activity, but without
the ORM "eager loading" typically means to use joins so that results across multiple
tables can be loaded in one result set instead of multiplying numbers
of queries as more depth is added (i.e. ``r + r*r2 + r*r2*r3`` ...)

For more long-term profiling of queries, or to implement an application-side
"slow query" monitor, events can be used to intercept cursor executions,
using a recipe like the following::

    from sqlalchemy import event
    from sqlalchemy.engine import Engine
    import time
    import logging

    logging.basicConfig()
    logger = logging.getLogger("myapp.sqltime")
    logger.setLevel(logging.DEBUG)

    @event.listens_for(Engine, "before_cursor_execute")
    def before_cursor_execute(conn, cursor, statement,
                            parameters, context, executemany):
        conn.info.setdefault('query_start_time', []).append(time.time())
        logger.debug("Start Query: %s" % statement)

    @event.listens_for(Engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement,
                            parameters, context, executemany):
        total = time.time() - conn.info['query_start_time'].pop(-1)
        logger.debug("Query Complete!")
        logger.debug("Total Time: %f" % total)

Above, we use the :meth:`.ConnectionEvents.before_cursor_execute` and
:meth:`.ConnectionEvents.after_cursor_execute` events to establish an interception
point around when a statement is executed.  We attach a timer onto the
connection using the :class:`._ConnectionRecord.info` dictionary; we use a
stack here for the occasional case where the cursor execute events may be nested.

Code Profiling
^^^^^^^^^^^^^^

If logging reveals that individual queries are taking too long, you'd
need a breakdown of how much time was spent within the database
processing the query, sending results over the network, being handled
by the :term:`DBAPI`, and finally being received by SQLAlchemy's result set
and/or ORM layer.   Each of these stages can present their own
individual bottlenecks, depending on specifics.

For that you need to use the
`Python Profiling Module <https://docs.python.org/2/library/profile.html>`_.
Below is a simple recipe which works profiling into a context manager::

    import cProfile
    import StringIO
    import pstats
    import contextlib

    @contextlib.contextmanager
    def profiled():
        pr = cProfile.Profile()
        pr.enable()
        yield
        pr.disable()
        s = StringIO.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats()
        # uncomment this to see who's calling what
        # ps.print_callers()
        print s.getvalue()

To profile a section of code::

    with profiled():
        Session.query(FooClass).filter(FooClass.somevalue==8).all()

The output of profiling can be used to give an idea where time is
being spent.   A section of profiling output looks like this::

    13726 function calls (13042 primitive calls) in 0.014 seconds

    Ordered by: cumulative time

    ncalls  tottime  percall  cumtime  percall filename:lineno(function)
    222/21    0.001    0.000    0.011    0.001 lib/sqlalchemy/orm/loading.py:26(instances)
    220/20    0.002    0.000    0.010    0.001 lib/sqlalchemy/orm/loading.py:327(_instance)
    220/20    0.000    0.000    0.010    0.000 lib/sqlalchemy/orm/loading.py:284(populate_state)
       20    0.000    0.000    0.010    0.000 lib/sqlalchemy/orm/strategies.py:987(load_collection_from_subq)
       20    0.000    0.000    0.009    0.000 lib/sqlalchemy/orm/strategies.py:935(get)
        1    0.000    0.000    0.009    0.009 lib/sqlalchemy/orm/strategies.py:940(_load)
       21    0.000    0.000    0.008    0.000 lib/sqlalchemy/orm/strategies.py:942(<genexpr>)
        2    0.000    0.000    0.004    0.002 lib/sqlalchemy/orm/query.py:2400(__iter__)
        2    0.000    0.000    0.002    0.001 lib/sqlalchemy/orm/query.py:2414(_execute_and_instances)
        2    0.000    0.000    0.002    0.001 lib/sqlalchemy/engine/base.py:659(execute)
        2    0.000    0.000    0.002    0.001 lib/sqlalchemy/sql/elements.py:321(_execute_on_connection)
        2    0.000    0.000    0.002    0.001 lib/sqlalchemy/engine/base.py:788(_execute_clauseelement)

    ...

Above, we can see that the ``instances()`` SQLAlchemy function was called 222
times (recursively, and 21 times from the outside), taking a total of .011
seconds for all calls combined.

Execution Slowness
^^^^^^^^^^^^^^^^^^

The specifics of these calls can tell us where the time is being spent.
If for example, you see time being spent within ``cursor.execute()``,
e.g. against the DBAPI::

    2    0.102    0.102    0.204    0.102 {method 'execute' of 'sqlite3.Cursor' objects}

this would indicate that the database is taking a long time to start returning
results, and it means your query should be optimized, either by adding indexes
or restructuring the query and/or underlying schema.  For that task,
analysis of the query plan is warranted, using a system such as EXPLAIN,
SHOW PLAN, etc. as is provided by the database backend.

Result Fetching Slowness - Core
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If on the other hand you see many thousands of calls related to fetching rows,
or very long calls to ``fetchall()``, it may
mean your query is returning more rows than expected, or that the fetching
of rows itself is slow.   The ORM itself typically uses ``fetchall()`` to fetch
rows (or ``fetchmany()`` if the :meth:`.Query.yield_per` option is used).

An inordinately large number of rows would be indicated
by a very slow call to ``fetchall()`` at the DBAPI level::

    2    0.300    0.600    0.300    0.600 {method 'fetchall' of 'sqlite3.Cursor' objects}

An unexpectedly large number of rows, even if the ultimate result doesn't seem
to have many rows, can be the result of a cartesian product - when multiple
sets of rows are combined together without appropriately joining the tables
together.   It's often easy to produce this behavior with SQLAlchemy Core or
ORM query if the wrong :class:`.Column` objects are used in a complex query,
pulling in additional FROM clauses that are unexpected.

On the other hand, a fast call to ``fetchall()`` at the DBAPI level, but then
slowness when SQLAlchemy's :class:`.ResultProxy` is asked to do a ``fetchall()``,
may indicate slowness in processing of datatypes, such as unicode conversions
and similar::

    # the DBAPI cursor is fast...
    2    0.020    0.040    0.020    0.040 {method 'fetchall' of 'sqlite3.Cursor' objects}

    ...

    # but SQLAlchemy's result proxy is slow, this is type-level processing
    2    0.100    0.200    0.100    0.200 lib/sqlalchemy/engine/result.py:778(fetchall)

In some cases, a backend might be doing type-level processing that isn't
needed.   More specifically, seeing calls within the type API that are slow
are better indicators - below is what it looks like when we use a type like
this::

    from sqlalchemy import TypeDecorator
    import time

    class Foo(TypeDecorator):
        impl = String

        def process_result_value(self, value, thing):
            # intentionally add slowness for illustration purposes
            time.sleep(.001)
            return value

the profiling output of this intentionally slow operation can be seen like this::

      200    0.001    0.000    0.237    0.001 lib/sqlalchemy/sql/type_api.py:911(process)
      200    0.001    0.000    0.236    0.001 test.py:28(process_result_value)
      200    0.235    0.001    0.235    0.001 {time.sleep}

that is, we see many expensive calls within the ``type_api`` system, and the actual
time consuming thing is the ``time.sleep()`` call.

Make sure to check the :doc:`Dialect documentation <dialects/index>`
for notes on known performance tuning suggestions at this level, especially for
databases like Oracle.  There may be systems related to ensuring numeric accuracy
or string processing that may not be needed in all cases.

There also may be even more low-level points at which row-fetching performance is suffering;
for example, if time spent seems to focus on a call like ``socket.receive()``,
that could indicate that everything is fast except for the actual network connection,
and too much time is spent with data moving over the network.

Result Fetching Slowness - ORM
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To detect slowness in ORM fetching of rows (which is the most common area
of performance concern), calls like ``populate_state()`` and ``_instance()`` will
illustrate individual ORM object populations::

    # the ORM calls _instance for each ORM-loaded row it sees, and
    # populate_state for each ORM-loaded row that results in the population
    # of an object's attributes
    220/20    0.001    0.000    0.010    0.000 lib/sqlalchemy/orm/loading.py:327(_instance)
    220/20    0.000    0.000    0.009    0.000 lib/sqlalchemy/orm/loading.py:284(populate_state)

The ORM's slowness in turning rows into ORM-mapped objects is a product
of the complexity of this operation combined with the overhead of cPython.
Common strategies to mitigate this include:

* fetch individual columns instead of full entities, that is::

      session.query(User.id, User.name)

  instead of::

      session.query(User)

* Use :class:`.Bundle` objects to organize column-based results::

      u_b = Bundle('user', User.id, User.name)
      a_b = Bundle('address', Address.id, Address.email)

      for user, address in session.query(u_b, a_b).join(User.addresses):
          # ...

* Use result caching - see :ref:`examples_caching` for an in-depth example
  of this.

* Consider a faster interpreter like that of Pypy.

The output of a profile can be a little daunting but after some
practice they are very easy to read.

If you're feeling ambitious, there's also a more involved example of
SQLAlchemy profiling within the SQLAlchemy unit tests in the
``tests/aaa_profiling`` section.  Tests in this area
use decorators that assert a
maximum number of method calls being used for particular operations,
so that if something inefficient gets checked in, the tests will
reveal it (it is important to note that in cPython, function calls have
the highest overhead of any operation, and the count of calls is more
often than not nearly proportional to time spent).   Of note are the
the "zoomark" tests which use a fancy "SQL capturing" scheme which
cuts out the overhead of the DBAPI from the equation - although that
technique isn't really necessary for garden-variety profiling.

I'm inserting 400,000 rows with the ORM and it's really slow!
--------------------------------------------------------------

The SQLAlchemy ORM uses the :term:`unit of work` pattern when synchronizing
changes to the database. This pattern goes far beyond simple "inserts"
of data. It includes that attributes which are assigned on objects are
received using an attribute instrumentation system which tracks
changes on objects as they are made, includes that all rows inserted
are tracked in an identity map which has the effect that for each row
SQLAlchemy must retrieve its "last inserted id" if not already given,
and also involves that rows to be inserted are scanned and sorted for
dependencies as needed. Objects are also subject to a fair degree of
bookkeeping in order to keep all of this running, which for a very
large number of rows at once can create an inordinate amount of time
spent with large data structures, hence it's best to chunk these.

Basically, unit of work is a large degree of automation in order to
automate the task of persisting a complex object graph into a
relational database with no explicit persistence code, and this
automation has a price.

ORMs are basically not intended for high-performance bulk inserts -
this is the whole reason SQLAlchemy offers the Core in addition to the
ORM as a first-class component.

For the use case of fast bulk inserts, the
SQL generation and execution system that the ORM builds on top of
is part of the Core.  Using this system directly, we can produce an INSERT that
is competitive with using the raw database API directly.

The example below illustrates time-based tests for four different
methods of inserting rows, going from the most automated to the least.
With cPython 2.7, runtimes observed::

    classics-MacBook-Pro:sqlalchemy classic$ python test.py
    SQLAlchemy ORM: Total time for 100000 records 14.3528850079 secs
    SQLAlchemy ORM pk given: Total time for 100000 records 10.0164160728 secs
    SQLAlchemy Core: Total time for 100000 records 0.775382995605 secs
    sqlite3: Total time for 100000 records 0.676795005798 sec

We can reduce the time by a factor of three using recent versions of `Pypy <http://pypy.org/>`_::

    classics-MacBook-Pro:sqlalchemy classic$ /usr/local/src/pypy-2.1-beta2-osx64/bin/pypy test.py
    SQLAlchemy ORM: Total time for 100000 records 5.88369488716 secs
    SQLAlchemy ORM pk given: Total time for 100000 records 3.52294301987 secs
    SQLAlchemy Core: Total time for 100000 records 0.613556146622 secs
    sqlite3: Total time for 100000 records 0.442467927933 sec

Script::

    import time
    import sqlite3

    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import Column, Integer, String,  create_engine
    from sqlalchemy.orm import scoped_session, sessionmaker

    Base = declarative_base()
    DBSession = scoped_session(sessionmaker())
    engine = None

    class Customer(Base):
        __tablename__ = "customer"
        id = Column(Integer, primary_key=True)
        name = Column(String(255))

    def init_sqlalchemy(dbname='sqlite:///sqlalchemy.db'):
        global engine
        engine = create_engine(dbname, echo=False)
        DBSession.remove()
        DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)

    def test_sqlalchemy_orm(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        for i in range(n):
            customer = Customer()
            customer.name = 'NAME ' + str(i)
            DBSession.add(customer)
            if i % 1000 == 0:
                DBSession.flush()
        DBSession.commit()
        print("SQLAlchemy ORM: Total time for " + str(n) +
                    " records " + str(time.time() - t0) + " secs")

    def test_sqlalchemy_orm_pk_given(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        for i in range(n):
            customer = Customer(id=i+1, name="NAME " + str(i))
            DBSession.add(customer)
            if i % 1000 == 0:
                DBSession.flush()
        DBSession.commit()
        print("SQLAlchemy ORM pk given: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")

    def test_sqlalchemy_core(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        engine.execute(
            Customer.__table__.insert(),
            [{"name": 'NAME ' + str(i)} for i in range(n)]
        )
        print("SQLAlchemy Core: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")

    def init_sqlite3(dbname):
        conn = sqlite3.connect(dbname)
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS customer")
        c.execute("CREATE TABLE customer (id INTEGER NOT NULL, "
                                    "name VARCHAR(255), PRIMARY KEY(id))")
        conn.commit()
        return conn

    def test_sqlite3(n=100000, dbname='sqlite3.db'):
        conn = init_sqlite3(dbname)
        c = conn.cursor()
        t0 = time.time()
        for i in range(n):
            row = ('NAME ' + str(i),)
            c.execute("INSERT INTO customer (name) VALUES (?)", row)
        conn.commit()
        print("sqlite3: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " sec")

    if __name__ == '__main__':
        test_sqlalchemy_orm(100000)
        test_sqlalchemy_orm_pk_given(100000)
        test_sqlalchemy_core(100000)
        test_sqlite3(100000)



Sessions / Queries
===================


"This Session's transaction has been rolled back due to a previous exception during flush." (or similar)
---------------------------------------------------------------------------------------------------------

This is an error that occurs when a :meth:`.Session.flush` raises an exception, rolls back
the transaction, but further commands upon the `Session` are called without an
explicit call to :meth:`.Session.rollback` or :meth:`.Session.close`.

It usually corresponds to an application that catches an exception
upon :meth:`.Session.flush` or :meth:`.Session.commit` and
does not properly handle the exception.    For example::

    from sqlalchemy import create_engine, Column, Integer
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base(create_engine('sqlite://'))

    class Foo(Base):
        __tablename__ = 'foo'
        id = Column(Integer, primary_key=True)

    Base.metadata.create_all()

    session = sessionmaker()()

    # constraint violation
    session.add_all([Foo(id=1), Foo(id=1)])

    try:
        session.commit()
    except:
        # ignore error
        pass

    # continue using session without rolling back
    session.commit()


The usage of the :class:`.Session` should fit within a structure similar to this::

    try:
        <use session>
        session.commit()
    except:
       session.rollback()
       raise
    finally:
       session.close()  # optional, depends on use case

Many things can cause a failure within the try/except besides flushes. You
should always have some kind of "framing" of your session operations so that
connection and transaction resources have a definitive boundary, otherwise
your application doesn't really have its usage of resources under control.
This is not to say that you need to put try/except blocks all throughout your
application - on the contrary, this would be a terrible idea.  You should
architect your application such that there is one (or few) point(s) of
"framing" around session operations.

For a detailed discussion on how to organize usage of the :class:`.Session`,
please see :ref:`session_faq_whentocreate`.

But why does flush() insist on issuing a ROLLBACK?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It would be great if :meth:`.Session.flush` could partially complete and then not roll
back, however this is beyond its current capabilities since its internal
bookkeeping would have to be modified such that it can be halted at any time
and be exactly consistent with what's been flushed to the database. While this
is theoretically possible, the usefulness of the enhancement is greatly
decreased by the fact that many database operations require a ROLLBACK in any
case. Postgres in particular has operations which, once failed, the
transaction is not allowed to continue::

    test=> create table foo(id integer primary key);
    NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "foo_pkey" for table "foo"
    CREATE TABLE
    test=> begin;
    BEGIN
    test=> insert into foo values(1);
    INSERT 0 1
    test=> commit;
    COMMIT
    test=> begin;
    BEGIN
    test=> insert into foo values(1);
    ERROR:  duplicate key value violates unique constraint "foo_pkey"
    test=> insert into foo values(2);
    ERROR:  current transaction is aborted, commands ignored until end of transaction block

What SQLAlchemy offers that solves both issues is support of SAVEPOINT, via
:meth:`.Session.begin_nested`. Using :meth:`.Session.begin_nested`, you can frame an operation that may
potentially fail within a transaction, and then "roll back" to the point
before its failure while maintaining the enclosing transaction.

But why isn't the one automatic call to ROLLBACK enough?  Why must I ROLLBACK again?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is again a matter of the :class:`.Session` providing a consistent interface and
refusing to guess about what context its being used. For example, the
:class:`.Session` supports "framing" above within multiple levels. Such as, suppose
you had a decorator ``@with_session()``, which did this::

    def with_session(fn):
       def go(*args, **kw):
           session.begin(subtransactions=True)
           try:
               ret = fn(*args, **kw)
               session.commit()
               return ret
           except:
               session.rollback()
               raise
       return go

The above decorator begins a transaction if one does not exist already, and
then commits it, if it were the creator. The "subtransactions" flag means that
if :meth:`.Session.begin` were already called by an enclosing function, nothing happens
except a counter is incremented - this counter is decremented when :meth:`.Session.commit`
is called and only when it goes back to zero does the actual COMMIT happen. It
allows this usage pattern::

    @with_session
    def one():
       # do stuff
       two()


    @with_session
    def two():
       # etc.

    one()

    two()

``one()`` can call ``two()``, or ``two()`` can be called by itself, and the
``@with_session`` decorator ensures the appropriate "framing" - the transaction
boundaries stay on the outermost call level. As you can see, if ``two()`` calls
``flush()`` which throws an exception and then issues a ``rollback()``, there will
*always* be a second ``rollback()`` performed by the decorator, and possibly a
third corresponding to two levels of decorator. If the ``flush()`` pushed the
``rollback()`` all the way out to the top of the stack, and then we said that
all remaining ``rollback()`` calls are moot, there is some silent behavior going
on there. A poorly written enclosing method might suppress the exception, and
then call ``commit()`` assuming nothing is wrong, and then you have a silent
failure condition. The main reason people get this error in fact is because
they didn't write clean "framing" code and they would have had other problems
down the road.

If you think the above use case is a little exotic, the same kind of thing
comes into play if you want to SAVEPOINT- you might call ``begin_nested()``
several times, and the ``commit()``/``rollback()`` calls each resolve the most
recent ``begin_nested()``. The meaning of ``rollback()`` or ``commit()`` is
dependent upon which enclosing block it is called, and you might have any
sequence of ``rollback()``/``commit()`` in any order, and its the level of nesting
that determines their behavior.

In both of the above cases, if ``flush()`` broke the nesting of transaction
blocks, the behavior is, depending on scenario, anywhere from "magic" to
silent failure to blatant interruption of code flow.

``flush()`` makes its own "subtransaction", so that a transaction is started up
regardless of the external transactional state, and when complete it calls
``commit()``, or ``rollback()`` upon failure - but that ``rollback()`` corresponds
to its own subtransaction - it doesn't want to guess how you'd like to handle
the external "framing" of the transaction, which could be nested many levels
with any combination of subtransactions and real SAVEPOINTs. The job of
starting/ending the "frame" is kept consistently with the code external to the
``flush()``, and we made a decision that this was the most consistent approach.



How do I make a Query that always adds a certain filter to every query?
------------------------------------------------------------------------------------------------

See the recipe at `PreFilteredQuery <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PreFilteredQuery>`_.

I've created a mapping against an Outer Join, and while the query returns rows, no objects are returned.  Why not?
------------------------------------------------------------------------------------------------------------------

Rows returned by an outer join may contain NULL for part of the primary key,
as the primary key is the composite of both tables.  The :class:`.Query` object ignores incoming rows
that don't have an acceptable primary key.   Based on the setting of the ``allow_partial_pks``
flag on :func:`.mapper`, a primary key is accepted if the value has at least one non-NULL
value, or alternatively if the value has no NULL values.  See ``allow_partial_pks``
at :func:`.mapper`.


I'm using ``joinedload()`` or ``lazy=False`` to create a JOIN/OUTER JOIN and SQLAlchemy is not constructing the correct query when I try to add a WHERE, ORDER BY, LIMIT, etc. (which relies upon the (OUTER) JOIN)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

The joins generated by joined eager loading are only used to fully load related
collections, and are designed to have no impact on the primary results of the query.
Since they are anonymously aliased, they cannot be referenced directly.

For detail on this beahvior, see :doc:`orm/loading`.

Query has no ``__len__()``, why not?
------------------------------------

The Python ``__len__()`` magic method applied to an object allows the ``len()``
builtin to be used to determine the length of the collection. It's intuitive
that a SQL query object would link ``__len__()`` to the :meth:`.Query.count`
method, which emits a `SELECT COUNT`. The reason this is not possible is
because evaluating the query as a list would incur two SQL calls instead of
one::

    class Iterates(object):
        def __len__(self):
            print "LEN!"
            return 5

        def __iter__(self):
            print "ITER!"
            return iter([1, 2, 3, 4, 5])

    list(Iterates())

output::

    ITER!
    LEN!

How Do I use Textual SQL with ORM Queries?
-------------------------------------------

See:

* :ref:`orm_tutorial_literal_sql` - Ad-hoc textual blocks with :class:`.Query`

* :ref:`session_sql_expressions` - Using :class:`.Session` with textual SQL directly.

I'm calling ``Session.delete(myobject)`` and it isn't removed from the parent collection!
------------------------------------------------------------------------------------------

See :ref:`session_deleting_from_collections` for a description of this behavior.

why isn't my ``__init__()`` called when I load objects?
-------------------------------------------------------

See :ref:`mapping_constructors` for a description of this behavior.

how do I use ON DELETE CASCADE with SA's ORM?
----------------------------------------------

SQLAlchemy will always issue UPDATE or DELETE statements for dependent
rows which are currently loaded in the :class:`.Session`.  For rows which
are not loaded, it will by default issue SELECT statements to load
those rows and udpate/delete those as well; in other words it assumes
there is no ON DELETE CASCADE configured.
To configure SQLAlchemy to cooperate with ON DELETE CASCADE, see
:ref:`passive_deletes`.

I set the "foo_id" attribute on my instance to "7", but the "foo" attribute is still ``None`` - shouldn't it have loaded Foo with id #7?
----------------------------------------------------------------------------------------------------------------------------------------------------

The ORM is not constructed in such a way as to support
immediate population of relationships driven from foreign
key attribute changes - instead, it is designed to work the
other way around - foreign key attributes are handled by the
ORM behind the scenes, the end user sets up object
relationships naturally. Therefore, the recommended way to
set ``o.foo`` is to do just that - set it!::

    foo = Session.query(Foo).get(7)
    o.foo = foo
    Session.commit()

Manipulation of foreign key attributes is of course entirely legal.  However,
setting a foreign-key attribute to a new value currently does not trigger
an "expire" event of the :func:`.relationship` in which it's involved.  This means
that for the following sequence::

    o = Session.query(SomeClass).first()
    assert o.foo is None  # accessing an un-set attribute sets it to None
    o.foo_id = 7

``o.foo`` is initialized to ``None`` when we first accessed it.  Setting
``o.foo_id = 7`` will have the value of "7" as pending, but no flush
has occurred - so ``o.foo`` is still ``None``::

    # attribute is already set to None, has not been
    # reconciled with o.foo_id = 7 yet
    assert o.foo is None

For ``o.foo`` to load based on the foreign key mutation is usually achieved
naturally after the commit, which both flushes the new foreign key value
and expires all state::

    Session.commit()  # expires all attributes

    foo_7 = Session.query(Foo).get(7)

    assert o.foo is foo_7  # o.foo lazyloads on access

A more minimal operation is to expire the attribute individually - this can
be performed for any :term:`persistent` object using :meth:`.Session.expire`::

    o = Session.query(SomeClass).first()
    o.foo_id = 7
    Session.expire(o, ['foo'])  # object must be persistent for this

    foo_7 = Session.query(Foo).get(7)

    assert o.foo is foo_7  # o.foo lazyloads on access

Note that if the object is not persistent but present in the :class:`.Session`,
it's known as :term:`pending`.   This means the row for the object has not been
INSERTed into the database yet.  For such an object, setting ``foo_id`` does not
have meaning until the row is inserted; otherwise there is no row yet::

    new_obj = SomeClass()
    new_obj.foo_id = 7

    Session.add(new_obj)

    # accessing an un-set attribute sets it to None
    assert new_obj.foo is None

    Session.flush()  # emits INSERT

    # expire this because we already set .foo to None
    Session.expire(o, ['foo'])

    assert new_obj.foo is foo_7  # now it loads


.. topic:: Attribute loading for non-persistent objects

    One variant on the "pending" behavior above is if we use the flag
    ``load_on_pending`` on :func:`.relationship`.   When this flag is set, the
    lazy loader will emit for ``new_obj.foo`` before the INSERT proceeds; another
    variant of this is to use the :meth:`.Session.enable_relationship_loading`
    method, which can "attach" an object to a :class:`.Session` in such a way that
    many-to-one relationships load as according to foreign key attributes
    regardless of the object being in any particular state.
    Both techniques are **not recommended for general use**; they were added to suit
    specific programming scenarios encountered by users which involve the repurposing
    of the ORM's usual object states.

The recipe `ExpireRelationshipOnFKChange <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/ExpireRelationshipOnFKChange>`_ features an example using SQLAlchemy events
in order to coordinate the setting of foreign key attributes with many-to-one
relationships.

Is there a way to automagically have only unique keywords (or other kinds of objects) without doing a query for the keyword and getting a reference to the row containing that keyword?
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

When people read the many-to-many example in the docs, they get hit with the
fact that if you create the same ``Keyword`` twice, it gets put in the DB twice.
Which is somewhat inconvenient.

This `UniqueObject <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/UniqueObject>`_ recipe was created to address this issue.


