.. _faq_performance:

Performance
===========

.. contents::
    :local:
    :class: faq
    :backlinks: none

.. _faq_how_to_profile:

How can I profile a SQLAlchemy powered application?
---------------------------------------------------

Looking for performance issues typically involves two strategies.  One
is query profiling, and the other is code profiling.

Query Profiling
^^^^^^^^^^^^^^^

Sometimes just plain SQL logging (enabled via python's logging module
or via the ``echo=True`` argument on :func:`_sa.create_engine`) can give an
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
(:func:`_orm.joinedload()`, :func:`.subqueryload()`)
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
        logger.debug("Start Query: %s", statement)

    @event.listens_for(Engine, "after_cursor_execute")
    def after_cursor_execute(conn, cursor, statement,
                            parameters, context, executemany):
        total = time.time() - conn.info['query_start_time'].pop(-1)
        logger.debug("Query Complete!")
        logger.debug("Total Time: %f", total)

Above, we use the :meth:`_events.ConnectionEvents.before_cursor_execute` and
:meth:`_events.ConnectionEvents.after_cursor_execute` events to establish an interception
point around when a statement is executed.  We attach a timer onto the
connection using the :class:`._ConnectionRecord.info` dictionary; we use a
stack here for the occasional case where the cursor execute events may be nested.

.. _faq_code_profiling:

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
    import io
    import pstats
    import contextlib

    @contextlib.contextmanager
    def profiled():
        pr = cProfile.Profile()
        pr.enable()
        yield
        pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
        ps.print_stats()
        # uncomment this to see who's calling what
        # ps.print_callers()
        print(s.getvalue())

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
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If on the other hand you see many thousands of calls related to fetching rows,
or very long calls to ``fetchall()``, it may
mean your query is returning more rows than expected, or that the fetching
of rows itself is slow.   The ORM itself typically uses ``fetchall()`` to fetch
rows (or ``fetchmany()`` if the :meth:`_query.Query.yield_per` option is used).

An inordinately large number of rows would be indicated
by a very slow call to ``fetchall()`` at the DBAPI level::

    2    0.300    0.600    0.300    0.600 {method 'fetchall' of 'sqlite3.Cursor' objects}

An unexpectedly large number of rows, even if the ultimate result doesn't seem
to have many rows, can be the result of a cartesian product - when multiple
sets of rows are combined together without appropriately joining the tables
together.   It's often easy to produce this behavior with SQLAlchemy Core or
ORM query if the wrong :class:`_schema.Column` objects are used in a complex query,
pulling in additional FROM clauses that are unexpected.

On the other hand, a fast call to ``fetchall()`` at the DBAPI level, but then
slowness when SQLAlchemy's :class:`_engine.CursorResult` is asked to do a ``fetchall()``,
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

Make sure to check the :ref:`Dialect documentation <dialect_toplevel>`
for notes on known performance tuning suggestions at this level, especially for
databases like Oracle.  There may be systems related to ensuring numeric accuracy
or string processing that may not be needed in all cases.

There also may be even more low-level points at which row-fetching performance is suffering;
for example, if time spent seems to focus on a call like ``socket.receive()``,
that could indicate that everything is fast except for the actual network connection,
and too much time is spent with data moving over the network.

Result Fetching Slowness - ORM
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

* Consider a faster interpreter like that of PyPy.

The output of a profile can be a little daunting but after some
practice they are very easy to read.

.. seealso::

    :ref:`examples_performance` - a suite of performance demonstrations
    with bundled profiling capabilities.

I'm inserting 400,000 rows with the ORM and it's really slow!
-------------------------------------------------------------

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
is part of the :ref:`Core <sqlexpression_toplevel>`.  Using this system directly, we can produce an INSERT that
is competitive with using the raw database API directly.

.. note::

    When using the psycopg2 dialect, consider making use of the :ref:`batch
    execution helpers <psycopg2_executemany_mode>` feature of psycopg2, now
    supported directly by the SQLAlchemy psycopg2 dialect.

Alternatively, the SQLAlchemy ORM offers the :ref:`bulk_operations`
suite of methods, which provide hooks into subsections of the unit of
work process in order to emit Core-level INSERT and UPDATE constructs with
a small degree of ORM-based automation.

The example below illustrates time-based tests for several different
methods of inserting rows, going from the most automated to the least.
With cPython 2.7, runtimes observed::

    SQLAlchemy ORM: Total time for 100000 records 6.89754080772 secs
    SQLAlchemy ORM pk given: Total time for 100000 records 4.09481811523 secs
    SQLAlchemy ORM bulk_save_objects(): Total time for 100000 records 1.65821218491 secs
    SQLAlchemy ORM bulk_insert_mappings(): Total time for 100000 records 0.466513156891 secs
    SQLAlchemy Core: Total time for 100000 records 0.21024107933 secs
    sqlite3: Total time for 100000 records 0.137335062027 sec

We can reduce the time by a factor of nearly three using recent versions of `PyPy <http://pypy.org/>`_::

    SQLAlchemy ORM: Total time for 100000 records 2.39429616928 secs
    SQLAlchemy ORM pk given: Total time for 100000 records 1.51412987709 secs
    SQLAlchemy ORM bulk_save_objects(): Total time for 100000 records 0.568987131119 secs
    SQLAlchemy ORM bulk_insert_mappings(): Total time for 100000 records 0.320806980133 secs
    SQLAlchemy Core: Total time for 100000 records 0.206904888153 secs
    sqlite3: Total time for 100000 records 0.165791988373 sec

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
        for i in xrange(n):
            customer = Customer()
            customer.name = 'NAME ' + str(i)
            DBSession.add(customer)
            if i % 1000 == 0:
                DBSession.flush()
        DBSession.commit()
        print(
            "SQLAlchemy ORM: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")


    def test_sqlalchemy_orm_pk_given(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        for i in xrange(n):
            customer = Customer(id=i + 1, name="NAME " + str(i))
            DBSession.add(customer)
            if i % 1000 == 0:
                DBSession.flush()
        DBSession.commit()
        print(
            "SQLAlchemy ORM pk given: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")


    def test_sqlalchemy_orm_bulk_save_objects(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        for chunk in range(0, n, 10000):
            DBSession.bulk_save_objects(
                [
                    Customer(name="NAME " + str(i))
                    for i in xrange(chunk, min(chunk + 10000, n))
                ]
            )
        DBSession.commit()
        print(
            "SQLAlchemy ORM bulk_save_objects(): Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")


    def test_sqlalchemy_orm_bulk_insert(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        for chunk in range(0, n, 10000):
            DBSession.bulk_insert_mappings(
                Customer,
                [
                    dict(name="NAME " + str(i))
                    for i in xrange(chunk, min(chunk + 10000, n))
                ]
            )
        DBSession.commit()
        print(
            "SQLAlchemy ORM bulk_insert_mappings(): Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")


    def test_sqlalchemy_core(n=100000):
        init_sqlalchemy()
        t0 = time.time()
        engine.execute(
            Customer.__table__.insert(),
            [{"name": 'NAME ' + str(i)} for i in xrange(n)]
        )
        print(
            "SQLAlchemy Core: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " secs")


    def init_sqlite3(dbname):
        conn = sqlite3.connect(dbname)
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS customer")
        c.execute(
            "CREATE TABLE customer (id INTEGER NOT NULL, "
            "name VARCHAR(255), PRIMARY KEY(id))")
        conn.commit()
        return conn


    def test_sqlite3(n=100000, dbname='sqlite3.db'):
        conn = init_sqlite3(dbname)
        c = conn.cursor()
        t0 = time.time()
        for i in xrange(n):
            row = ('NAME ' + str(i),)
            c.execute("INSERT INTO customer (name) VALUES (?)", row)
        conn.commit()
        print(
            "sqlite3: Total time for " + str(n) +
            " records " + str(time.time() - t0) + " sec")

    if __name__ == '__main__':
        test_sqlalchemy_orm(100000)
        test_sqlalchemy_orm_pk_given(100000)
        test_sqlalchemy_orm_bulk_save_objects(100000)
        test_sqlalchemy_orm_bulk_insert(100000)
        test_sqlalchemy_core(100000)
        test_sqlite3(100000)


