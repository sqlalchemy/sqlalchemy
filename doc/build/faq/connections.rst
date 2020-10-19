Connections / Engines
=====================

.. contents::
    :local:
    :class: faq
    :backlinks: none


How do I configure logging?
---------------------------

See :ref:`dbengine_logging`.

How do I pool database connections?   Are my connections pooled?
----------------------------------------------------------------

SQLAlchemy performs application-level connection pooling automatically
in most cases.  With the exception of SQLite, a :class:`_engine.Engine` object
refers to a :class:`.QueuePool` as a source of connectivity.

For more detail, see :ref:`engines_toplevel` and :ref:`pooling_toplevel`.

How do I pass custom connect arguments to my database API?
----------------------------------------------------------

The :func:`_sa.create_engine` call accepts additional arguments either
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

The primary cause of this error is that the MySQL connection has timed out
and has been closed by the server.   The MySQL server closes connections
which have been idle a period of time which defaults to eight hours.
To accommodate this, the immediate setting is to enable the
:paramref:`_sa.create_engine.pool_recycle` setting, which will ensure that a
connection which is older than a set amount of seconds will be discarded
and replaced with a new connection when it is next checked out.

For the more general case of accommodating database restarts and other
temporary loss of connectivity due to network issues, connections that
are in the pool may be recycled in response to more generalized disconnect
detection techniques.  The section :ref:`pool_disconnects` provides
background on both "pessimistic" (e.g. pre-ping) and "optimistic"
(e.g. graceful recovery) techniques.   Modern SQLAlchemy tends to favor
the "pessimistic" approach.

.. seealso::

    :ref:`pool_disconnects`

.. _mysql_sync_errors:

"Commands out of sync; you can't run this command now" / "This result object does not return rows. It has been closed automatically"
------------------------------------------------------------------------------------------------------------------------------------

The MySQL drivers have a fairly wide class of failure modes whereby the state of
the connection to the server is in an invalid state.  Typically, when the connection
is used again, one of these two error messages will occur.    The reason is because
the state of the server has been changed to one in which the client library
does not expect, such that when the client library emits a new statement
on the connection, the server does not respond as expected.

In SQLAlchemy, because database connections are pooled, the issue of the messaging
being out of sync on a connection becomes more important, since when an operation
fails, if the connection itself is in an unusable state, if it goes back into the
connection pool, it will malfunction when checked out again.  The mitigation
for this issue is that the connection is **invalidated** when such a failure
mode occurs so that the underlying database connection to MySQL is discarded.
This invalidation occurs automatically for many known failure modes and can
also be called explicitly via the :meth:`_engine.Connection.invalidate` method.

There is also a second class of failure modes within this category where a context manager
such as ``with session.begin_nested():`` wants to "roll back" the transaction
when an error occurs; however within some failure modes of the connection, the
rollback itself (which can also be a RELEASE SAVEPOINT operation) also
fails, causing misleading stack traces.

Originally, the cause of this error used to be fairly simple, it meant that
a multithreaded program was invoking commands on a single connection from more
than one thread.   This applied to the original "MySQLdb" native-C driver that was
pretty much the only driver in use.   However, with the introduction of pure Python
drivers like PyMySQL and MySQL-connector-Python, as well as increased use of
tools such as gevent/eventlet, multiprocessing (often with Celery), and others,
there is a whole series of factors that has been known to cause this problem, some of
which have been improved across SQLAlchemy versions but others which are unavoidable:

* **Sharing a connection among threads** - This is the original reason these kinds
  of errors occurred.  A program used the same connection in two or more threads at
  the same time, meaning multiple sets of messages got mixed up on the connection,
  putting the server-side session into a state that the client no longer knows how
  to interpret.   However, other causes are usually more likely today.

* **Sharing the filehandle for the connection among processes** - This usually occurs
  when a program uses ``os.fork()`` to spawn a new process, and a TCP connection
  that is present in th parent process gets shared into one or more child processes.
  As multiple processes are now emitting messages to essentially the same filehandle,
  the server receives interleaved messages and breaks the state of the connection.

  This scenario can occur very easily if a program uses Python's "multiprocessing"
  module and makes use of an :class:`_engine.Engine` that was created in the parent
  process.  It's common that "multiprocessing" is in use when using tools like
  Celery.  The correct approach should be either that a new :class:`_engine.Engine`
  is produced when a child process first starts, discarding any :class:`_engine.Engine`
  that came down from the parent process; or, the :class:`_engine.Engine` that's inherited
  from the parent process can have it's internal pool of connections disposed by
  calling :meth:`_engine.Engine.dispose`.

* **Greenlet Monkeypatching w/ Exits** - When using a library like gevent or eventlet
  that monkeypatches the Python networking API, libraries like PyMySQL are now
  working in an asynchronous mode of operation, even though they are not developed
  explicitly against this model.  A common issue is that a greenthread is interrupted,
  often due to timeout logic in the application.  This results in the ``GreenletExit``
  exception being raised, and the pure-Python MySQL driver is interrupted from
  its work, which may have been that it was receiving a response from the server
  or preparing to otherwise reset the state of the connection.   When the exception
  cuts all that work short, the conversation between client and server is now
  out of sync and subsequent usage of the connection may fail.   SQLAlchemy
  as of version 1.1.0 knows how to guard against this, as if a database operation
  is interrupted by a so-called "exit exception", which includes ``GreenletExit``
  and any other subclass of Python ``BaseException`` that is not also a subclass
  of ``Exception``, the connection is invalidated.

* **Rollbacks / SAVEPOINT releases failing** - Some classes of error cause
  the connection to be unusable within the context of a transaction, as well
  as when operating in a "SAVEPOINT" block.  In these cases, the failure
  on the connection has rendered any SAVEPOINT as no longer existing, yet
  when SQLAlchemy, or the application, attempts to "roll back" this savepoint,
  the "RELEASE SAVEPOINT" operation fails, typically with a message like
  "savepoint does not exist".   In this case, under Python 3 there will be
  a chain of exceptions output, where the ultimate "cause" of the error
  will be displayed as well.  Under Python 2, there are no "chained" exceptions,
  however recent versions of SQLAlchemy will attempt to emit a warning
  illustrating the original failure cause, while still throwing the
  immediate error which is the failure of the ROLLBACK.

.. _faq_execute_retry:

How Do I "Retry" a Statement Execution Automatically?
-------------------------------------------------------

The documentation section :ref:`pool_disconnects` discusses the strategies
available for pooled connections that have been disconnected since the last
time a particular connection was checked out.   The most modern feature
in this regard is the :paramref:`_sa.create_engine.pre_ping` parameter, which
allows that a "ping" is emitted on a database connection when it's retrieved
from the pool, reconnecting if the current connection has been disconnected.

It's important to note that this "ping" is only emitted **before** the
connection is actually used for an operation.   Once the connection is
delivered to the caller, per the Python :term:`DBAPI` specification it is now
subject to an **autobegin** operation, which means it will automatically BEGIN
a new transaction when it is first used that remains in effect for subsequent
statements, until the DBAPI-level ``connection.commit()`` or
``connection.rollback()`` method is invoked.

As discussed at :ref:`autocommit`, there is a library level "autocommit"
feature which is deprecated in 1.4 that causes :term:`DML` and :term:`DDL`
executions to commit automatically after individual statements are executed;
however, outside of this deprecated case, modern use of SQLAlchemy works with
this transaction in all cases and does not commit any data unless explicitly
told to commit.

At the ORM level, a similar situation where the ORM
:class:`_orm.Session` object also presents a legacy "autocommit" operation is
present; however even if this legacy mode of operation is used, the
:class:`_orm.Session` still makes use of transactions internally,
particularly within the :meth:`_orm.Session.flush` process.

The implication that this has for the notion of "retrying" a statement is that
in the default case, when a connection is lost, **the entire transaction is
lost**. There is no useful way that the database can "reconnect and retry" and
continue where it left off, since data is already lost.   For this reason,
SQLAlchemy does not have a transparent "reconnection" feature that works
mid-transaction, for the case when the database connection has disconnected
while being used. The canonical approach to dealing with mid-operation
disconnects is to **retry the entire operation from the start of the
transaction**, often by using a Python "retry" decorator, or to otherwise
architect the application in such a way that it is resilient against
transactions that are dropped.

There is also the notion of extensions that can keep track of all of the
statements that have proceeded within a transaction and then replay them all in
a new transaction in order to approximate a "retry" operation.  SQLAlchemy's
:ref:`event system <core_event_toplevel>` does allow such a system to be
constructed, however this approach is also not generally useful as there is
no way to guarantee that those
:term:`DML` statements will be working against the same state, as once a
transaction has ended the state of the database in a new transaction may be
totally different.   Architecting "retry" explicitly into the application
at the points at which transactional operations begin and commit remains
the better approach since the application-level transactional methods are
the ones that know best how to re-run their steps.

Otherwise, if SQLAlchemy were to provide a feature that transparently and
silently "reconnected" a connection mid-transaction, the effect would be that
data is silently lost.   By trying to hide the problem, SQLAlchemy would make
the situation much worse.

However, if we are **not** using transactions, then there are more options
available, as the next section describes.

.. _faq_execute_retry_autocommit:

Using DBAPI Autocommit Allows for a Readonly Version of Transparent Reconnect
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

With the rationale for not having a transparent reconnection mechanism stated,
the preceding section rests upon the assumption that the application is in
fact using DBAPI-level transactions.  As most DBAPIs now offer :ref:`native
"autocommit" settings <dbapi_autocommit>`, we can make use of these features to
provide a limited form of transparent reconnect for **read only,
autocommit only operations**.  A transparent statement retry may be applied to
the ``cursor.execute()`` method of the DBAPI, however it is still not safe to
apply to the ``cursor.executemany()`` method of the DBAPI, as the statement may
have consumed any portion of the arguments given.

.. warning:: The following recipe should **not** be used for operations that
   write data.   Users should carefully read and understand how the recipe
   works and test failure modes very carefully against the specifically
   targeted DBAPI driver before making production use of this recipe.
   The retry mechanism does not guarantee prevention of disconnection errors
   in all cases.

A simple retry mechanism may be applied to the DBAPI level ``cursor.execute()``
method by making use of the :meth:`_events.DialectEvents.do_execute` and
:meth:`_events.DialectEvents.do_execute_no_params` hooks, which will be able to
intercept disconnections during statement executions.   It will **not**
intercept connection failures during result set fetch operations, for those
DBAPIs that don't fully buffer result sets.  The recipe requires that the
database support DBAPI level autocommit and is **not guaranteed** for
particular backends.  A single function ``reconnecting_engine()`` is presented
which applies the event hooks to a given :class:`_engine.Engine` object,
returning an always-autocommit version that enables DBAPI-level autocommit.
A connection will transparently reconnect for single-parameter and no-parameter
statement executions::


  import time

  from sqlalchemy import event


  def reconnecting_engine(engine, num_retries, retry_interval):
      def _run_with_retries(fn, context, cursor, statement, *arg, **kw):
          for retry in range(num_retries + 1):
              try:
                  fn(cursor, statement, context=context, *arg)
              except engine.dialect.dbapi.Error as raw_dbapi_err:
                  connection = context.root_connection
                  if engine.dialect.is_disconnect(
                      raw_dbapi_err, connection, cursor
                  ):
                      if retry > num_retries:
                          raise
                      engine.logger.error(
                          "disconnection error, retrying operation",
                          exc_info=True,
                      )
                      connection.invalidate()

                      # use SQLAlchemy 2.0 API if available
                      if hasattr(connection, "rollback"):
                          connection.rollback()
                      else:
                          trans = connection.get_transaction()
                          if trans:
                              trans.rollback()

                      time.sleep(retry_interval)
                      context.cursor = cursor = connection.connection.cursor()
                  else:
                      raise
              else:
                  return True

      e = engine.execution_options(isolation_level="AUTOCOMMIT")

      @event.listens_for(e, "do_execute_no_params")
      def do_execute_no_params(cursor, statement, context):
          return _run_with_retries(
              context.dialect.do_execute_no_params, context, cursor, statement
          )

      @event.listens_for(e, "do_execute")
      def do_execute(cursor, statement, parameters, context):
          return _run_with_retries(
              context.dialect.do_execute, context, cursor, statement, parameters
          )

      return e

Given the above recipe, a reconnection mid-transaction may be demonstrated
using the following proof of concept script.  Once run, it will emit a
``SELECT 1`` statement to the database every five seconds::

    from sqlalchemy import create_engine
    from sqlalchemy import select

    if __name__ == "__main__":

        engine = create_engine("mysql://scott:tiger@localhost/test", echo_pool=True)

        def do_a_thing(engine):
            with engine.begin() as conn:
                while True:
                    print("ping: %s" % conn.execute(select([1])).scalar())
                    time.sleep(5)

        e = reconnecting_engine(
            create_engine(
                "mysql://scott:tiger@localhost/test", echo_pool=True
            ),
            num_retries=5,
            retry_interval=2,
        )

        do_a_thing(e)

Restart the database while the script runs to demonstrate the transparent
reconnect operation::

    $ python reconnect_test.py
    ping: 1
    ping: 1
    disconnection error, retrying operation
    Traceback (most recent call last):
      ...
    MySQLdb._exceptions.OperationalError: (2006, 'MySQL server has gone away')
    2020-10-19 16:16:22,624 INFO sqlalchemy.pool.impl.QueuePool Invalidate connection <_mysql.connection open to 'localhost' at 0xf59240>
    ping: 1
    ping: 1
    ...

.. versionadded: 1.4  the above recipe makes use of 1.4-specific behaviors and will
   not work as given on previous SQLAlchemy versions.

The above recipe is tested for SQLAlchemy 1.4.



Why does SQLAlchemy issue so many ROLLBACKs?
--------------------------------------------

SQLAlchemy currently assumes DBAPI connections are in "non-autocommit" mode -
this is the default behavior of the Python database API, meaning it
must be assumed that a transaction is always in progress. The
connection pool issues ``connection.rollback()`` when a connection is returned.
This is so that any transactional resources remaining on the connection are
released. On a database like PostgreSQL or MSSQL where table resources are
aggressively locked, this is critical so that rows and tables don't remain
locked within connections that are no longer in use. An application can
otherwise hang. It's not just for locks, however, and is equally critical on
any database that has any kind of transaction isolation, including MySQL with
InnoDB. Any connection that is still inside an old transaction will return
stale data, if that data was already queried on that connection within
isolation. For background on why you might see stale data even on MySQL, see
http://dev.mysql.com/doc/refman/5.1/en/innodb-transaction-model.html

I'm on MyISAM - how do I turn it off?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The behavior of the connection pool's connection return behavior can be
configured using ``reset_on_return``::

    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool

    engine = create_engine('mysql://scott:tiger@localhost/myisam_database', pool=QueuePool(reset_on_return=False))

I'm on SQL Server - how do I turn those ROLLBACKs into COMMITs?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
version of the DBAPI connection via the :attr:`_engine.Connection.connection` attribute on
:class:`_engine.Connection`, and for the really-real DBAPI connection you can call the
:attr:`.ConnectionFairy.connection` attribute on that - but there should never be any need to access
the non-pool-proxied DBAPI connection, as all methods are proxied through::

    engine = create_engine(...)
    conn = engine.connect()
    conn.connection.<do DBAPI things>
    cursor = conn.connection.cursor(<DBAPI specific arguments..>)

You must ensure that you revert any isolation level settings or other
operation-specific settings on the connection back to normal before returning
it to the pool.

As an alternative to reverting settings, you can call the :meth:`_engine.Connection.detach` method on
either :class:`_engine.Connection` or the proxied connection, which will de-associate
the connection from the pool such that it will be closed and discarded
when :meth:`_engine.Connection.close` is called::

    conn = engine.connect()
    conn.detach()  # detaches the DBAPI connection from the connection pool
    conn.connection.<go nuts>
    conn.close()  # connection is closed for real, the pool replaces it with a new connection

How do I use engines / connections / sessions with Python multiprocessing, or os.fork()?
----------------------------------------------------------------------------------------

This is covered in the section :ref:`pooling_multiprocessing`.

