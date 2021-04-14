.. _migration_20_toplevel:

=============================
Migrating to SQLAlchemy 2.0
=============================

.. admonition:: About this document

    SQLAlchemy 2.0 presents a major shift for a wide variety of key
    SQLAlchemy usage patterns in both the Core and ORM components.   The goal
    of this release is to make a slight readjustment in some of the most
    fundamental assumptions of SQLAlchemy since its early beginnings, and to
    deliver a newly streamlined usage model that is hoped to be significantly
    more minimalist and consistent between the Core and ORM components, as well
    as more capable.   The move of Python to be Python 3 only as well as the
    emergence of gradual typing systems for Python 3 are the initial
    inspirations for this shift, as is the changing nature of the Python
    community which now includes not just hardcore database programmers but a
    vast new community of data scientists and students of many different
    disciplines.

    SQLAlchemy started with Python 2.3 which had no context managers, no
    function decorators, Unicode as a second class feature, and a variety of
    other shortcomings that would be unknown today.  The biggest changes in
    SQLAlchemy 2.0 are targeting the residual assumptions left over from this
    early period in SQLAlchemy's development as well as the leftover artifacts
    resulting from the incremental  introduction of key API features such as
    :class:`.orm.query.Query`  and Declarative. It also hopes standardize some
    newer capabilities that have proven to be very effective.


Overview
========

The SQLAlchemy 2.0 transition presents itself in the SQLAlchemy 1.4 release as
a series of steps that allow an application of any size or complexity to be
migrated to SQLAlchemy 2.0 using a gradual, iterative process.  Lessons learned
from the Python 2 to Python 3 transition have inspired a system that intends to
as great a degree as possible to not require any "breaking" changes, or any
change that would need to be made universally or not at all.

As a means of both proving the 2.0 architecture as well as allowing a fully
iterative transition environment, the entire scope of 2.0's new APIs and
features are present and available within the 1.4 series; this includes
major new areas of functionality such as the SQL caching system, the new ORM
statement execution model, new transactional paradigms for both ORM and Core, a
new ORM declarative system that unifies classical and declarative mapping,
support for Python dataclasses, and asyncio support for Core and ORM.

The steps to achieve 2.0 migration are in the following subsections; overall,
the general strategy is that once an application runs on 1.4 with all
warning flags turned on and does not emit any 2.0-deprecation warnings, it is
now cross-compatible with SQLAlchemy 2.0.


First Prerequisite, step one - A Working 1.3 Application
---------------------------------------------------------

The first step is getting an existing application onto 1.4, in the case of
a typical non trivial application, is to ensure it runs on SQLAlchemy 1.3 with
no deprecation warnings.   Release 1.4 does have a few changes linked to
conditions that warn in previous version, including some warnings that were
introduced in 1.3, in particular some changes to the behavior of the
:paramref:`_orm.relationship.viewonly` and
:paramref:`_orm.relationship.sync_backref` flags.

For best results, the application should be able to run, or pass all of its
tests, with the latest SQLAlchemy 1.3 release with no SQLAlchemy deprecation
warnings; these are warnings emitted for the :class:`_exc.SADeprecationWarning`
class.

First Prerequisite, step two - A Working 1.4 Application
--------------------------------------------------------

Once the application is good to go on SQLAlchemy 1.3, the next step is to get
it running on SQLAlchemy 1.4.  In the vast majority of cases, applications
should run without problems from SQLAlchemy 1.3 to 1.4.   However, it's always
the case between any 1.x and 1.y release, APIs and behaviors have changed
either subtly or in some cases a little less subtly, and the SQLAlchemy
project always gets a good deal of regression reports for the first few
months.

The 1.x->1.y release process usually has a few changes around the margins
that are a little bit more dramatic and are based around use cases that are
expected to be very seldom if at all used.   For 1.4, the changes identified
as being in this realm are as follows:

* :ref:`change_5526` - this impacts code that would be manipulating the
  :class:`_engine.URL` object and may impact code that makes use of the
  :class:`_engine.CreateEnginePlugin` extension point.   This is an uncommon
  case but may affect in particular some test suites that are making use of
  special database provisioning logic.   A github search for code that uses
  the relatively new and little-known :class:`_engine.CreateEnginePlugin`
  class found two projects that were unaffected by the change.

* :ref:`change_4617` - this change may impact code that was somehow relying
  upon behavior that was mostly unusable in the :class:`_sql.Select` construct,
  where it would create unnamed subqueries that were usually confusing and
  non-working.  These subqueries would be rejected by most databases in any
  case as a name is usually required except on SQLite, however it is possible
  some applications will need to adjust some queries that are inadvertently
  relying upon this.

* :ref:`change_select_join` - somewhat related, the :class:`_sql.Select` class
  featured ``.join()`` and ``.outerjoin()`` methods that implicitly created a
  subquery and then returned a :class:`_sql.Join` construct, which again would
  be mostly useless and produced lots of confusion.  The decision was made to
  move forward with the vastly more useful 2.0-style join-building approach
  where these methods now work the same way as the ORM :meth:`_orm.Query.join`
  method.

* :ref:`change_deferred_construction` - some error messages related to
  construction of a :class:`_orm.Query` or :class:`_sql.Select` may not be
  emitted until compilation / execution, rather than at construction time.
  This might impact some test suites that are testing against failure modes.

For the full overview of SQLAlchemy 1.4 changes, see the
:doc:`/changelog/migration_14` document.

Migration to 2.0 Step One - Python 3 only (Python 3.6 minimum)
--------------------------------------------------------------

SQLAlchemy 2.0 was first inspired by the fact that Python 2's EOL was in
2020.   SQLAlchemy is taking a longer period of time than other major
projects to drop Python 2.7 support, since it is not too much in the way
of things for the moment.   However, version 2.0 hopes to start embracing
:pep:`484` and other new features to a great degree, so it is likely
that release 1.4 will be the last Python 2 supporting version, even if
there is a SQLAlchemy 1.5 (which is also unlikely at the moment).

In order to use SQLAlchemy 2.0, the application will need to be runnable on
at least **Python 3.6** as of this writing.  SQLAlchemy 1.4 now supports
Python 3.6 or newer within the Python 3 series; throughout the 1.4 series,
the application can remain running on Python 2.7 or on at least Python 3.6.

.. _migration_20_deprecations_mode:

Migration to 2.0 Step Two - Turn on RemovedIn20Warnings
-------------------------------------------------------

SQLAlchemy 1.4 features a conditional deprecation warning system inspired
by the Python "-3" flag that would indicate legacy patterns in a running
application.   For SQLAlchemy 1.4, the :class:`_exc.RemovedIn20Warning`
deprecation class is emitted only when an environment variable
``SQLALCHEMY_WARN_20`` is set to either of ``true`` or ``1``.

Given the example program below::

  from sqlalchemy import column
  from sqlalchemy import create_engine
  from sqlalchemy import select
  from sqlalchemy import table


  engine = create_engine("sqlite://")

  engine.execute("CREATE TABLE foo (id integer)")
  engine.execute("INSERT INTO foo (id) VALUES (1)")


  foo = table("foo", column("id"))
  result = engine.execute(select([foo.c.id]))

  print(result.fetchall())

The above program uses several patterns that many users will already identify
as "legacy", namely the use of the :meth:`_engine.Engine.execute` method
that's part of the :ref:`connectionless execution <dbengine_implicit>`
system.  When we run the above program against 1.4, it returns a single line::

  $ python test3.py
  [(1,)]

To enable "2.0 deprecations mode", we enable the ``SQLALCHEMY_WARN_20=1``
variable, and additionally ensure that a `warnings filter`_ that will not
suppress any warnings is selected::

    SQLALCHEMY_WARN_20=1 python -W always::DeprecationWarning test3.py

Since the reported warning location is not always in the correct place, locating
the offending code may be difficult without the full stacktrace. This can be achieved
by transforming the warnings to exceptions by specifying the ``error`` warning filter,
using Python option ``-W error::DeprecationWarning``.

.. _warnings filter: https://docs.python.org/3/library/warnings.html#the-warnings-filter

With warnings turned on, our program now has a lot to say::

  $ SQLALCHEMY_WARN_20=1 python2 -W always::DeprecationWarning test3.py
  test3.py:9: RemovedIn20Warning: The Engine.execute() function/method is considered legacy as of the 1.x series of SQLAlchemy and will be removed in 2.0. All statement execution in SQLAlchemy 2.0 is performed by the Connection.execute() method of Connection, or in the ORM by the Session.execute() method of Session. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9) (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    engine.execute("CREATE TABLE foo (id integer)")
  /home/classic/dev/sqlalchemy/lib/sqlalchemy/engine/base.py:2856: RemovedIn20Warning: Passing a string to Connection.execute() is deprecated and will be removed in version 2.0.  Use the text() construct, or the Connection.exec_driver_sql() method to invoke a driver-level SQL string. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    return connection.execute(statement, *multiparams, **params)
  /home/classic/dev/sqlalchemy/lib/sqlalchemy/engine/base.py:1639: RemovedIn20Warning: The current statement is being autocommitted using implicit autocommit.Implicit autocommit will be removed in SQLAlchemy 2.0.   Use the .begin() method of Engine or Connection in order to use an explicit transaction for DML and DDL statements. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    self._commit_impl(autocommit=True)
  test3.py:10: RemovedIn20Warning: The Engine.execute() function/method is considered legacy as of the 1.x series of SQLAlchemy and will be removed in 2.0. All statement execution in SQLAlchemy 2.0 is performed by the Connection.execute() method of Connection, or in the ORM by the Session.execute() method of Session. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9) (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    engine.execute("INSERT INTO foo (id) VALUES (1)")
  /home/classic/dev/sqlalchemy/lib/sqlalchemy/engine/base.py:2856: RemovedIn20Warning: Passing a string to Connection.execute() is deprecated and will be removed in version 2.0.  Use the text() construct, or the Connection.exec_driver_sql() method to invoke a driver-level SQL string. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    return connection.execute(statement, *multiparams, **params)
  /home/classic/dev/sqlalchemy/lib/sqlalchemy/engine/base.py:1639: RemovedIn20Warning: The current statement is being autocommitted using implicit autocommit.Implicit autocommit will be removed in SQLAlchemy 2.0.   Use the .begin() method of Engine or Connection in order to use an explicit transaction for DML and DDL statements. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    self._commit_impl(autocommit=True)
  /home/classic/dev/sqlalchemy/lib/sqlalchemy/sql/selectable.py:4271: RemovedIn20Warning: The legacy calling style of select() is deprecated and will be removed in SQLAlchemy 2.0.  Please use the new calling style described at select(). (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9) (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    return cls.create_legacy_select(*args, **kw)
  test3.py:14: RemovedIn20Warning: The Engine.execute() function/method is considered legacy as of the 1.x series of SQLAlchemy and will be removed in 2.0. All statement execution in SQLAlchemy 2.0 is performed by the Connection.execute() method of Connection, or in the ORM by the Session.execute() method of Session. (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9) (Background on SQLAlchemy 2.0 at: http://sqlalche.me/e/b8d9)
    result = engine.execute(select([foo.c.id]))
  [(1,)]

With the above guidance, we can migrate our program to use 2.0 styles, and
as a bonus our program is much clearer::

  from sqlalchemy import column
  from sqlalchemy import create_engine
  from sqlalchemy import select
  from sqlalchemy import table
  from sqlalchemy import text


  engine = create_engine("sqlite://")

  # don't rely on autocommit for DML and DDL
  with engine.begin() as connection:
      # use connection.execute(), not engine.execute()
      # use the text() construct to execute textual SQL
      connection.execute(text("CREATE TABLE foo (id integer)"))
      connection.execute(text("INSERT INTO foo (id) VALUES (1)"))


  foo = table("foo", column("id"))

  with engine.connect() as connection:
      # use connection.execute(), not engine.execute()
      # select() now accepts column / table expressions positionally
      result = connection.execute(select(foo.c.id))

  print(result.fetchall())


The goal of "2.0 deprecations mode" is that a program which runs with no
:class:`_exc.RemovedIn20Warning` warnings with "2.0 deprecations mode" turned
on is then ready to run in SQLAlchemy 2.0.


Migration to 2.0 Step Three - Resolve all RemovedIn20Warnings
--------------------------------------------------------------

Code can be developed iteratively to resolve these warnings.  Within
the SQLAlchemy project itself, the approach taken is as follows:

1. enable the ``SQLALCHEMY_WARN_20=1`` environment variable in the test suite,
   for SQLAlchemy this is in the tox.ini file

2. Within the setup for the test suite, set up a series of warnings filters
   that will select for particular subsets of warnings to either raise an
   exception, or to be ignored (or logged).   Work with just one subgroup of warnings
   at a time.  Below, a warnings filter is configured for an application where
   the change to the Core level ``.execute()`` calls will be needed in order
   for all tests to pass, but all other 2.0-style warnings will be suppressed:

   .. sourcecode::

        import warnings
        from sqlalchemy import exc

        # for warnings related to execute() / scalar(), raise
        for msg in [
            r"The (?:Executable|Engine)\.(?:execute|scalar)\(\) function",
            r"The current statement is being autocommitted using implicit "
            "autocommit,",
            r"The connection.execute\(\) method in SQLAlchemy 2.0 will accept "
            "parameters as a single dictionary or a single sequence of "
            "dictionaries only.",
            r"The Connection.connect\(\) function/method is considered legacy",
            r".*DefaultGenerator.execute\(\)",
        ]:
          warnings.filterwarnings(
              "error", message=msg, category=exc.RemovedIn20Warning,
          )

        # for all other warnings, just log
        warnings.filterwarnings(
          "always", category=exc.RemovedIn20Warning
        )

3. As each sub-category of warnings are resolved in the application, new
   warnings that are caught by the "always" filter can be added to the list
   of "errors" to be resolved.

4. Once no more warnings are emitted, the filter can be removed.

Migration to 2.0 Step Four - Use the ``future`` flag on Engine
--------------------------------------------------------------

The :class:`_engine.Engine` object features an updated
transaction-level API in version 2.0.  In 1.4, this new API is available
by passing the flag ``future=True`` to the :func:`_sa.create_engine`
function.

When the :paramref:`_sa.create_engine.future` flag is used, the :class:`_future.Engine`
and :class:`_future.Connection` objects support the 2.0 API fully and not at all
any legacy features, including the new argument format for :meth:`_future.Connection.execute`,
the removal of "implicit autocommit", string statements require the
:func:`_sql.text` construct unless the :meth:`_future.Connection.exec_driver_sql`
method is used, and connectionless execution from the :class:`_future.Engine`
is removed.

If all :class:`_exc.RemovedIn20Warning` warnings have been resolved regarding
use of the :class:`_engine.Engine` and :class:`_engine.Connection`, then the
:paramref:`_sa.create_engine.future` flag may be enabled and there should be
no errors raised.

The new engine is described at :class:`_future.Engine` which delivers a new
:class:`_future.Connection` object.    In addition to the above changes, the,
:class:`_future.Connection` object features
:meth:`_future.Connection.commit` and
:meth:`_future.Connection.rollback` methods, to support the new
"commit-as-you-go" mode of operation::


    from sqlalchemy import create_engine

    engine = create_engine("postgresql:///")

    with engine.connect() as conn:
        conn.execute(text("insert into table (x) values (:some_x)"), {"some_x": 10})

        conn.commit()  # commit as you go



Migration to 2.0 Step Four - Use the ``future`` flag on Session
---------------------------------------------------------------

The :class:`_orm.Session` object also features an updated transaction/connection
level API in version 2.0.  This API is available in 1.4 using the
:paramref:`_orm.Session.future` flag on :class:`_orm.Session` or on
:class:`_orm.sessionmaker`.

The :class:`_orm.Session` object supports "future" mode in place, and involves
these changes:

1. The :class:`_orm.Session` no longer supports "bound metadata" when it
   resolves the engine to be used for connectivity.   This means that an
   :class:`_engine.Engine` object **must** be passed to the constructor (this
   may be either a legacy or future style object).

2. The :paramref:`_orm.Session.begin.subtransactions` flag is no longer
   supported.

3. The :meth:`_orm.Session.commit` method always emits a COMMIT to the database,
   rather than attempting to reconcile "subtransactions".

4. The :meth:`_orm.Session.rollback` method always rolls back the full
   stack of transactions at once, rather than attempting to keep
   "subtransactions" in place.


The :class:`_orm.Session` also supports more flexible creational patterns
in 1.4 which are now closely matched to the patterns used by the
:class:`_engine.Connection` object.   Highlights include that the
:class:`_orm.Session` may be used as a context manager::

    from sqlalchemy.orm import Session
    with Session(engine) as session:
        session.add(MyObject())
        session.commit()

In addition, the :class:`_orm.sessionmaker` object supports a
:meth:`_orm.sessionmaker.begin` context manager that will create a
:class:`_orm.Session` and begin /commit a transaction in one block::

    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(engine)

    with Session.begin() as session:
        session.add(MyObject())

See the section :ref:`orm_session_vs_engine` for a comparison of
:class:`_orm.Session` creational patterns compared to those of
:class:`_engine.Connection`.

Once the application passes all tests/ runs with ``SQLALCHEMY_WARN_20=1``
and all ``exc.RemovedIn20Warning`` occurrences set to raise an error,
**the application is ready!**.

The sections that follow will detail the specific changes to make for all
major API modifications.


2.0 Migration - Core Connection / Transaction
=============================================


.. _migration_20_autocommit:

Library-level (but not driver level) "Autocommit" removed from both Core and ORM
--------------------------------------------------------------------------------

**Synopsis**

In SQLAlchemy 1.x, the following statements will automatically commit
the underlying DBAPI transaction, but in SQLAlchemy
2.0 this will not occur::

    conn = engine.connect()

    # won't autocommit in 2.0
    conn.execute(some_table.insert().values(foo='bar'))

Nor will this autocommit::

    conn = engine.connect()

    # won't autocommit in 2.0
    conn.execute(text("INSERT INTO table (foo) VALUES ('bar')"))

The common workaround for custom DML that requires commit, the "autocommit"
execution option, will be removed::


    conn = engine.connect()

    # won't autocommit in 2.0
    conn.execute(
      text("EXEC my_procedural_thing()").execution_options(autocommit=True)
    )


**Migration to 2.0**

The method that is cross-compatible with :term:`1.x style` and :term:`2.0
style` execution is to make use of the :meth:`_engine.Connection.begin` method,
or the :meth:`_engine.Engine.begin` context manager::

    with engine.begin() as conn:
        conn.execute(some_table.insert().values(foo='bar'))
        conn.execute(some_other_table.insert().values(bat='hoho'))

    with engine.connect() as conn:
        with conn.begin():
            conn.execute(some_table.insert().values(foo='bar'))
            conn.execute(some_other_table.insert().values(bat='hoho'))

    with engine.begin() as conn:
        conn.execute(text("EXEC my_procedural_thing()"))

When using :term:`2.0 style` with the :paramref:`_sa.create_engine.future`
flag, "commit as you go" style may also be used, as the
:class:`_future.Connection` features **autobegin** behavior, which takes place
when a statement is first invoked in the absence of an explicit call to
:meth:`_future.Connection.begin`::

    with engine.connect() as conn:
        conn.execute(some_table.insert().values(foo='bar'))
        conn.execute(some_other_table.insert().values(bat='hoho'))

        conn.commit()

When :ref:`2.0 deprecations mode <migration_20_deprecations_mode>` is enabled,
a warning will emit when the deprecated "autocommit" feature takes place,
indicating those places where an explicit transaction should be noted.


**Discussion**

SQLAlchemy's first releases were at odds with the spirit of the Python DBAPI
(:pep:`249`) in that it tried to hide :pep:`249`'s emphasis on "implicit begin"
and "explicit commit" of transactions.    Fifteen years later we now see this
was essentially a mistake, as SQLAlchemy's many patterns that attempt to "hide"
the presence of a transaction make for a more complex API which works
inconsistently and is extremely confusing to especially those users who are new
to relational databases and ACID transactions in general.   SQLAlchemy 2.0 will
do away with all attempts to implicitly commit transactions, and usage patterns
will always require that the user demarcate the "beginning" and the "end" of a
transaction in some way, in the same way as reading or writing to a file in
Python has a "beginning" and an "end".

In the case of autocommit for a pure textual statement, there is actually a
regular expression that parses every statement in order to detect autocommit!
Not surprisingly, this regex is continuously failing to accommodate for various
kinds of statements and  stored procedures that imply a "write" to the
database, leading to ongoing confusion as some statements produce results in
the database and others don't.  By preventing the user from being aware of the
transactional concept, we get a lot of bug reports on this one because users
don't understand that databases always use a transaction, whether or not some
layer is autocommitting it.

SQLAlchemy 2.0 will require that all database actions at every level be
explicit as to how the transaction should be used.    For the vast majority
of Core use cases, it's the pattern that is already recommended::

    with engine.begin() as conn:
        conn.execute(some_table.insert().values(foo='bar'))

For "commit as you go, or rollback instead" usage, which resembles how the
:class:`_orm.Session` is normally used today, the "future" version of
:class:`_future.Connection`, which is the one that is returned from an
:class:`_future.Engine` that was created using the
:paramref:`_sa.create_engine.future` flag, includes new
:meth:`_future.Connection.commit` and :meth:`_future.Connection.rollback`
methods, which act upon a transaction that is now begun automatically when
a statement is first invoked::

    # 1.4 / 2.0 code

    from sqlalchemy import create_engine

    engine = create_engine(..., future=True)

    with engine.connect() as conn:
        conn.execute(some_table.insert().values(foo='bar'))
        conn.commit()

        conn.execute(text("some other SQL"))
        conn.rollback()

Above, the ``engine.connect()`` method will return a :class:`_engine.Connection` that
features **autobegin**, meaning the ``begin()`` event is emitted when the
execute method is first used (note however that there is no actual "BEGIN" in
the Python DBAPI).  "autobegin" is a new pattern in SQLAlchemy 1.4 that
is featured both by :class:`_future.Connection` as well as the ORM
:class:`_orm.Session` object; autobegin allows that the :meth:`_future.Connection.begin`
method may be called explicitly when the object is first acquired, for schemes
that wish to demarcate the beginning of the transaction, but if the method
is not called, then it occurs implicitly when work is first done on the object.

The removal of "autocommit" is closely related to the removal of
"connectionless" execution discussed at :ref:`migration_20_implicit_execution`.
All of these legacy patterns built up from the fact that Python did not have
context managers or decorators when SQLAlchemy was first created, so there were
no convenient idiomatic patterns for demarcating the use of a resource.

Driver-level autocommit remains available
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

True "autocommit" behavior is now widely available with most DBAPI
implementations, and is supported by SQLAlchemy via the
:paramref:`_engine.Connection.execution_options.isolation_level` parameter as
discussed at :ref:`dbapi_autocommit`.  True autocommit is treated as an "isolation level"
so that the structure of application code does not change when autocommit is
used; the :meth:`_engine.Connection.begin` context manager as well as
methods like :meth:`_future.Connection.commit` may still be used, they are
simply no-ops at the database driver level when DBAPI-level autocommit
is turned on.

.. _migration_20_implicit_execution:

"Implicit" and "Connectionless" execution, "bound metadata" removed
--------------------------------------------------------------------

**Synopsis**

The ability to associate an :class:`_engine.Engine` with a :class:`_schema.MetaData`
object, which then makes available a range of so-called "connectionless"
execution patterns, is removed::

    from sqlalchemy import MetaData

    metadata = MetaData(bind=engine)  # no longer supported

    metadata.create_all()   # requires Engine or Connection

    metadata.reflect()  # requires Engine or Connection

    t = Table('t', metadata, autoload=True)  # use autoload_with=engine

    result = engine.execute(t.select())  # no longer supported

    result = t.select().execute()  # no longer supported

**Migration to 2.0**

For schema level patterns, explicit use of an :class:`_engine.Engine`
or :class:`_engine.Connection` is required.   The :class:`_engine.Engine`
may still be used directly as the source of connectivity for a
:meth:`_schema.MetaData.create_all` operation or autoload operation.
For executing statements, only the :class:`_engine.Connection` object
has a :meth:`_engine.Connection.execute` method (in addition to
the ORM-level :meth:`_orm.Session.execute` method)::


    from sqlalchemy import MetaData

    metadata = MetaData()

    # engine level:

    # create tables
    metadata.create_all(engine)

    # reflect all tables
    metadata.reflect(engine)

    # reflect individual table
    t = Table('t', metadata, autoload_with=engine)


    # connection level:


    with engine.connect() as connection:
        # create tables, requires explicit begin and/or commit:
        with connection.begin():
            metadata.create_all(connection)

        # reflect all tables
        metadata.reflect(connection)

        # reflect individual table
        t = Table('t', metadata, autoload_with=connection)

        # execute SQL statements
        result = conn.execute(t.select())


**Discussion**


The Core documentation has already standardized on the desired pattern here,
so it is likely that most modern applications would not have to change
much in any case, however there are likely many applications that still
rely upon ``engine.execute()`` calls that will need to be adjusted.

"Connectionless" execution refers to the still fairly popular pattern of
invoking ``.execute()`` from the :class:`_engine.Engine`::

  result = engine.execute(some_statement)

The above operation implicitly procures a :class:`_engine.Connection` object,
and runs the ``.execute()`` method on it.  While this appears to be a simple
convenience feature, it has been shown to give rise to several issues:

* Programs that feature extended strings of ``engine.execute()`` calls have
  become prevalent, overusing a feature that was intended to be seldom used and
  leading to inefficient non-transactional applications.  New users are
  confused as to the difference between ``engine.execute()`` and
  ``connection.execute()`` and the nuance between these two approaches is
  often not understood.

* The feature relies upon the "application level autocommit" feature in order
  to make sense, which itself is also being removed as it is also
  :ref:`inefficient and misleading <migration_20_autocommit>`.

* In order to handle result sets, ``Engine.execute`` returns a result object
  with unconsumed cursor results.  This cursor result necessarily still links
  to the DBAPI connection which remains in an open transaction, all of which is
  released once the result set has fully consumed the rows waiting within the
  cursor.   This means that ``Engine.execute`` does not actually close out the
  connection resources that it claims to be managing when the call is complete.
  SQLAlchemy's "autoclose" behavior is well-tuned enough that users don't
  generally report any negative effects from this system, however it remains
  an overly implicit and inefficient system left over from SQLAlchemy's
  earliest releases.

The removal of "connectionless" execution then leads to the removal of
an even more legacy pattern, that of "implicit, connectionless" execution::

  result = some_statement.execute()

The above pattern has all the issues of "connectionless" execution, plus it
relies upon the "bound metadata" pattern, which SQLAlchemy has tried to
de-emphasize for many years.   This was SQLAlchemy's very first advertised
usage model in version 0.1, which became obsolete almost immediately when
the :class:`_engine.Connection` object was introduced and later Python
context managers provided a better pattern for using resources within a
fixed scope.

With implicit execution removed, "bound metadata" itself also no longer has
a purpose within this system.   In modern use "bound metadata" tends to still
be somewhat convenient for working within :meth:`_schema.MetaData.create_all`
calls as well as with :class:`_orm.Session` objects, however having these
functions receive an :class:`_engine.Engine` explicitly provides for clearer
application design.

Many Choices becomes One Choice
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Overall, the above executional patterns were introduced in SQLAlchemy's
very first 0.1 release before the :class:`_engine.Connection` object even existed.
After many years of de-emphasizing these patterns, "implicit, connectionless"
execution and "bound metadata" are no longer as widely used so in 2.0 we seek
to finally reduce the number of choices for how to execute a statement in
Core from "many choices"::

  # many choices

  # bound metadata?
  metadata = MetaData(engine)

  # or not?
  metadata = MetaData()

  # execute from engine?
  result = engine.execute(stmt)

  # or execute the statement itself (but only if you did
  # "bound metadata" above, which means you can't get rid of "bound" if any
  # part of your program uses this form)
  result = stmt.execute()

  # execute from connection, but it autocommits?
  conn = engine.connect()
  conn.execute(stmt)

  # execute from connection, but autocommit isn't working, so use the special
  # option?
  conn.execution_options(autocommit=True).execute(stmt)

  # or on the statement ?!
  conn.execute(stmt.execution_options(autocommit=True))

  # or execute from connection, and we use explicit transaction?
  with conn.begin():
      conn.execute(stmt)

to "one choice", where by "one choice" we mean "explicit connection with
explicit transaction"; there are still a few ways to demarcate
transaction blocks depending on need.  The "one choice" is to procure a
:class:`_engine.Connection` and then to explicitly demarcate the transaction,
in the case that the operation is a write operation::

  # one choice - work with explicit connection, explicit transaction
  # (there remain a few variants on how to demarcate the transaction)

  # "begin once" - one transaction only per checkout
  with engine.begin() as conn:
      result = conn.execute(stmt)

  # "commit as you go" - zero or more commits per checkout
  with engine.connect() as conn:
      result = conn.execute(stmt)
      conn.commit()

  # "commit as you go" but with a transaction block instead of autobegin
  with engine.connect() as conn:
      with conn.begin():
          result = conn.execute(stmt)


execute() method more strict, execution options are more prominent
-------------------------------------------------------------------------------

**Synopsis**

The argument patterns that may be used with the :meth:`_engine.Connection`
execute method in SQLAlchemy 2.0 are highly simplified, removing many previously
available argument patterns.  The new API in the 1.4 series is described at
:meth:`_future.Connection`. The examples below illustrate the patterns that
require modification::


  connection = engine.connect()

  # direct string SQL not supported; use text() or exec_driver_sql() method
  result = connection.execute("select * from table")

  # positional parameters no longer supported, only named
  # unless using exec_driver_sql()
  result = connection.execute(table.insert(), ('x', 'y', 'z'))

  # **kwargs no longer accepted, pass a single dictionary
  result = connection.execute(table.insert(), x=10, y=5)

  # multiple *args no longer accepted, pass a list
  result = connection.execute(
      table.insert(),
      {"x": 10, "y": 5}, {"x": 15, "y": 12}, {"x": 9, "y": 8}
  )


**Migration to 2.0**

The new :meth:`_future.Connection.execute` method now accepts a subset of the
argument styles that are accepted by the 1.x :meth:`_engine.Connection.execute`
method, so the following code is cross-compatible between 1.x and 2.0::


  connection = engine.connect()

  from sqlalchemy import text
  result = connection.execute(text("select * from table"))

  # pass a single dictionary for single statement execution
  result = connection.execute(table.insert(), {"x": 10, "y": 5})

  # pass a list of dictionaries for executemany
  result = connection.execute(
      table.insert(),
      [{"x": 10, "y": 5}, {"x": 15, "y": 12}, {"x": 9, "y": 8}]
  )



**Discussion**

The use of ``*args`` and ``**kwargs`` has been removed both to remove the
complexity of guessing what kind of arguments were passed to the method, as
well as to make room for other options, namely the
:paramref:`_future.Connection.execute.execution_options` dictionary that is now
available to provide options on a per statement basis. The method is also
modified so that its use pattern matches that of the
:meth:`_orm.Session.execute` method, which is a much more prominent API in 2.0
style.

The removal of direct string SQL is to resolve an inconsistency between
:meth:`_engine.Connection.execute` and :meth:`_orm.Session.execute`,
where in the former case the string is passed to the driver raw, and in the
latter case it is first converted to a :func:`_sql.text` construct.  By
allowing only :func:`_sql.text` this also limits the accepted parameter
format to "named" and not "positional".  Finally, the string SQL use case
is becoming more subject to scrutiny from a security perspective, and
the :func:`_sql.text` construct has come to represent an explicit boundary
into the textual SQL realm where attention to untrusted user input must be
given.


.. _migration_20_result_rows:

Result rows act like named tuples
---------------------------------

**Synopsis**

Version 1.4 introduces an :ref:`all new Result object <change_result_14_core>`
that in turn returns :class:`_engine.Row` objects, which behave like named
tuples when using "future" mode::

    engine = create_engine(..., future=True)  # using future mode

    with engine.connect() as conn:
        result = conn.execute(text("select x, y from table"))

        row = result.first()  # suppose the row is (1, 2)

        "x" in row   # evaluates to False, in 1.x / future=False, this would be True

        1 in row  # evaluates to True, in 1.x / future=False, this would be False


**Migration to 2.0**

Application code or test suites that are testing for a particular key
being present in a row would need to test the ``row.keys()`` collection
instead.  This is however an unusual use case as a result row is typically
used by code that already knows what columns are present within it.

**Discussion**

Already part of 1.4, the previous ``KeyedTuple`` class that was used when
selecting rows from the :class:`_query.Query` object has been replaced by the
:class:`.Row` class, which is the base of the same :class:`.Row` that comes
back with Core statement results when using the
:paramref:`_sa.create_engine.future` flag with :class:`_engine.Engine` (when
the :paramref:`_sa.create_engine.future` flag is not set, Core result sets use
the :class:`.LegacyRow` subclass, which maintains backwards-compatible
behaviors for the ``__contains__()`` method; ORM exclusively uses the
:class:`.Row` class directly).

This :class:`.Row` behaves like a named tuple, in that it acts as a sequence
but also supports attribute name access, e.g. ``row.some_column``.  However,
it also provides the previous "mapping" behavior via the special attribute
``row._mapping``, which produces a Python mapping such that keyed access
such as ``row["some_column"]`` can be used.

In order to receive results as mappings up front, the ``mappings()`` modifier
on the result can be used::

    from sqlalchemy.future.orm import Session

    session = Session(some_engine)

    result = session.execute(stmt)
    for row in result.mappings():
        print("the user is: %s" % row["User"])

The :class:`.Row` class as used by the ORM also supports access via entity
or attribute::

    from sqlalchemy.future import select

    stmt = select(User, Address).join(User.addresses)

    for row in session.execute(stmt).mappings():
        print("the user is: %s  the address is: %s" % (
            row[User],
            row[Address]
        ))

.. seealso::

    :ref:`change_4710_core`


2.0 Migration - Core Usage
=============================

.. _migration_20_5284:

select() no longer accepts varied constructor arguments, columns are passed positionally
-----------------------------------------------------------------------------------------

**synopsis**

The :func:`_sql.select` construct as well as the related method :meth:`_sql.FromClause.select`
will no longer accept keyword arguments to build up elements such as the
WHERE clause, FROM list and ORDER BY.   The list of columns may now be
sent positionally, rather than as a list.  Additionally, the :func:`_sql.case` construct
now accepts its WHEN criteria positionally, rather than as a list::

    # select_from / order_by keywords no longer supported
    stmt = select([1], select_from=table, order_by=table.c.id)

    # whereclause parameter no longer supported
    stmt = select([table.c.x], table.c.id == 5)

    # whereclause parameter no longer supported
    stmt = table.select(table.c.id == 5)

    # list emits a deprecation warning
    stmt = select([table.c.x, table.c.y])

    # list emits a deprecation warning
    case_clause = case(
      [
        (table.c.x == 5, "five"),
        (table.c.x == 7, "seven")
      ],
      else_="neither five nor seven"
    )


**Migration to 2.0**

Only the "generative" style of :func:`_sql.select` will be supported.  The list
of columns / tables to SELECT from should be passed positionally.  The
:func:`_sql.select` construct in SQLAlchemy 1.4 accepts both the legacy
styles and the new styles using an auto-detection scheme, so the code below
is cross-compatible with 1.4 and 2.0::

    # use generative methods
    stmt = select(1).select_from(table).order_by(table.c.id)

    # use generative methods
    stmt = select(table).where(table.c.id == 5)

    # use generative methods
    stmt = table.select().where(table.c.id == 5)

    # pass columns clause expressions positionally
    stmt = select(table.c.x, table.c.y)

    # case conditions passed positionally
    case_clause = case(
      (table.c.x == 5, "five"),
      (table.c.x == 7, "seven"),
      else_="neither five nor seven"
    )

**Discussion**

SQLAlchemy has for many years developed a convention for SQL constructs
accepting an argument either as a list or as positional arguments.   This
convention states that **structural** elements, those that form the structure
of a SQL statement, should be passed **positionally**.   Conversely,
**data** elements, those that form the parameterized data of a SQL statement,
should be passed **as lists**.   For many years, the :func:`_sql.select`
construct could not participate in this convention smoothly because of the
very legacy calling pattern where the "WHERE" clause would be passed positionally.
SQLAlchemy 2.0 finally resolves this by changing the :func:`_sql.select` construct
to only accept the "generative" style that has for many years been the only
documented style in the Core tutorial.

Examples of "structural" vs. "data" elements are as follows::

  # table columns for CREATE TABLE - structural
  table = Table("table", metadata, Column('x', Integer), Column('y', Integer))

  # columns in a SELECT statement - structural
  stmt = select(table.c.x, table.c.y)

  # literal elements in an IN clause - data
  stmt = stmt.where(table.c.y.in_([1, 2, 3]))

.. seealso::

    :ref:`change_5284`

    :ref:`error_c9ae`

insert/update/delete DML no longer accept keyword constructor arguments
-----------------------------------------------------------------------

**Synopsis**

In a similar way as to the previous change to :func:`_sql.select`, the
constructor arguments to :func:`_sql.insert`, :func:`_sql.update` and
:func:`_sql.delete` other than the table argument are essentially removed::

    # no longer supported
    stmt = insert(table, values={"x": 10, "y": 15}, inline=True)

    # no longer supported
    stmt = insert(table, values={"x": 10, "y": 15}, returning=[table.c.x])

    # no longer supported
    stmt = table.delete(table.c.x > 15)

    # no longer supported
    stmt = table.update(
        table.c.x < 15,
        preserve_parameter_order=True
    ).values(
        [(table.c.y, 20), (table.c.x, table.c.y + 10)]
    )

**Migration to 2.0**

The following examples illustrate generative method use for the above
examples::

    # use generative methods, **kwargs OK for values()
    stmt = insert(table).values(x=10, y=15).inline()

    # use generative methods, dictionary also still  OK for values()
    stmt = insert(table).values({"x": 10, "y": 15}).returning(table.c.x)

    # use generative methods
    stmt = table.delete().where(table.c.x > 15)

    # use generative methods, ordered_values() replaces preserve_parameter_order
    stmt = table.update().where(
        table.c.x < 15,
    ).ordered_values(
        (table.c.y, 20), (table.c.x, table.c.y + 10)
    )

**Discussion**

The API and internals is being simplified for the DML constructs in a similar
manner as that of the :func:`_sql.select` construct.



2.0 Migration - ORM Configuration
=============================================

Declarative becomes a first class API
-------------------------------------

**Synopsis**

The ``sqlalchemy.ext.declarative`` package is mostly, with some exceptions,
moved to the ``sqlalchemy.orm`` package.  The :func:`_orm.declarative_base`
and :func:`_orm.declared_attr` functions are present without any behavioral
changes.  A new super-implementation of :func:`_orm.declarative_base`
known as :class:`_orm.registry` now serves as the top-level ORM configurational
construct, which also provides for decorator-based declarative and new
support for classical mappings that integrate with the declarative registry.

**Migration to 2.0**

Change imports::

    from sqlalchemy.ext import declarative_base, declared_attr

To::

    from sqlalchemy.orm import declarative_base, declared_attr

**Discussion**

After ten years or so of popularity, the ``sqlalchemy.ext.declarative``
package is now integrated into the ``sqlalchemy.orm`` namespace, with the
exception of the declarative "extension" classes which remain as Declarative
extensions.   The change is detailed further in the 1.4 migration guide
at :ref:`change_5508`.


.. seealso::

  :ref:`orm_mapping_classes_toplevel` - all new unified documentation for
  Declarative, classical mapping, dataclasses, attrs, etc.


  :ref:`change_5508`


The original "mapper()" function now a core element of Declarative, renamed
----------------------------------------------------------------------------

**Synopsis**

The :func:`_orm.mapper` function moves behind the scenes to be invoked
by higher level APIs.  The new version of this function is the method
:meth:`_orm.registry.map_imperatively` taken from a :class:`_orm.registry`
object.

**Migration to 2.0**

Code that works with classical mappings should change imports and code from::

    from sqlalchemy.orm import mapper


    mapper(SomeClass, some_table, properties={
        "related": relationship(SomeRelatedClass)
    })

To work from a central :class:`_orm.registry` object::

    from sqlalchemy.orm import registry

    mapper_reg = registry()

    mapper_reg.map_imperatively(SomeClass, some_table, properties={
        "related": relationship(SomeRelatedClass)
    })

The above :class:`_orm.registry` is also the source for declarative mappings,
and classical mappings now have access to this registry including string-based
configuration on :func:`_orm.relationship`::

    from sqlalchemy.orm import registry

    mapper_reg = registry()

    Base = mapper_reg.generate_base()

    class SomeRelatedClass(Base):
        __tablename__ = 'related'

        # ...


    mapper_reg.map_imperatively(SomeClass, some_table, properties={
        "related": relationship(
            "SomeRelatedClass",
            primaryjoin="SomeRelatedClass.related_id == SomeClass.id"
        )
    })


**Discussion**

By popular demand, "classical mapping" is staying around, however the new
form of it is based off of the :class:`_orm.registry` object and is available
as :meth:`_orm.registry.map_imperatively`.

In addition, the primary rationale used for "classical mapping" is that of
keeping the :class:`_schema.Table` setup distinct from the class.  Declarative
has always allowed this style using so-called
:ref:`hybrid declarative <orm_imperative_table_configuration>`. However, to
remove the base class requirement, a first class :ref:`decorator
<declarative_config_toplevel>` form has been added.

As yet another separate but related enhancement, support for :ref:`Python
dataclasses <orm_declarative_dataclasses>` is added as well to both
declarative decorator and classical mapping forms.

.. seealso::

  :ref:`orm_mapping_classes_toplevel` - all new unified documentation for
  Declarative, classical mapping, dataclasses, attrs, etc.

2.0 Migration - ORM Usage
=============================================

The biggest visible change in SQLAlchemy 2.0 is the use of
:meth:`_orm.Session.execute` in conjunction with :func:`_sql.select` to run ORM
queries, instead of using :meth:`_orm.Session.query`.  As mentioned elsewhere,
there is no plan to actually remove the :meth:`_orm.Session.query` API itself,
as it is now implemented by using the new API internally it will remain as a
legacy API, and both APIs can be used freely.

The table below provides an introduction to the general change in
calling form with links to documentation for each technique
presented.  The individual migration notes are in the embedded sections
following the table, and may include additional notes not summarized here.


.. container:: sliding-table

  .. list-table:: **Overview of Major ORM Querying Patterns**
    :header-rows: 1

    * - :term:`1.x style` form
      - :term:`2.0 style` form
      - See Also

    * - ::

          session.query(User).get(42)

      - ::

          session.get(User, 42)

      - :ref:`migration_20_get_to_session`

    * - ::

          session.query(User).all()

      - ::

          session.execute(
              select(User)
          ).scalars().all()

      - :ref:`migration_20_unify_select`

        :meth:`_engine.Result.scalars`

    * - ::

          session.query(User).\
          filter_by(name='some user').one()

      - ::

          session.execute(
              select(User).
              filter_by(name="some user")
          ).scalar_one()

      - :ref:`migration_20_unify_select`

        :meth:`_engine.Result.scalar_one`

    * - ::

            session.query(User).options(
                joinedload(User.addresses)
            ).all()

      - ::

            session.execute(
                select(User).
                options(
                  joinedload(User.addresses)
                )
            ).unique().all()

      - :ref:`joinedload_not_uniqued`

    * - ::

          session.query(User).\
              join(Address).\
              filter(Address.email == 'e@sa.us').\
              all()

      - ::

          session.execute(
              select(User).
              join(Address).
              where(Address.email == 'e@sa.us')
          ).scalars().all()

      - :ref:`migration_20_unify_select`

        :ref:`orm_queryguide_joins`

    * - ::

          session.query(User).from_statement(
              text("select * from users")
          ).all()

      - ::

          session.execute(
              select(User).
              from_statement(
                  text("select * from users")
              )
          ).scalars().all()

      - :ref:`orm_queryguide_selecting_text`

    * - ::

          session.query(User).\
              join(User.addresses).\
              options(
                contains_eager(User.addresses)
              ).\
              populate_existing().all()

      - ::

          session.execute(
              select(User).
              join(User.addresses).
              options(contains_eager(User.addresses)).
              execution_options(populate_existing=True)
          ).scalars().all()

      -

          :ref:`orm_queryguide_execution_options`

          :ref:`orm_queryguide_populate_existing`

    *
      - ::

          session.query(User).\
              filter(User.name == 'foo').\
              update(
                  {"fullname": "Foo Bar"},
                  synchronize_session="evaluate"
              )


      - ::

          session.execute(
              update(User).
              where(User.name == 'foo').
              values(fullname="Foo Bar").
              execution_options(synchronize_session="evaluate")
          )

      - :ref:`orm_expression_update_delete`

.. _migration_20_unify_select:

ORM Query Unified with Core Select
----------------------------------

**Synopsis**

The :class:`_orm.Query` object (as well as the :class:`_baked.BakedQuery` and
:class:`_horizontal.ShardedQuery` extensions) become long term legacy objects,
replaced by the direct usage of the :func:`_sql.select` construct in conjunction
with the :meth:`_orm.Session.execute` method.  Results
that are returned from :class:`_orm.Query` in the form of lists of objects
or tuples, or as scalar ORM objects are returned from :meth:`_orm.Session.execute`
uniformly as :class:`_engine.Result` objects, which feature an interface
consistent with that of Core execution.

Legacy code examples are illustrated below::

    session = Session(engine)

    # becomes legacy use case
    user = session.query(User).filter_by(name='some user').one()

    # becomes legacy use case
    user = session.query(User).get(5)

    # becomes legacy use case
    for user in session.query(User).join(User.addresses).filter(Address.email == 'some@email.com'):
        # ...

    # becomes legacy use case
    users = session.query(User).options(joinedload(User.addresses)).order_by(User.id).all()

    # becomes legacy use case
    users = session.query(User).from_statement(
        text("select * from users")
    ).all()

    # etc

**Migration to 2.0**

Because the vast majority of an ORM application is expected to make use of
:class:`_orm.Query` objects as well as that the :class:`_orm.Query` interface
being available does not impact the new interface, the object will stay
around in 2.0 but will no longer be part of documentation nor will it be
supported for the most part.  The :func:`_sql.select` construct now suits
both the Core and ORM use cases, which when invoked via the :meth:`_orm.Session.execute`
method will return ORM-oriented results, that is, ORM objects if that's what
was requested.

The :func:`_sql.Select` construct **adds many new methods** for
compatibility with :class:`_orm.Query`, including :meth:`_sql.Select.filter`
:meth:`_sql.Select.filter_by`, newly reworked :meth:`_sql.Select.join`
and :meth:`_sql.Select.outerjoin` methods, :meth:`_sql.Select.options`,
etc.    Other more supplemental methods of :class:`_orm.Query` such as
:meth:`_orm.Query.populate_existing` are implemented via execution options.

Return results are in terms of a
:class:`_result.Result` object, the new version of the SQLAlchemy
``ResultProxy`` object, which also adds many new methods for compatibility
with :class:`_orm.Query`, including :meth:`_engine.Result.one`, :meth:`_engine.Result.all`,
:meth:`_engine.Result.first`, :meth:`_engine.Result.one_or_none`, etc.

The :class:`_engine.Result` object however does require some different calling
patterns, in that when first returned it will **always return tuples**
and it will **not deduplicate results in memory**.    In order to return
single ORM objects the way :class:`_orm.Query` does, the :meth:`_engine.Result.scalars`
modifier must be called first.  In order to return uniqued objects, as is
necessary when using joined eager loading, the :meth:`_engine.Result.unique`
modifier must be called first.

Documentation for all new features of :func:`_sql.select` including execution
options, etc. are at :doc:`/orm/queryguide`.

Below are some examples of how to migrate to :func:`_sql.select`::


    session = Session(engine)

    user = session.execute(
        select(User).filter_by(name="some user")
    ).scalar_one()


    # get() moves to the Session directly
    user = session.get(User, 5)

    for user in session.execute(
        select(User).join(User.addresses).filter(Address.email == "some@email.case")
    ).scalars():
        # ...

    # when using joinedload() against collections, use unique() on the result
    users = session.execute(
        select(User).options(joinedload(User.addresses)).order_by(User.id)
    ).unique().all()

    # select() has ORM-ish methods like from_statement() that only work
    # if the statement is against ORM entities
    users = session.execute(
        select(User).from_statement(text("select * from users"))
    ).scalars().all()

**Discussion**

The fact that SQLAlchemy has both a :func:`_expression.select` construct
as well as a separate :class:`_orm.Query` object that features an extremely
similar, but fundamentally incompatible interface is likely the greatest
inconsistency in SQLAlchemy, one that arose as a result of small incremental
additions over time that added up to two major APIs that are divergent.

In SQLAlchemy's first releases, the :class:`_orm.Query` object didn't exist
at all.  The original idea was that the :class:`_orm.Mapper` construct itself would
be able to select rows, and that :class:`_schema.Table` objects, not classes,
would be used to create the various criteria in a Core-style approach.   The
:class:`_query.Query` came along some months / years into SQLAlchemy's history
as a user proposal for a new, "buildable" querying object originally called ``SelectResults``
was accepted.
Concepts like a ``.where()`` method, which ``SelectResults`` called ``.filter()``,
were not present in SQLAlchemy previously, and the :func:`_sql.select` construct
used only the "all-at-once" construction style that's now deprecated
at :ref:`migration_20_5284`.

As the new approach took off, the object evolved into the :class:`_orm.Query`
object as new features such as being able to select individual columns,
being able to select multiple entities at once, being able to build subqueries
from a :class:`_orm.Query` object rather than from a :class:`_sql.select`
object were added.   The goal became that :class:`_orm.Query` should have the
full functionality of :class:`_sql.select` in that it could be composed to
build SELECT statements fully with no explicit use of :func:`_sql.select`
needed.   At the same time, :func:`_sql.select` had also evolved "generative"
methods like :meth:`_sql.Select.where` and :meth:`_sql.Select.order_by`.

In modern SQLAlchemy, this goal has been achieved and the two objects are now
completely overlapping in functionality.  The major challenge to unifying these
objects was that the :func:`_sql.select` object needed to remain **completely
agnostic of the ORM**.  To achieve this, the vast majority of logic from
:class:`_orm.Query` has been moved into the SQL compile phase, where
ORM-specific compiler plugins receive the
:class:`_sql.Select` construct and interpret its contents in terms of an
ORM-style query, before passing off to the core-level compiler in order to
create a SQL string.  With the advent of the new
`SQL compilation caching system <change_4639>`,
the majority of this ORM logic is also cached.


.. seealso::

  :ref:`change_5159`

.. _migration_20_get_to_session:

ORM Query - get() method moves to Session
------------------------------------------

**Synopsis**

The :meth:`_orm.Query.get` method remains for legacy purposes, but the
primary interface is now the :meth:`_orm.Session.get` method::

    # legacy usage
    user_obj = session.query(User).get(5)

**Migration to 2.0**

In 1.4 / 2.0, the :class:`_orm.Session` object adds a new
:meth:`_orm.Session.get` method::

    # 1.4 / 2.0 cross-compatible use
    user_obj = session.get(User, 5)

**Discussion**

The :class:`_orm.Query` object is to be a legacy object in 2.0, as ORM
queries are now available using the :func:`_sql.select` object.  As the
:meth:`_orm.Query.get` method defines a special interaction with the
:class:`_orm.Session` and does not necessarily even emit a query, it's more
appropriate that it be part of :class:`_orm.Session`, where it is similar
to other "identity" methods such as :class:`_orm.Session.refresh` and
:class:`_orm.Session.merge`.

SQLAlchemy originally included "get()" to resemble the Hibernate
``Session.load()`` method.  As is so often the case, we got it slightly
wrong as this method is really more about the :class:`_orm.Session` than
with writing a SQL query.

.. _migration_20_orm_query_join_strings:

ORM Query  - Joining / loading on relationships uses attributes, not strings
----------------------------------------------------------------------------

**Synopsis**

This refers to patterns such as that of :meth:`_query.Query.join` as well as
query options like :func:`_orm.joinedload` which currently accept a mixture of
string attribute names or actual class attributes.   The string forms
will all be removed in 2.0::

    # string use removed
    q = session.query(User).join("addresses")

    # string use removed
    q = session.query(User).options(joinedload("addresess"))

    # string use removed
    q = session.query(Address).filter(with_parent(u1, "addresses"))


**Migration to 2.0**

Modern SQLAlchemy 1.x versions support the recommended technique which
is to use mapped attributes::

    # compatible with all modern SQLAlchemy versions

    q = session.query(User).join(User.addresses)

    q = session.query(User).options(joinedload(User.addresess))

    q = session.query(Address).filter(with_parent(u1, User.addresses))

The same techniques apply to :term:`2.0-style` style use::

    # SQLAlchemy 1.4 / 2.0 cross compatible use

    stmt = select(User).join(User.addresses)
    result = session.execute(stmt)

    stmt = select(User).options(joinedload(User.addresess))
    result = session.execute(stmt)

    stmt = select(Address).where(with_parent(u1, User.addresses))
    result = session.execute(stmt)

**Discussion**

The string calling form is ambiguous and requires that the internals do extra
work to determine the appropriate path and retrieve the correct mapped
property. By passing the ORM mapped attribute directly, not only is the
necessary information passed up front, the attribute is also typed and is
more potentially compatible with IDEs and pep-484 integrations.


ORM Query - Chaining using lists of attributes, rather than individual calls, removed
-------------------------------------------------------------------------------------

**Synopsis**

"Chained" forms of joining and loader options which accept multiple mapped
attributes in a list will be removed::

    # chaining removed
    q = session.query(User).join("orders", "items", "keywords")


**Migration to 2.0**

Use individual calls to :meth:`_orm.Query.join` for 1.x /2.0 cross compatible
use::

    q = session.query(User).join(User.orders).join(Order.items).join(Item.keywords)

For :term:`2.0-style` use, :class:`_sql.Select` has the same behavior of
:meth:`_sql.Select.join`, and also features a new :meth:`_sql.Select.join_from`
method that allows an explicit left side::

    # 1.4 / 2.0 cross compatible

    stmt = select(User).join(User.orders).join(Order.items).join(Item.keywords)
    result = session.execute(stmt)

    # join_from can also be helpful
    stmt = select(User).join_from(User, Order).join_from(Order, Item, Order.items)
    result = session.execute(stmt)

**Discussion**

Removing the chaining of attributes is in line with simplifying the calling
interface of methods such as :meth:`_sql.Select.join`.

.. _migration_20_query_join_options:

ORM Query - join(..., aliased=True), from_joinpoint removed
-----------------------------------------------------------

**Synopsis**

The ``aliased=True`` option on :meth:`_query.Query.join` is removed, as is
the ``from_joinpoint`` flag::

  # no longer supported
  q = session.query(Node).\
    join("children", aliased=True).filter(Node.name == "some sub child").
    join("children", from_joinpoint=True, aliased=True).\
    filter(Node.name == 'some sub sub child')

**Migration to 2.0**

Use explicit aliases instead::

  n1 = aliased(Node)
  n2 = aliased(Node)

  q = select(Node).join(Node.children.of_type(n1)).\
      where(n1.name == "some sub child").\
      join(n1.children.of_type(n2)).\
      where(n2.name == "some sub child")


**Discussion**

The ``aliased=True`` option on :meth:`_query.Query.join` is another feature that
seems to be almost never used, based on extensive code searches to find
actual use of this feature.   The internal complexity that the ``aliased=True``
flag requires is **enormous**, and will be going away in 2.0.

Most users aren't familiar with this flag, however it allows for automatic
aliasing of elements along a join, which then applies automatic aliasing
to filter conditions.  The original use case was to assist in long chains
of self-referential joins, as in the example shown above.  However,
the automatic adaption of the filter criteria is enormously
complicated internally and almost never used in real world applications.  The
pattern also leads to issues such as if filter criteria need to be added
at each link in the chain; the pattern then must use the ``from_joinpoint``
flag which SQLAlchemy developers could absolutely find no occurrence of this
parameter ever being used in real world applications.

The ``aliased=True`` and ``from_joinpoint`` parameters were developed at a time
when the :class:`_query.Query` object didn't yet have good capabilities regarding
joining along relationship attributes, functions like
:meth:`.PropComparator.of_type` did not exist, and the :func:`.aliased`
construct itself didn't exist early on.

.. _migration_20_query_distinct:

Using DISTINCT with additional columns, but only select the entity
-------------------------------------------------------------------

**Synopsis**

:class:`_query.Query` will automatically add columns in the ORDER BY when
distinct is used.  The following query will select from all User columns
as well as "address.email_address" but only return User objects::

    # 1.xx code

    result = session.query(User).join(User.addresses).\
        distinct().order_by(Address.email_address).all()

In version 2.0, the "email_address" column will not be automatically added
to the columns clause, and the above query will fail, since relational
databases won't allow you to ORDER BY "address.email_address" when using
DISTINCT if it isn't also in the columns clause.

**Migration to 2.0**

In 2.0, the column must be added explicitly.  To resolve the issue of only
returning the main entity object, and not the extra column, use the
:meth:`_result.Result.columns` method::

    # 1.4 / 2.0 code

    stmt = select(User, Address.email_address).join(User.addresses).\
        distinct().order_by(Address.email_address)

    result = session.execute(stmt).columns(User).all()

**Discussion**

This case is an example of the limited flexibility of :class:`_orm.Query`
leading to the case where implicit, "magical" behavior needed to be added;
the "email_address" column is implicitly added to the columns clause, then
additional internal logic would omit that column from the actual results
returned.

The new approach simplifies the interaction and makes what's going on
explicit, while still making it possible to fulfill the original use case
without inconvenience.


.. _migration_20_query_from_self:

Selecting from the query itself as a subquery, e.g. "from_self()"
-------------------------------------------------------------------

**Synopsis**

The :meth:`_orm.Query.from_self` method will be removed from :class:`_orm.Query`::

    # from_self is removed
    q = session.query(User, Address.email_address).\
      join(User.addresses).\
      from_self(User).order_by(Address.email_address)


**Migration to 2.0**

The :func:`._orm.aliased` construct may be used to emit ORM queries against
an entity that is in terms of any arbitrary selectable.   It has been enhanced
in version 1.4 to smoothly accommodate being used multiple times against
the same subquery for different entities as well.  This can be
used in :term:`1.x style` with :class:`_orm.Query` as below; note that
since the final query wants to query in terms of both the ``User`` and
``Address`` entities, two separate :func:`_orm.aliased` constructs are created::

    from sqlalchemy.orm import aliased

    subq = session.query(User, Address.email_address).\
      join(User.addresses).subquery()

    ua = aliased(User, subq)

    aa = aliased(Address, subq)

    q = session.query(ua, aa).order_by(aa.email_address)

The same form may be used in :term:`2.0 style`::

    from sqlalchemy.orm import aliased

    subq = select(User, Address.email_address).\
      join(User.addresses).subquery()

    ua = aliased(User, subq)

    aa = aliased(Address, subq)

    stmt = select(ua, aa).order_by(aa.email_address)

    result = session.execute(stmt)


**Discussion**

The :meth:`_query.Query.from_self` method is a very complicated method that is rarely
used.   The purpose of this method is to convert a :class:`_query.Query` into a
subquery, then return a new :class:`_query.Query` which SELECTs from that subquery.
The elaborate aspect of this method is that the returned query applies
automatic translation of ORM entities and columns to be stated in the SELECT in
terms of the subquery, as well as that it allows the entities and columns to be
SELECTed from to be modified.

Because :meth:`_query.Query.from_self` packs an intense amount of implicit
translation into the SQL it produces, while it does allow a certain kind of
pattern to be executed very succinctly, real world use of this method is
infrequent as it is not simple to understand.

The new approach makes use of the :func:`_orm.aliased` construct so that the
ORM internals don't need to guess which entities and columns should be adapted
and in what way; in the example above, the ``ua`` and ``aa`` objects, both
of which are :class:`_orm.AliasedClass` instances, provide to the internals
an unambiguous marker as to where the subquery should be referred towards
as well as what entity column or relationship is being considered for a given
component of the query.

SQLAlchemy 1.4 also features an improved labeling style that no longer requires
the use of long labels that include the table name in order to disambiguate
columns of same names from different tables.  In the above examples, even if
our ``User`` and ``Address`` entities have overlapping column names, we can
select from both entities at once without having to specify any particular
labeling::

  # 1.4 / 2.0 code

  subq = select(User, Address).\
      join(User.addresses).subquery()

  ua = aliased(User, subq)
  aa = aliased(Address, subq)

  stmt = select(ua, aa).order_by(aa.email_address)
  result = session.execute(stmt)

The above query will disambiguate the ``.id`` column of ``User`` and
``Address``, where ``Address.id`` is rendered and tracked as ``id_1``::

  SELECT anon_1.id AS anon_1_id, anon_1.id_1 AS anon_1_id_1,
         anon_1.user_id AS anon_1_user_id,
         anon_1.email_address AS anon_1_email_address
  FROM (
    SELECT "user".id AS id, address.id AS id_1,
    address.user_id AS user_id, address.email_address AS email_address
    FROM "user" JOIN address ON "user".id = address.user_id
  ) AS anon_1 ORDER BY anon_1.email_address


:ticket:`5221`

Selecting entities from alternative selectables; Query.select_entity_from()
---------------------------------------------------------------------------

**Synopsis**

The :meth:`_orm.Query.select_entity_from` method will be removed in 2.0::

    subquery = session.query(User).filter(User.id == 5).subquery()

    user = session.query(User).select_entity_from(subquery).first()

**Migration to 2.0**

As is the case described at :ref:`migration_20_query_from_self`, the
:func:`_orm.aliased` object provides a single place that operations like
"select entity from a subquery" may be achieved.  Using :term:`1.x style`::

    from sqlalchemy.orm import aliased

    subquery = session.query(User).filter(User.id == 5).subquery()

    ua = aliased(User, subquery)

    user = session.query(ua).first()

Using :term:`2.0 style`::

    from sqlalchemy.orm import aliased

    subquery = select(User).where(User.id == 5).subquery()

    ua = aliased(User, subquery)

    user = session.execute(select(ua)).scalars().first()

**Discussion**

The points here are basically the same as those discussed at
:ref:`migration_20_query_from_self`.   The :meth:`_orm.Query.select_from_entity`
method was another way to instruct the query to load rows for a particular
ORM mapped entity from an alternate selectable, which involved having the
ORM apply automatic aliasing to that entity wherever it was used in the
query later on, such as in the WHERE clause or ORDER BY.   This intensely
complex feature is seldom used in this way, where as was the case with
:meth:`_orm.Query.from_self`, it's much easier to follow what's going on
when using an explicit :func:`_orm.aliased` object, both from a user point
of view as well as how the internals of the SQLAlchemy ORM must handle it.


.. _joinedload_not_uniqued:

ORM Rows not uniquified by default
----------------------------------

**Synopsis**

ORM rows returned by ``session.execute(stmt)`` are no longer automatically
"uniqued".    This will normally be a welcome change, except in the case
where the "joined eager loading" loader strategy is used with collections::

    # In the legacy API, many rows each have the same User primary key, but
    # only one User per primary key is returned
    users = session.query(User).options(joinedload(User.addresses))

    # In the new API, uniquing is available but not implicitly
    # enabled
    result = session.execute(
        select(User).options(joinedload(User.addresses))
    )

    # this actually will raise an error to let the user know that
    # uniquing should be applied
    rows = result.all()

**Migrating to 2.0**

When using a joined load of a collection, it's required that the
:meth:`_engine.Result.unique` method is called.  The ORM will actually set
a default row handler that will raise an error if this is not done, to
ensure that a joined eager load collection does not return duplicate rows
while still maintaining explicitness::

    # 1.4 / 2.0 code

    stmt = select(User).options(joinedload(User.addresses))

    # statement will raise if unique() is not used, due to joinedload()
    # of a collection.  in all other cases, unique() is not needed.
    # By stating unique() explicitly, confusion over discrepancies between
    # number of objects/ rows returned vs. "SELECT COUNT(*)" is resolved
    rows = session.execute(stmt).unique().all()

**Discussion**

The situation here is a little bit unusual, in that SQLAlchemy is requiring
that a method be invoked that it is in fact entirely capable of doing
automatically.   The reason for requiring that the method be called is to
ensure the developer is "opting in" to the use of the
:meth:`_engine.Result.unique` method, such that they will not be confused when
a straight count of rows does not conflict with the count of
records in the actual result set, which has been a long running source of
user confusion and bug reports for many years.    That the uniquifying is
not happening in any other case by default will improve performance and
also improve clarity in those cases where automatic uniquing was causing
confusing results.

To the degree that having to call :meth:`_engine.Result.unique` when joined
eager load collections are used is inconvenient, in modern SQLAlchemy
the :func:`_orm.selectinload` strategy presents a collection-oriented
eager loader that is superior in most respects to :func:`_orm.joinedload`
and should be preferred.


Autocommit mode removed from Session; autobegin support added
-------------------------------------------------------------

**Synopsis**

The :class:`_orm.Session` will no longer support "autocommit" mode, that
is, this pattern::

    from sqlalchemy.orm import Session

    sess = Session(engine, autocommit=True)

    # no transaction begun, but emits SQL, won't be supported
    obj = sess.query(Class).first()


    # session flushes in a transaction that it begins and
    # commits, won't be supported
    sess.flush()


**Migration to 2.0**

The main reason a :class:`_orm.Session` is used in "autocommit" mode
is so that the :meth:`_orm.Session.begin` method is available, so that framework
integrations and event hooks can control when this event happens.  In 1.4,
the :class:`_orm.Session` now features :ref:`autobegin behavior <change_5074>`
which resolves this issue; the :meth:`_orm.Session.begin` method may now
be called::


    from sqlalchemy.orm import Session

    sess = Session(engine)

    sess.begin()  # begin explicitly; if not called, will autobegin
                  # when database access is needed

    sess.add(obj)

    sess.commit()

**Discussion**

The "autocommit" mode is another holdover from the first versions
of SQLAlchemy.  The flag has stayed around mostly in support of allowing
explicit use of :meth:`_orm.Session.begin`, which is now solved by 1.4,
as well as to allow the use of "subtransactions", which are also removed in
2.0.

Session "subtransaction" behavior removed
------------------------------------------

See the section :ref:`session_subtransactions` for background on this
change.


2.0 Migration - ORM Extension and Recipe Changes
================================================

Dogpile cache recipe and Horizontal Sharding uses new Session API
------------------------------------------------------------------

As the :class:`_orm.Query` object becomes legacy, these two recipes
which previously relied upon subclassing of the :class:`_orm.Query`
object now make use of the :meth:`_orm.SessionEvents.do_orm_execute`
hook.    See the section :ref:`do_orm_execute_re_executing` for
an example.



Baked Query Extension Superseded by built-in caching
-----------------------------------------------------

The baked query extension is superseded by the built in caching system and
is no longer used by the ORM internals.

See :ref:`sql_caching` for full background on the new caching system.



Asyncio Support
=====================

SQLAlchemy 1.4 includes asyncio support for both Core and ORM.
The new API exclusively makes use of the "future" patterns noted above.
See :ref:`change_3414` for background.
