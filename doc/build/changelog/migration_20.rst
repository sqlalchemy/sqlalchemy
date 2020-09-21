.. _migration_20_toplevel:

=============================
SQLAlchemy 2.0 Transition
=============================

.. admonition:: About this document

    SQLAlchemy 2.0 is expected to be a major shift for a wide variety of key
    SQLAlchemy usage patterns in both the Core and ORM components.   The goal
    of this release is to make a slight readjustment in some of the most
    fundamental assumptions of SQLAlchemy since its early beginnings, and to
    deliver a newly streamlined usage model that is hoped to be significantly
    more minimalist and consistent between the Core and ORM components, as well
    as more capable.   The move of Python to be Python 3 only as well as the
    emergence of static typing systems for Python 3 are the initial
    inspirations for this shift, as is the changing nature of the Python
    community which now includes not just hardcore database programmers but a
    vast new community of data scientists and students of many different
    disciplines.

    With the benefit of fifteen years of widespread use and tens of thousands
    of user  questions and issues  answered, SQLAlchemy has been ready to
    reorganize some of its priorities for quite some time, and the "big shift"
    to Python 3 only is seen as a great opportunity to put the deepest ones
    into play.  SQLAlchemy's first releases were for Python 2.3, which had no
    context managers, no decorators, Unicode support as mostly an added-on
    feature that was poorly understood, and a variety of other syntactical
    shortcomings that would be unknown today.   The vast majority of Python
    packages that are today taken for granted did not exist. SQLAlchemy itself
    struggled with major API adjustments through versions 0.1 to 0.5, with such
    major concepts as :class:`_engine.Connection`, :class:`.orm.query.Query`, and the
    Declarative mapping approach only being conceived and added to releases
    gradually over a period of a several years.

    The biggest changes in SQLAlchemy 2.0 are targeting the residual
    assumptions left over from this early period in SQLAlchemy's development as
    well as the leftover artifacts resulting from the incremental  introduction
    of key API features such as :class:`.orm.query.Query`  and Declarative.
    It also hopes standardize some newer capabilities that have proven to be
    very effective.

    Within each section below, please note that individual changes are still
    at differing degrees of certainty; some changes are definitely happening
    while others are not yet clear, and may change based on the results of
    further prototyping as well as community feedback.


SQLAlchemy 1.x to 2.0 Transition
================================

.. admonition:: Certainty: definite

    This change will proceed.

An extremely high priority of the SQLAlchemy 2.0 project is that transition
from the 1.x to 2.0 series will be as straightforward as possible.  The
strategy will allow for any application to move gradually towards a SQLAlchemy
2.0 model, first by running on Python 3 only, next running under SQLAlchemy 1.4
without deprecation warnings, and then by making use of SQLAlchemy 2.0-style
APIs that will be fully available in SQLAlchemy 1.4.

The steps to achieve this are as follows:

* All applications should ensure that they are fully ported to Python 3 and
  that Python 2 compatibility can be dropped.   This is the first prerequisite
  to moving towards 2.0.

* a significant portion of the internal architecture of SQLAlchemy 2.0
  is expected to be made available in SQLAlchemy 1.4.  It is hoped that
  features such as the rework of statement execution and transparent caching
  features, as well as deep refactorings of ``select()`` and ``Query()`` to
  fully support the new execution and caching model will be included, pending
  that continued prototyping of these features are successful. These new
  architectures will work within the SQLAlchemy 1.4 release transparently with
  little discernible effect, but will enable 2.0-style usage to be possible, as
  well as providing for the initial real-world adoption of the new
  architectures.

* A new deprecation class :class:`_exc.RemovedIn20Warning` is added, which
  subclasses :class:`_exc.SADeprecationWarning`.   Applications and their test
  suites can opt to enable or disable reporting of the
  :class:`_exc.RemovedIn20Warning` warning as needed, by setting the
  environment variable ``SQLALCHEMY_WARN_20=1`` **before** the program
  runs.   To some extent, the
  :class:`_exc.RemovedIn20Warning` deprecation class is analogous to the ``-3``
  flag available on Python 2 which reports on future Python 3
  incompatibilities.   See :ref:`deprecation_20_mode` for background
  on turning this on.

* APIs which emit :class:`_exc.RemovedIn20Warning` should always feature a new
  1.4-compatible usage pattern that applications can migrate towards.  This
  pattern will then be fully compatible with SQLAlchemy 2.0.   In this way,
  an application can gradually adjust all of its 1.4-style code to work fully
  against 2.0 as well.

* Currently, the main API which is explicitly incompatible with SQLAlchemy 1.x
  style is the behavior of the :class:`_engine.Engine` and
  :class:`_engine.Connection` objects in terms connectionless execution as well
  as "autocommit", in that the future API no longer has these behaviors, and
  two new methods :meth:`_future.Connection.commit` and
  :meth:`_future.Connection.rollback` are added in order to accommodate for
  commit-as-you-go use.  These new objects are currently in a separate package
  ``sqlalchemy.future``; in order to access the future versions of these, pass
  the parameter :paramref:`_engine.create_engine.future` to the
  :func:`_engine.create_engine` function.

* The :class:`_orm.Session` object also has a newer behavior when using the
  :meth:`_orm.Session.execute` method, in that incoming statements are
  interpreted in an ORM context if applicable, as well as that the
  :class:`_engine.Result` object returned uses new-style tuples
  (see :ref:`migration_20_result_rows`).    Within 1.4 this newer style
  is enabled by passing :paramref:`_orm.Session.future` to the session
  constructor or :class:`_orm.sessionmaker` object.

Python 3 Only
=============

.. admonition:: Certainty: definite

    This change will proceed.

At the top level, Python 2 is now retired in 2020, and new Python development
across the board is expected to be in Python 3.   SQLAlchemy will maintain
Python 2 support throughout the 1.4 series.  It is not yet decided if there
will be a 1.5 series as well and if this series would also continue to
support Python 2 or not.  However, SQLAlchemy 2.0 will be Python 3 only.

It is hoped that introduction of :pep:`484` may proceed from that point forward
over the course of subsequent major releases, including that SQLAlchemy's
source will be fully annotated, as well as that ORM level integrations for
:pep:`484` will be standard.  However, :pep:`484` integration is not a goal of
SQLAlchemy 2.0 itself, and support for this new system in full is expected
to occur over the course of many major releases.

.. _migration_20_autocommit:

Library-level (but not driver level) "Autocommit" removed from both Core and ORM
================================================================================

.. admonition:: Certainty: definite

  Review the new future API for engines and connections at:

    :class:`_future.Connection`

    :class:`.future.Engine`

    :func:`_future.create_engine`

  "autocommit" at the ORM level is already not a widely used pattern except to
  the degree that the ``.begin()`` call is desirable, and a new flag
  ``autobegin=False`` will suit that use case.  For Core, the "autocommit"
  pattern will lose most of its relevance as a result of "connectionless"
  execution going away as well, so once applications make sure they are
  checking out connections for their Core operations, they need only use
  ``engine.begin()`` instead of ``engine.connect()``, which is already the
  canonically documented pattern in the 1.x docs.   For true "autocommit", the
  "AUTOCOMMIT" isolation level remains available.

SQLAlchemy's first releases were at odds with the spirit of the Python
DBAPI (:pep:`249`) in that
it tried to hide :pep:`249`'s emphasis on "implicit begin" and "explicit commit"
of transactions.    Fifteen years later we now see this was essentially a
mistake, as SQLAlchemy's many patterns that attempt to "hide" the presence
of a transaction make for a more complex API which works inconsistently and
is extremely confusing to especially those users who are new to relational
databases and ACID transactions in general.   SQLAlchemy 2.0 will do away
with all attempts to implicitly commit transactions, and usage patterns
will always require that the user demarcate the "beginning" and the "end"
of a transaction in some way, in the same way as reading or writing to a file
in Python has a "beginning" and an "end".

In SQLAlchemy 1.x, the following statements will automatically commit
the underlying DBAPI transaction and then begin a new one, but in SQLAlchemy
2.0 this will not occur::

    conn = engine.connect()

    # won't autocommit in 2.0
    conn.execute(some_table.insert().values(foo='bar'))

Nor will this autocommit::

    conn = engine.connect()

    # won't autocommit in 2.0
    conn.execute(text("INSERT INTO table (foo) VALUES ('bar')"))

The options to force "autocommit" for specific connections or statements
are also removed::

    # "autocommit" execution option is removed in 2.0
    conn.execution_options(autocommit=True).execute(stmt)

    conn.execute(stmt.execution_options(autocommit=True))

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
:class:`_orm.Session` is normally used today, new ``.commit()`` and
``.rollback()`` methods will also be added to :class:`_engine.Connection` itself.
These will typically be used in conjunction with the :meth:`_engine.Engine.connect`
method::

    # 1.4 / 2.0 code

    from sqlalchemy.future import create_engine

    engine = create_engine(...)

    with engine.connect() as conn:
        conn.execute(some_table.insert().values(foo='bar'))
        conn.commit()

        conn.execute(text("some other SQL"))
        conn.rollback()

Above, the ``engine.connect()`` method will return a :class:`_engine.Connection` that
features **autobegin**, meaning the ``begin()`` event is emitted when the
execute method is first used (note however that there is no actual "BEGIN" in
the Python DBAPI).   This is the same as how the ORM :class:`.Session` will
work also and is not too dissimilar from how things work now.

For the ORM, the above patterns are already more or less how the
:class:`.Session` is used already::

    session = sessionmaker()

    session.add(<things>)

    session.execute(<things>)

    session.commit()


To complement the ``begin()`` use case of Core, the :class:`.Session` will
also include a new mode of operation called ``autobegin=False``, which is
intended to replace the ``autocommit=True`` mode. In this mode, the
:class:`.Session` will require that :meth:`.Session.begin` is called in order
to work with the database::

  # 1.4 / 2.0 code

  session = sessionmaker(autobegin=False)

  with session.begin():
      session.add(<things>)

The difference between ``autobegin=False`` and ``autocommit=True`` is that
the :class:`.Session` will not allow any database activity outside of the
above transaction block.  The 1.4 change :ref:`change_5074` is part of this
architecture.

In the case of both core :class:`_engine.Connection` as well as orm :class:`.Session`,
if neither ``.commit()`` nor ``.rollback()`` are called, the connection is
returned to the pool normally where an implicit (yes, still need this one)
rollback will occur.  This is the case already for Core and ORM::

    with engine.connect() as conn:
        results = conn.execute(text("select * from some_table"))
        return results

        # connection is returned to the pool, transaction is implicitly
        # rolled back.

    # or

    session = sessionmaker()
    results = session.execute(<some query>)

    # connection is returned to the pool, transaction is implicitly
    # rolled back.
    session.close()

Driver-level autocommit remains available
-----------------------------------------

Use cases for driver-level autocommit include some DDL patterns, particularly
on PostgreSQL, which require that autocommit mode at the database level is
set up.  Similarly, an "autocommit" mode can apply to an application that
is oriented in a per-statement style of organization and perhaps wants
statements individually handled by special proxy servers.

Because the Python DBAPI enforces a non-autocommit API by default, these
modes of operation can only be enabled by DBAPI-specific features that
re-enable autocommit.  SQLAlchemy allows this for backends that support
it using the "autocommit isolation level" setting.  Even though "autocommit"
is not technically a database isolation level, it effectively supersedes any
other isolation level; this concept was first inspired by the psycopg2 database
driver.

To use a connection in autocommit mode::

   with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
       conn.execute(text("CREATE DATABASE foobar"))


The above code is already available in current SQLAlchemy releases.   Driver
support is available for PostgreSQL, MySQL, SQL Server, and as of SQLAlchemy
1.3.16 Oracle and SQLite as well.

.. seealso::

    :ref:`dbapi_autocommit`

.. _migration_20_implicit_execution:

"Implicit" and "Connectionless" execution, "bound metadata" removed
====================================================================

.. admonition:: Certainty: definite

  The Core documentation has already standardized on the desired pattern here,
  so it is likely that most modern applications would not have to change
  much in any case, however there are probably a lot of apps that have
  a lot of ``engine.execute()`` calls that will need to be adjusted.

"Connectionless" execution refers to the still fairly popular pattern of
invoking ``.execute()`` from the :class:`_engine.Engine`::

  result = engine.execute(some_statement)

The above operation implicitly procures a :class:`_engine.Connection` object,
and runs the ``.execute()`` method on it.   This seems like a pretty simple
and intuitive method to have so that people who just need to invoke a few
SQL statements don't need all the verbosity with connecting and all that.

Fast forward fifteen years later and here is all that's wrong with that:

* Programs that feature extended strings of ``engine.execute()`` calls, for
  each statement getting a new connection from the connection pool (or
  perhaps making a new database connection if the pool is in heavy use),
  beginning a new transaction, invoking the statement, committing, returning
  the connection to the pool.  That is, the nuance that this was intended for
  a few ad-hoc statements but not industrial strength database operations
  is lost immediately.   New users are confused as to the difference between
  ``engine.execute()`` and ``connection.execute()``.   Too many choices are
  presented.

* The above technique relies upon the "autocommit" feature, in order to work
  as expected with any statement that implies a "write".   Since autocommit
  is already misleading, the above pattern is no longer feasible (the older
  "threadlocal" engine strategy which provided for begin/commit on the engine
  itself is also removed by SQLAlchemy 1.3).

* The above pattern returns a result which is not yet consumed.   So how
  exactly does the connection that was used for the statement, as well as the
  transaction necessarily begun for it, get handled, when there is still
  an active cursor ?    The answer is in multiple parts.  First off, the
  state of the cursor after the statement is invoked is inspected, to see if
  the statement in fact has results to return, that is, the ``cursor.description``
  attribute is non-None.   If not, we assume this is a DML or DDL statement,
  the cursor is closed immediately, and the result is returned after the
  connection is closed.  If there is a result, we leave the cursor and
  connection open, the :class:`_engine.ResultProxy` is then responsible for
  autoclosing the cursor when the results are fully exhausted, and at that
  point another special flag in the :class:`_engine.ResultProxy` indicates that the
  connection also needs to be returned to the pool.

That last one especially sounds crazy right?   That's why ``engine.execute()``
is going away.  It looks simple on the outside but it is unfortunately not,
and also, it's unnecessary and is frequently mis-used.  A whole series of
intricate "autoclose" logic within the :class:`_engine.ResultProxy` can be removed
when this happens.

With "connectionless" execution going away, we also take away a pattern that
is even more legacy, which is that of "implicit, connectionless" execution::

  result = some_statement.execute()

The above pattern has all the issues of "connectionless" execution, plus it
relies upon the "bound metadata" pattern, which SQLAlchemy has tried to
de-emphasize for many years.

Because implicit execution is removed, there's really no reason for "bound"
metadata to exist.  There are many internal structures that are involved with
locating the "bind" for a particular statement, to see if an :class:`_engine.Engine`
is associated with some SQL statement exists which necessarily involves an
additional traversal of the statement, just to find the correct dialect with
which to compile it.  This complex and error-prone logic can be removed from
Core by removing "bound" metadata.

Overall, the above executional patterns were introduced in SQLAlchemy's
very first 0.1 release before the :class:`_engine.Connection` object even existed.
After many years of de-emphasizing these patterns, "implicit, connectionless"
execution and "bound metadata" are no longer as widely used so in 2.0 we seek
to finally reduce the number of choices for how to execute a statement in
Core from "many"::

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

to "one"::

  # one choice!  (this works now!)

  with engine.begin() as conn:
      result = conn.execute(stmt)


  # OK one and a half choices (the commit() is 1.4 / 2.0 using future engine):

  with engine.connect() as conn:
      result = conn.execute(stmt)
      conn.commit()

Slight Caveat - there still may need to be a "statement.execute()" kind of feature
----------------------------------------------------------------------------------

.. admonition:: Certainty: tentative

  Things get a little tricky with "dynamic" ORM relationships as well as the
  patterns that Flask uses so we have to figure something out.

To suit the use case of ORM "dynamic" relationships as well as Flask-oriented
ORM patterns, there still may be some semblance of "implicit" execution of
a statement, however, it won't really be "connectionless".   Likely, a statement
can be directly bound to a :class:`_engine.Connection` or :class:`.Session` once
constructed::

  # 1.4 / 2.0 code (tentative)

  stmt = select(some_table).where(criteria)

  with engine.begin() as conn:
      stmt = stmt.invoke_with(conn)

      result = stmt.execute()

The above pattern, if we do it, will not be a prominently encouraged public
API; it will be used for particular extensions like "dynamic" relationships and
Flask-style queries only.

execute() method more strict, .execution_options() are available on ORM Session
================================================================================

.. admonition:: Certainty: definite

  Review the new future API for connections at:

    :class:`_future.Connection`


The use of execution options is expected to be more prominent as the Core and
ORM are largely unified at the statement handling level.   To suit this,
the :class:`_orm.Session` will be able to receive execution options local
to a series of statement executions in the same way as that of
:class:`_engine.Connection`::

    # 1.4 / 2.0 code

    session = Session()

    result = session.execution_options(stream_results=True).execute(stmt)

The calling signature for the ``.execute()`` method itself will work in
a "positional only" spirit, since :pep:`570` is only available in
Python 3.8 and SQLAlchemy will still support Python 3.6 and 3.7 for a little
longer.   The signature "in spirit" would be::

    # execute() signature once minimum version is Python 3.8
    def execute(self, statement, params=None, /, **options):

The interim signature will be::

    # 1.4 / 2.0 using sqlalchemy.future.create_engine,
    # sqlalchemy.future.orm.Session / sessionmaker / etc

    def execute(self, statement, _params=None, **options):

That is, by naming "``_params``" with an underscore we suggest that this
be passed positionally and not by name.

The ``**options`` keywords will be another way of passing execution options.
So that an execution may look like::

    # 1.4 / 2.0 future

    result = connection.execute(table.insert(), {"foo": "bar"}, isolation_level='AUTOCOMMIT')

    result = session.execute(stmt, stream_results=True)

.. _change_result_20_core:

ResultProxy replaced with Result which has more refined methods and behaviors
=============================================================================

.. admonition:: Certainty: definite

  Review the new future API for result sets:

    :class:`_engine.Result`


A major goal of SQLAlchemy 2.0 is to unify how "results" are handled between
the ORM and Core.   Towards this goal, version 1.4 will already standardized
both Core and ORM on a reworked notion of the ``RowProxy`` class, which
is now much more of a "named tuple"-like object.   Beyond that however,
SQLAlchemy 2.0 seeks to unify the means by which a set of rows is called
upon, where the more refined ORM-like methods ``.all()``, ``.one()`` and
``.first()`` will now also be how Core retrieves rows, replacing the
cursor-like ``.fetchall()``, ``.fetchone()`` methods.   The notion of
receiving "chunks" of a result at a time will be standardized across both
systems using a new method ``.partitions()`` which will behave similarly to
``.fetchmany()``, but will work in terms of iterators.

These new methods will be available from the "Result" object that is similar to
the existing "ResultProxy" object, but will be present both in Core and ORM
equally::

    # 1.4 / 2.0 with future create_engine

    from sqlalchemy.future import create_engine

    engine = create_engine(...)

    with engine.begin() as conn:
        stmt = table.insert()

        result = conn.execute(stmt)

        # Result against an INSERT DML
        result.inserted_primary_key

        stmt = select(table)

        result = conn.execute(stmt)  # statement is executed

        result.all()  # list
        result.one()  # first row, if doesn't exist or second row exists it raises
        result.one_or_none()  # first row or none, if second row exists it raises
        result.first()  # first row (warns if additional rows remain?)
        result  # iterator

        result.partitions(size=1000)  # partition result into iterator of lists of size N


        # limiting columns

        result.scalar()  # first col of first row  (warns if additional rows remain?)
        result.scalars()  # iterator of first col of each row
        result.scalars().all()  # same, as a list
        result.scalars(1)  # iterator of second col of each row
        result.scalars('a')  # iterator of the "a" col of each row

        result.columns('a', 'b').<anything>  # limit column tuples
        result.columns(table.c.a, table.c.b)  # using Column (or ORM attribute) objects

        result.columns('b', 'a')  # order is maintained

        # if the result is an ORM result, you could do:
        result.columns(User, Address)   # assuming these are available entities

        # or to get just User as a list
        result.scalars(User).all()

        # index access and slices ?
        result[0].all()  # same as result.scalars().all()
        result[2:5].all()  # same as result.columns('c', 'd', 'e').all()

.. _migration_20_result_rows:

Result rows unified between Core and ORM on named-tuple interface
==================================================================

Already part of 1.4, the previous ``KeyedTuple`` class that was used when
selecting rows from the :class:`_query.Query` object has been replaced by the
:class:`.Row` class, which is the base of the same :class:`.Row` that comes
back with Core statement results (in 1.4 it is the :class:`.LegacyRow` class).

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

Declarative becomes a first class API
=====================================

.. admonition:: Certainty: definite

  This is now committed in master and the new documenation can be seen at
  :ref:`orm_mapping_classes_toplevel`.

Declarative will now be part of ``sqlalchemy.orm`` in 2.0, and in 1.4 the
new version will be present in ``sqlalchemy.future.orm``.   The concept
of the ``Base`` class will be there as it is now and do the same thing
it already does, however it will also have some new capabilities.

.. seealso::

  :ref:`orm_mapping_classes_toplevel` - all new unified documentation for
  Declarative, classical mapping, dataclasses, attrs, etc.


The original "mapper()" function now a core element of Declarative, renamed
===========================================================================

.. admonition:: Certainty: definite

  This is now committed in master and the new documenation can be seen at
  :ref:`orm_mapping_classes_toplevel`.

By popular demand, "classical mapping" is staying around, however the new
form of it is based off of the :class:`_orm.registry` object and is available
as :meth:`_orm.registry.map_imperatively`.

In addition, the primary rationale used for "classical mapping" is that of
keeping the :class:`_schema.Table` setup distinct from the class.  Declarative
has always allowed this style using so-called
:ref:`hybrid declarative <orm_imperative_table_configuration>`. However,
to remove the base class requirement, a first class :ref:`decorator <declarative_config_toplevel>`
form has been added.

As yet another separate but related enhancement, support for :ref:`Python
dataclasses <orm_declarative_dataclasses>` is added as well to both
declarative decorator and classical mapping forms.

.. seealso::

  :ref:`orm_mapping_classes_toplevel` - all new unified documentation for
  Declarative, classical mapping, dataclasses, attrs, etc.

.. _migration_20_unify_select:

ORM Query Unified with Core Select
==================================

.. admonition:: Certainty: definite

    This is now implemented in 1.4.  The :class:`_orm.Query` object now
    generates a :class:`_sql.Select` object, which is then executed
    via :meth:`_orm.Session.execute`.  The API to instead use :class:`_sql.Select`
    and :meth:`_orm.Session.execute` directly, foregoing the usage of
    :class:`_orm.Query` altogether, is fully available in 1.4.   Most internal
    ORM systems for loading and refreshing objects has been transitioned to
    use :class:`_sql.Select` directly.

    The ``session.query(<cls>)`` pattern itself will likely **not** be fully
    removed.   As this pattern is extremely prevalent and numerous within any
    individual application, and that it does not intrinsically suggest an
    "antipattern" from a development standpoint, at the moment we are hoping
    that a transition to 2.0 won't require a rewrite of every ``session.query()``
    call, however it will be a legacy pattern that may warn as such.

Ever wonder why SQLAlchemy :func:`_expression.select` uses :meth:`_expression.Select.where` to add
a WHERE clause and :class:`_query.Query` uses :meth:`_query.Query.filter` ?   Same here!
The :class:`_query.Query` object was not part of SQLAlchemy's original concept.
Originally, the idea was that the :class:`_orm.Mapper` construct itself would
be able to select rows, and that :class:`_schema.Table` objects, not classes,
would be used to create the various criteria in a Core-style approach.   The
:class:`_query.Query` was basically an extension that was proposed by a user who
quite plainly had a better idea of how to build up SQL queries.   The
"buildable" approach of :class:`_query.Query`, originally called ``SelectResults``,
was also adapted to the Core SQL objects, so that :func:`_expression.select` gained
methods like :meth:`_expression.Select.where`, rather than being an all-at-once composed
object.  Later on, ORM classes gained the ability to be used directly in
constructing SQL criteria.    :class:`_query.Query` evolved over many years to
eventually support production of all the SQL that :func:`_expression.select` does, to
the point where having both forms has now become redundant.

SQLAlchemy 2.0 will resolve the inconsistency here by promoting the concept
of :func:`_expression.select` to be the single way that one constructs a SELECT construct.
For Core usage, the ``select()`` works mostly as it does now, except that it
gains a real working ``.join()`` method that will append JOIN conditions to the
statement in the same way as works for :meth:`_query.Query.join` right now.

For ORM use however, one can construct a :func:`_expression.select` using ORM objects, and
then when delivered to the ``.invoke()`` or ``.execute()`` method of
:class:`.Session`, it will be interpreted appropriately::

    from sqlalchemy.future import select
    stmt = select(User).join(User.addresses).where(Address.email == 'foo@bar.com')

    from sqlalchemy.future.orm import Session
    session = Session(some_engine)

    rows = session.execute(stmt).all()

Similarly, methods like :meth:`_query.Query.update` and :meth:`_query.Query.delete` are now
replaced by usage of the :func:`_expression.update` and :func:`_expression.delete` constructs directly::

    from sqlalchemy.future import update

    stmt = update(User).where(User.name == 'foo').values(name='bar')

    session.invoke(stmt).execution_options(synchronize_session=False).execute()

ORM Query relationship patterns simplified
==========================================

.. admonition:: Certainty: definite

  The patterns being removed here are enormously problematic internally,
  represent an older, obsolete way of doing things and the more advanced
  aspects of it are virtually never used

Joining / loading on relationships uses attributes, not strings
----------------------------------------------------------------

This refers to patterns such as that of :meth:`_query.Query.join` as well as
query options like :func:`_orm.joinedload` which currently accept a mixture of
string attribute names or actual class attributes.   The string calling form
leaves a lot more ambiguity and is also more complicated internally, so will
be deprecated in 1.4 and removed by 2.0.  This means the following won't work::

    q = select(User).join("addresses")

Instead, use the attribute::

    q = select(User).join(User.addresses)

Attributes are more explicit, such as if one were querying as follows::

    u1 = aliased(User)
    u2 = aliased(User)

    q = select(u1, u2).where(u1.id > u2.id).join(u1.addresses)

Above, the query knows that the join should be from the "u1" alias and
not "u2".

Similar changes will occur in all areas where strings are currently accepted::

    # removed
    q = select(User).options(joinedload("addresess"))

    # use instead
    q = select(User).options(joinedload(User.addresess))

    # removed
    q = select(Address).where(with_parent(u1, "addresses"))

    # use instead
    q = select(Address).where(with_parent(u1, User.addresses))

Chaining using lists of attributes, rather than individual calls, removed
--------------------------------------------------------------------------

"Chained" forms of joining and loader options which accept multiple mapped
attributes in a list will also be removed::

    # removed
    q = select(User).join("orders", "items", "keywords")

    # use instead
    q = select(User).join(User.orders).join(Order.items).join(Item.keywords)

.. _migration_20_query_join_options:

join(..., aliased=True), from_joinpoint removed
-----------------------------------------------

The ``aliased=True`` option on :meth:`_query.Query.join` is another feature that
seems to be almost never used, based on extensive code searches to find
actual use of this feature.   The internal complexity that the ``aliased=True``
flag requires is **enormous**, and will be going away in 2.0.

Since most users aren't familiar with this flag, it allows for automatic
aliasing of elements along a join, which then applies automatic aliasing
to filter conditions.  The original use case was to assist in long chains
of self-referential joins, such as::

  q = session.query(Node).\
    join("children", "children", aliased=True).\
    filter(Node.name == 'some sub child')

Where above, there would be two JOINs between three instances of the "node"
table assuming ``Node.children`` is a self-referential (e.g. adjacency list)
relationship to the ``Node`` class itself.    The "node" table would be aliased
at each step and the final ``filter()`` call would adapt itself to the last
"node" table in the chain.

It is this automatic adaption of the filter criteria that is enormously
complicated internally and almost never used in real world applications. The
above pattern also leads to issues such as if filter criteria need to be added
at each link in the chain; the pattern then must use the ``from_joinpoint``
flag which SQLAlchemy developers could absolutely find no occurrence of this
parameter ever being used in real world applications::

  q = session.query(Node).\
    join("children", aliased=True).filter(Node.name == 'some child').\
    join("children", aliased=True, from_joinpoint=True).\
    filter(Node.name == 'some sub child')

The ``aliased=True`` and ``from_joinpoint`` parameters were developed at a time
when the :class:`_query.Query` object didn't yet have good capabilities regarding
joining along relationship attributes, functions like
:meth:`.PropComparator.of_type` did not exist, and the :func:`.aliased`
construct itself didn't exist early on.

The above patterns are all suited by standard use of the :func:`.aliased`
construct, resulting in a much clearer query as well as removing hundreds of
lines of complexity from the internals of :class:`_query.Query` (or whatever it is
to be called in 2.0 :) ) ::

  n1 = aliased(Node)
  n2 = aliased(Node)
  q = select(Node).join(Node.children.of_type(n1)).\
      join(n1.children.of_type(n2)).\
      where(n1.name == "some child").\
      where(n2.name == "some sub child")

As was the case earlier, the ``.join()`` method will still allow arguments
of the form ``(target, onclause)`` as well::

  n1 = aliased(Node)
  n2 = aliased(Node)

  # still a little bit of "more than one way to do it" :)
  # but way better than before!   We'll be OK

  q = select(Node).join(n1, Node.children).\
      join(n2, n1.children).\
      where(n1.name == "some child").\
      where(n2.name == "some sub child")



By using attributes instead of strings above, the :meth:`_query.Query.join` method
no longer needs the almost never-used option of ``from_joinpoint``.

Other ORM Query patterns changed
=================================

This section will collect various :class:`_query.Query` patterns and how they work
in terms of :func:`_future.select`.

.. _migration_20_query_distinct:

Using DISTINCT with additional columns, but only select the entity
-------------------------------------------------------------------

:class:`_query.Query` will automatically add columns in the ORDER BY when
distinct is used.  The following query will select from all User columns
as well as "address.email_address" but only return User objects::

    # 1.xx code

    result = session.query(User).join(User.addresses).\
        distinct().order_by(Address.email_address).all()

Relational databases won't allow you to ORDER BY "address.email_address" if
it isn't also in the columns clause.   But the above query only wants "User"
objects back.  In 2.0, this very unusual use case is performed explicitly,
and the limiting of the entities/columns to ``User`` is done on the result::

    # 1.4/2.0 code

    from sqlalchemy.future import select

    stmt = select(User, Address.email_address).join(User.addresses).\
        distinct().order_by(Address.email_address)

    result = session.execute(stmt).scalars(User).all()

.. _migration_20_query_from_self:

Selecting from the query itself as a subquery, e.g. "from_self()"
-------------------------------------------------------------------

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

In SQLAlchemy 2.0, as the :func:`_future.select` construct will be expected
to handle every pattern the ORM :class:`_query.Query` does now, the pattern of
:meth:`_query.Query.from_self` can be invoked now by making use of the
:func:`_orm.aliased` function in conjunction with a subquery, that is
the :meth:`_query.Query.subquery` or :meth:`_expression.Select.subquery` method.    Version 1.4
of SQLAlchemy has enhanced the ability of the :func:`_orm.aliased` construct
to correctly extract columns from a given subquery.

Starting with a :meth:`_query.Query.from_self` query that selects from two different
entities, then converts itself to select just one of the entities from
a subquery::

  # 1.xx code

  q = session.query(User, Address.email_address).\
    join(User.addresses).\
    from_self(User).order_by(Address.email_address)

The above query SELECTS from "user" and "address", then applies a subquery
to SELECT only the "users" row but still with ORDER BY the email address
column::

  SELECT anon_1.user_id AS anon_1_user_id
  FROM (
    SELECT "user".id AS user_id, address.email_address AS address_email_address
    FROM "user" JOIN address ON "user".id = address.user_id
  ) AS anon_1 ORDER BY anon_1.address_email_address

The SQL query above illustrates the automatic translation of the "user" and
"address" tables in terms of the anonymously named subquery.

In 2.0, we perform these steps explicitly using :func:`_orm.aliased`::

  # 1.4/2.0 code

  from sqlalchemy.future import select
  from sqlalchemy.orm import aliased

  subq = select(User, Address.email_address).\
      join(User.addresses).subquery()

  # state the User and Address entities both in terms of the subquery
  ua = aliased(User, subq)
  aa = aliased(Address, subq)

  # then select using those entities
  stmt = select(ua).order_by(aa.email_address)
  result = session.execute(stmt)

The above query renders the identical SQL structure, but uses a more
succinct labeling scheme that doesn't pull in table names (that labeling
scheme is still available if the :meth:`_expression.Select.apply_labels` method is used)::

  SELECT anon_1.id AS anon_1_id
  FROM (
    SELECT "user".id AS id, address.email_address AS email_address
    FROM "user" JOIN address ON "user".id = address.user_id
  ) AS anon_1 ORDER BY anon_1.email_address

SQLAlchemy 1.4 features improved disambiguation of columns in subqueries,
so even if our ``User`` and ``Address`` entities have overlapping column names,
we can select from both entities at once without having to specify any
particular labeling::

  # 1.4/2.0 code

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


Transparent Statement Compilation Caching replaces "Baked" queries, works in Core
==================================================================================

.. admonition:: Certainty: definite

  This is now implemented in 1.4.   The migration notes at :ref:`change_4639`
  detail the change.

A major restructuring of the Core internals as well as of that of the ORM
:class:`_query.Query` will be reorganizing the major statement objects to have very
simplified "builder" internals, that is, when you construct an object like
``select(table).where(criteria).join(some_table)``, the arguments passed are
simply stored and as little processing as possible will occur.   Then there is
a new mechanism by which a cache key can be generated from all of the state
passed into the object at this point.   The Core execution system will make use
of this cache key when seeking to compile a statement, using a pre-compiled
object if one is available. If a compiled object needs to be constructed, the
additional work of interpreting things like the "where" clause, interpreting
``.join()``, etc. into SQL elements will occur at this point, in contrast to the
1.3.x and earlier series of SQLAlchemy and earlier where it occurs during
construction.

The Core execution system will also initiate this same task on behalf of the
"ORM" version of ``select()``; the "post-construction" worker is pluggable,
so in the context of the ORM, an object similar to the :class:`.QueryContext`
will perform this work.   While :class:`.QueryContext` is currently invoked
when one emits a call like ``query.all()``, constructing a ``select()``
object which is passed to the Core for execution, the new flow will be that
the ``select()`` object that was built up with ORM state will be sent to Core,
where the "post-construction" task invoked when no cached object is
present will invoke :class:`.QueryContext` which then processes all the
state of the ``select()`` in terms of the ORM, and then invokes it
like any other Core statement.  A similar "pre-result" step is associated
with the execution which is where the plain result rows will be filtered
into ORM rows.

This is in contrast to the 1.3.x and earlier series of SQLAlchemy where the
"post-construction" of the query and "pre-result" steps are instead
"pre-execution" and  "post-result", that is, they occur outside of where Core
would be able to  cache the results of the work performed.   The new
architecture integrates the work done by the ORM into a new flow supported by
Core.

To complete the above system, a new "lambda" based SQL construction system will
also be added, so that construction of ``select()`` and other constructs is
even faster outside of that which is cached; this "lambda" based system is
based on a similar concept as that of the "baked" query but is more
sophisticated and refined so that it is easier to use.   It also will be
completely optional, as the caching will still work without the use of lambda
constructs.

All SQLAlchemy applications will have access to a large portion of the
performance gains that are offered by the "baked" query system now, and it will
apply to all statements, Core / ORM, select/insert/update/delete/other, and
it will be fully transparent.   Applications that wish to reduce statement
building latency even further to the levels currently offered by the "baked"
system can opt to use the "lambda" constructs.

.. _joinedload_not_uniqued:

ORM Rows not uniquified by default
===================================

.. admonition:: Certainty: likely

    This is now partially implemented for the :term:`2.0 style` use of ORM
    queries, in that rows are not automatically uniquified unless unique() is
    called. However we have yet to receive user feedback (or
    complaints) on this change.

ORM rows returned by ``session.execute(stmt)`` are no longer automatically
"uniqued"; this must be called explicitly::

    # 1.4 / 2.0 code

    stmt = select(User).options(joinedload(User.addresses))

    # statement will raise if unique() is not used, due to joinedload()
    # of a collection.  in all other cases, unique() is not needed
    rows = session.invoke(stmt).unique().execute().all()

This includes when joined eager loading with collections is used.  It is
advised that for eager loading of collections, "selectin" loading is used
instead.   When collections that are set up to load as joined eager are present
and ``unique()`` is not used, an exception is raised, as this will produce many
duplicate rows and is not what the user intends.   Joined eager loading of
many-to-one relationships does not present any issue, however.

This change will also end the ancient issue of users being confused why
``session.query(User).join(User.addresses).count()`` returns a different number
than that of ``session.query(User).join(User.addresses).all()``.  The results
will now be the same.


Tuples, Scalars, single-row results with ORM / Core results made consistent
============================================================================

.. admonition:: Certainty: likely

    This is also implemented for :term:`2.0 style` ORM use however we don't
    have user feedback yet.

The :meth:`.future.Result.all` method now delivers named-tuple results
in all cases, even for an ORM select that is against a single entity.   This
is for consistency in the return type.

TODO description::

    # iterator
    for user in session.execute(stmt).scalars():

TODO description::

    users = session.execute(stmt).scalars().all()

TODO description::

    # first() no longer applies a limit
    users = session.execute(stmt.limit(1)).first()


    # first() when there are rows remaining warns
    users = session.execute(stmt).first()
    Warning: additional rows discarded; apply .limit(1) to the statement when
    using first()

How Do Magic Flask patterns etc work?!?!
-----------------------------------------

.. admonition:: Certainty: tentative

  This is where the "remove Query and replace with
  ``session.execute(select(User))``" pattern starts to hit a lot of friction,
  so there may still have to be some older-style patterns in place.  it's not
  clear if the ``.execute()`` step will be required, for example.


::

    session = scoped_session(...)

    class User(magic_flask_thing_that_links_to_scoped_session):
      # ...


    # old:

    users = User.query.filter(User.name.like('%foo%')).all()

    # new:

    <drumroll>

    users = User.select.where(User.name.like('%foo%')).execute().all()

Above, we backtrack slightly on the "implicit execution removed" aspect,
where Flask will be able to bind a query / select to the current Session.

Same thing with lazy=dynamic....
---------------------------------

The same pattern is needed for "dynamic" relationships::

    user.addresses.where(Address.id > 10).execute().all()


Asyncio Support
=====================

.. admonition:: Certainty: definite

  This is now implemented in 1.4.

There was previously an entire section here detailing how asyncio is a nice to
have, but not really necessary from a technical standpoint, there are some
approaches already, and maybe third parties can keep doing it.

What's changed is that there is now an approach to doing this in SQLAlchemy
directly that does not impact the existing library internals nor does it imply
an entirely separate version of everything be maintained, therefore this makes
it feasible to deliver this feature to those users who prefer an all-async
application style without impact on the traditional blocking archictecture.

SQLAlchemy 1.4 now includes full asyncio capability with initial support
using the :ref:`dialect-postgresql-asyncpg` Python database driver;
see :ref:`asyncio_toplevel`.

