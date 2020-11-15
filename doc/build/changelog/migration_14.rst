.. _migration_14_toplevel:

=============================
What's New in SQLAlchemy 1.4?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.3
    and SQLAlchemy version 1.4.

    Version 1.4 is taking on a different focus than other SQLAlchemy releases
    in that it is in many ways attempting to serve as a potential migration
    point for a more dramatic series of API changes currently planned for
    release  2.0 of SQLAlchemy.   The focus of SQLAlchemy 2.0 is a modernized
    and slimmed down API that removes lots of usage patterns that have long
    been discouraged, as well as mainstreams the best ideas in SQLAlchemy as
    first class API features, with the goal being that there is much less
    ambiguity in how the API is to be used, as well as that a series of
    implicit behaviors and rarely-used API flags that complicate the internals
    and hinder performance will be removed.

    For the current status of SQLAlchemy 2.0, see :ref:`migration_20_toplevel`.

Major API changes and features - General
=========================================

.. _change_5634:

Python 3.6 is the minimum Python 3 version; Python 2.7 still supported
----------------------------------------------------------------------

As Python 3.5 reached EOL in September of 2020, SQLAlchemy 1.4 now places
version 3.6 as the minimum Python 3 version.  Python 2.7 is still supported,
however the SQLAlchemy 1.4 series will be the last series to support Python 2.


.. _change_5159:

ORM Query is internally unified with select, update, delete; 2.0 style execution available
------------------------------------------------------------------------------------------

The biggest conceptual change to SQLAlchemy for version 2.0 and essentially
in 1.4 as well is that the great separation between the :class:`_sql.Select`
construct in Core and the :class:`_orm.Query` object in the ORM has been removed,
as well as between the :meth:`_orm.Query.update` and :meth:`_orm.Query.delete`
methods in how they relate to :class:`_dml.Update` and :class:`_dml.Delete`.

With regards to :class:`_sql.Select` and :class:`_orm.Query`, these two objects
have for many versions had similar, largely overlapping APIs and even some
ability to change between one and the other, while remaining very different in
their usage patterns and behaviors.   The historical background for this was
that the :class:`_orm.Query` object was introduced to overcome shortcomings in
the :class:`_sql.Select` object which used to be at the core of how ORM objects
were queried, except that they had to be queried in terms of
:class:`_schema.Table` metadata only.    However :class:`_orm.Query` had only a
simplistic interface for loading objects, and only over the course of many
major releases did it eventually gain most of the flexibility of the
:class:`_sql.Select` object, which then led to the ongoing awkwardness that
these two objects became highly similar yet still largely incompatible with
each other.

In version 1.4, all Core and ORM SELECT statements are rendered from a
:class:`_sql.Select` object directly; when the :class:`_orm.Query` object
is used, at statement invocation time it copies its state to a :class:`_sql.Select`
which is then invoked internally using :term:`2.0 style` execution.   Going forward,
the :class:`_orm.Query` object will become legacy only, and applications will
be encouraged to move to :term:`2.0 style` execution which allows Core constructs
to be used freely against ORM entities::

    with Session(engine, future=True) as sess:

        stmt = select(User).where(
            User.name == 'sandy'
        ).join(User.addresses).where(Address.email_address.like("%gmail%"))

        result = sess.execute(stmt)

        for user in result.scalars():
            print(user)

Things to note about the above example:

* The :class:`_orm.Session` and :class:`_orm.sessionmaker` objects now feature
  full context manager (i.e. the ``with:`` statement) capability;
  see the revised documentation at :ref:`session_getting` for an example.

* Within the 1.4 series, all :term:`2.0 style` ORM invocation uses a
  :class:`_orm.Session` that includes the :paramref:`_orm.Session.future`
  flag set to ``True``; this flag indicates the :class:`_orm.Session` should
  have 2.0-style behaviors, which include that ORM queries can be invoked
  from :class:`_orm.Session.execute` as well as some changes in transactional
  features.   In version 2.0 this flag will always be ``True``.

* The :func:`_sql.select` construct no longer needs brackets around the
  columns clause; see :ref:`change_5284` for background on this improvement.

* The :func:`_sql.select`  / :class:`_sql.Select` object has a :meth:`_sql.Select.join`
  method that acts like that of the :class:`_orm.Query` and even accommodates
  an ORM relationship attribute (without breaking the separation between
  Core and ORM!) - see :ref:`change_select_join` for background on this.

* Statements that work with ORM entities and are expected to return ORM
  results are invoked using :meth:`.orm.Session.execute`.  See
  :ref:`session_querying_20` for a primer.

* a :class:`_engine.Result` object is returned, rather than a plain list, which
  itself is a much more sophisticated version of the previous ``ResultProxy``
  object; this object is now used both for Core and ORM results.   See
  :ref:`change_result_14_core`,
  :ref:`change_4710_core`, and :ref:`change_4710_orm` for information on this.

Throughout SQLAlchemy's documentation, there will be many references to
:term:`1.x style` and :term:`2.0 style` execution.  This is to distinguish
between the two querying styles and to attempt to forwards-document the new
calling style going forward.  In SQLAlchemy 2.0, while the :class:`_orm.Query`
object may remain as a legacy construct, it will no longer be featured in
most documentation.

Similar adjustments have been made to "bulk updates and deletes" such that
Core :func:`_sql.update` and :func:`_sql.delete` can be used for bulk
operations.   A bulk update like the following::

    session.query(User).filter(User.name == 'sandy').update({"password": "foobar"}, synchronize_session="fetch")

can now be achieved in :term:`2.0 style` (and indeed the above runs internally
in this way) as follows::

    with Session(engine, future=True) as sess:
        stmt = update(User).where(
            User.name == 'sandy'
        ).values(password="foobar").execution_options(
            synchronize_session="fetch"
        )

        sess.execute(stmt)

Note the use of the :meth:`_sql.Executable.execution_options` method to pass
ORM-related options.  The use of "execution options" is now much more prevalent
within both Core and ORM, and many ORM-related methods from :class:`_orm.Query`
are now implemented as execution options (see :meth:`_orm.Query.execution_options`
for some examples).

.. seealso::

    :ref:`migration_20_toplevel`

:ticket:`5159`

.. _change_4639:

Transparent SQL Compilation Caching added to All DQL, DML Statements in Core, ORM
----------------------------------------------------------------------------------

One of the most broadly encompassing changes to ever land in a single
SQLAlchemy version, a many-month reorganization and refactoring of all querying
systems from the base of Core all the way through ORM now allows the
majority of Python computation involved producing SQL strings and related
statement metadata from a user-constructed statement to be cached in memory,
such that subsequent invocations of an identical statement construct will use
35-60% fewer CPU resources.

This caching goes beyond the construction of the SQL string to also include the
construction of result fetching structures that link the SQL construct to the
result set, and in the ORM it includes the accommodation of ORM-enabled
attribute loaders, relationship eager loaders and other options, and object
construction routines that must be built up each time an ORM query seeks to run
and construct ORM objects from result sets.

To introduce the general idea of the feature, given code from the
:ref:`examples_performance` suite as follows, which will invoke
a very simple query "n" times, for a default value of n=10000.   The
query returns only a single row, as the overhead we are looking to decrease
is that of **many small queries**.    The optimization is not as significant
for queries that return many rows::

    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        result = session.query(Customer).filter(Customer.id == id_).one()

This example in the 1.3 release of SQLAlchemy on a Dell XPS13 running Linux
completes as follows::

    test_orm_query : (10000 iterations); total time 3.440652 sec

In 1.4, the code above without modification completes::

    test_orm_query : (10000 iterations); total time 2.367934 sec

This first test indicates that regular ORM queries when using caching can run
over many iterations in the range of **30% faster**.

A second variant of the feature is the optional use of Python lambdas to defer
the construction of the query itself.  This is a more sophisticated variant of
the approach used by the "Baked Query" extension, which was introduced in
version 1.0.0.     The "lambda" feature may be used in a style very similar to
that of baked queries, except that it is available in an ad-hoc way for any SQL
construct.  It additionally includes the ability to scan each invocation of the
lambda for bound literal values that change on every invocation, as well as
changes to other constructs, such as querying from a different entity or column
each time, while still not having to run the actual code each time.

Using this API looks as follows::

    session = Session(bind=engine)
    for id_ in random.sample(ids, n):
        stmt = lambda_stmt(lambda: future_select(Customer))
        stmt += lambda s: s.where(Customer.id == id_)
        session.execute(stmt).scalar_one()

The code above completes::

    test_orm_query_newstyle_w_lambdas : (10000 iterations); total time 1.247092 sec

This test indicates that using the newer "select()" style of ORM querying,
in conjunction with a full "baked" style invocation that caches the entire
construction, can run over many iterations in the range of **60% faster** and
grants performance about the same as the baked query system which is now superseded
by the native caching system.

The new system makes use of the existing
:paramref:`_engine.Connection.execution_options.compiled_cache` execution
option and also adds a cache to the :class:`_engine.Engine` directly, which is
configured using the :paramref:`_engine.Engine.query_cache_size` parameter.

A significant portion of API and behavioral changes throughout 1.4 were
driven in order to support this new feature.

.. seealso::

    :ref:`sql_caching`

:ticket:`4639`
:ticket:`5380`
:ticket:`4645`
:ticket:`4808`
:ticket:`5004`

.. _change_5508:

Declarative is now integrated into the ORM with new features
-------------------------------------------------------------

After ten years or so of popularity, the ``sqlalchemy.ext.declarative``
package is now integrated into the ``sqlalchemy.orm`` namespace, with the
exception of the declarative "extension" classes which remain as Declarative
extensions.

The new classes added to ``sqlalchemy.orm`` include:

* :class:`_orm.registry` - a new class that supersedes the role of the
  "declarative base" class, serving as a registry of mapped classes which
  can be referenced via string name within :func:`_orm.relationship` calls
  and is agnostic of the style in which any particular class was mapped.

* :func:`_orm.declarative_base` - this is the same declarative base class that
  has been in use throughout the span of the declarative system, except it now
  references a :class:`_orm.registry` object internally and is implemented
  by the :meth:`_orm.registry.generate_base` method which can be invoked
  from a :class:`_orm.registry` directly.   The :func:`_orm.declarative_base`
  function creates this registry automatically so there is no impact on
  existing code.    The ``sqlalchemy.ext.declarative.declarative_base`` name
  is still present, emitting a 2.0 deprecation warning when
  :ref:`2.0 deprecations mode <deprecation_20_mode>` is enabled.

* :func:`_orm.declared_attr` - the same "declared attr" function call now
  part of ``sqlalchemy.orm``.  The ``sqlalchemy.ext.declarative.declared_attr``
  name is still present, emitting a 2.0 deprecation warning when
  :ref:`2.0 deprecations mode <deprecation_20_mode>` is enabled.

* Other names moved into ``sqlalchemy.orm`` include :func:`_orm.has_inherited_table`,
  :func:`_orm.synonym_for`, :class:`_orm.DeclarativeMeta`, :func:`_orm.as_declarative`.

In addition, The :func:`_declarative.instrument_declarative` function is
deprecated, superseded by :meth:`_orm.registry.map_declaratively`.  The
:class:`_declarative.ConcreteBase`, :class:`_declarative.AbstractConcreteBase`,
and :class:`_declarative.DeferredReflection` classes remain as extensions in the
:ref:`declarative_toplevel` package.

Mapping styles have now been organized such that they all extend from
the :class:`_orm.registry` object, and fall into these categories:

* :ref:`orm_declarative_mapping`
    * Using :func:`_orm.declarative_base` Base class w/ metaclass
        * :ref:`orm_declarative_table`
        * :ref:`Imperative Table (a.k.a. "hybrid table") <orm_imperative_table_configuration>`
    * Using :meth:`_orm.registry.mapped` Declarative Decorator
        * Declarative Table
        * Imperative Table (Hybrid)
            * :ref:`orm_declarative_dataclasses`
* :ref:`Imperative (a.k.a. "classical" mapping) <classical_mapping>`
    * Using :meth:`_orm.registry.map_imperatively`
        * :ref:`orm_imperative_dataclasses`

The existing classical mapping function :func:`_orm.mapper` remains, however
it is deprecated to call upon :func:`_orm.mapper` directly; the new
:meth:`_orm.registry.map_imperatively` method now routes the request through
the :meth:`_orm.registry` so that it integrates with other declarative mappings
unambiguously.

The new approach interoperates with 3rd party class instrumentation systems
which necessarily must take place on the class before the mapping process
does, allowing declarative mapping to work via a decorator instead of a
declarative base so that packages like dataclasses_ and attrs_ can be
used with declarative mappings, in addition to working with classical
mappings.

Declarative documentation has now been fully integrated into the ORM mapper
configuration documentation and includes examples for all styles of mappings
organized into one place. See the section
:ref:`orm_mapping_classes_toplevel` for the start of the newly reorganized
documentation.

.. _dataclasses: https://docs.python.org/3/library/dataclasses.html
.. _attrs: https://pypi.org/project/attrs/

.. seealso::

  :ref:`orm_mapping_classes_toplevel`

  :ref:`change_5027`

:ticket:`5508`


.. _change_5027:

Python Dataclasses, attrs Supported w/ Declarative, Imperative Mappings
-----------------------------------------------------------------------

Along with the new declarative decorator styles introduced in :ref:`change_5508`,
the :class:`_orm.Mapper` is now explicitly aware of the Python ``dataclasses``
module and will recognize attributes that are configured in this way, and
proceed to map them without skipping them as was the case previously.  In the
case of the ``attrs`` module, ``attrs`` already removes its own attributes
from the class so was already compatible with SQLAlchemy classical mappings.
With the addition of the :meth:`_orm.registry.mapped` decorator, both
attribute systems can now interoperate with Declarative mappings as well.

.. seealso::

  :ref:`orm_declarative_dataclasses`

  :ref:`orm_imperative_dataclasses`


:ticket:`5027`


.. _change_3414:

Asynchronous IO Support for Core and ORM
------------------------------------------

SQLAlchemy now supports Python ``asyncio``-compatible database drivers using an
all-new asyncio front-end interface to :class:`_engine.Connection` for Core
usage as well as :class:`_orm.Session` for ORM use, using the
:class:`_asyncio.AsyncConnection` and :class:`_asyncio.AsyncSession` objects.

.. note::  The new asyncio feature should be considered **alpha level** for
   the initial releases of SQLAlchemy 1.4.   This is super new stuff that uses
   some previously unfamiliar programming techniques.

The initial database API supported is the :ref:`dialect-postgresql-asyncpg`
asyncio driver for PostgreSQL.

The internal features of SQLAlchemy are fully integrated by making use of
the `greenlet <https://greenlet.readthedocs.io/en/latest/>`_ library in order
to adapt the flow of execution within SQLAlchemy's internals to propagate
asyncio ``await`` keywords outwards from the database driver to the end-user
API, which features ``async`` methods.  Using this approach, the asyncpg
driver is fully operational within SQLAlchemy's own test suite and features
compatibility with most psycopg2 features.   The approach was vetted and
improved upon by developers of the greenlet project for which SQLAlchemy
is appreciative.

.. sidebar:: greenlets are good

  Don't confuse the greenlet_ library with event-based IO libraries that build
  on top of it such as ``gevent`` and ``eventlet``; while the use of these
  libraries with SQLAlchemy is common, SQLAlchemy's asyncio integration
  **does not** make use of these event based systems in any way. The asyncio
  API integrates with the user-provided event loop, typically Python's own
  asyncio event loop, without the use of additional threads or event systems.
  The approach involves a single greenlet context switch per ``await`` call,
  and the extension which makes it possible is less than 20 lines of code.

The user facing ``async`` API itself is focused around IO-oriented methods such
as :meth:`_asyncio.AsyncEngine.connect` and
:meth:`_asyncio.AsyncConnection.execute`.   The new Core constructs strictly
support :term:`2.0 style` usage only; which means all statements must be
invoked given a connection object, in this case
:class:`_asyncio.AsyncConnection`.

Within the ORM, :term:`2.0 style` query execution is
supported, using :func:`_sql.select` constructs in conjunction with
:meth:`_asyncio.AsyncSession.execute`; the legacy :class:`_orm.Query`
object itself is not supported by the :class:`_asyncio.AsyncSession` class.

ORM features such as lazy loading of related attributes as well as unexpiry of
expired attributes are by definition disallowed in the traditional asyncio
programming model, as they indicate IO operations that would run implicitly
within the scope of a Python ``getattr()`` operation.   To overcome this, the
**traditional** asyncio application should make judicious use of :ref:`eager
loading <loading_toplevel>` techniques as well as forego the use of features
such as :ref:`expire on commit <session_committing>` so that such loads are not
needed.

For the asyncio application developer who **chooses to break** with
tradition, the new API provides a **strictly optional
feature** such that applications that wish to make use of such ORM features
can opt to organize database-related code into functions which can then be
run within greenlets using the :meth:`_asyncio.AsyncSession.run_sync`
method. See the ``greenlet_orm.py`` example at :ref:`examples_asyncio`
for a demonstration.

Support for asynchronous cursors is also provided using new methods
:meth:`_asyncio.AsyncConnection.stream` and
:meth:`_asyncio.AsyncSession.stream`, which support a new
:class:`_asyncio.AsyncResult` object that itself provides awaitable
versions of common methods like
:meth:`_asyncio.AsyncResult.all` and
:meth:`_asyncio.AsyncResult.fetchmany`.   Both Core and ORM are integrated
with the feature which corresponds to the use of "server side cursors"
in traditional SQLAlchemy.

.. seealso::

  :ref:`asyncio_toplevel`

  :ref:`examples_asyncio`



:ticket:`3414`

.. _change_deferred_construction:


Many Core and ORM statement objects now perform much of their construction and validation in the compile phase
--------------------------------------------------------------------------------------------------------------

A major initiative in the 1.4 series is to approach the model of both Core SQL
statements as well as the ORM Query to allow for an efficient, cacheable model
of statement creation and compilation, where the compilation step would be
cached, based on a cache key generated by the created statement object, which
itself is newly created for each use.  Towards this goal, much of the Python
computation which occurs within the construction of statements, particularly
that of the ORM :class:`_query.Query` as well as the :func:`_sql.select`
construct when used to invoke ORM queries, is being moved to occur within
the compilation phase of the statement which only occurs after the statement
has been invoked, and only if the statement's compiled form was not yet
cached.

From an end-user perspective, this means that some of the error messages which
can arise based on arguments passed to the object will no longer be raised
immediately, and instead will occur only when the statement is invoked for
the first time.    These conditions are always structural and not data driven,
so there is no risk of such a condition being missed due to a cached statement.

Error conditions which fall under this category include:

* when a :class:`_selectable.CompoundSelect` is constructed (e.g. a UNION, EXCEPT, etc.)
  and the SELECT statements passed do not have the same number of columns, a
  :class:`.CompileError` is now raised to this effect; previously, an
  :class:`.ArgumentError` would be raised immediately upon statement
  construction.

* Various error conditions which may arise when calling upon :meth:`.Query.join`
  will be evaluated at statement compilation time rather than when the method
  is first called.

.. seealso::

    :ref:`change_4639`

.. _change_4656:

Repaired internal importing conventions such that code linters may work correctly
---------------------------------------------------------------------------------

SQLAlchemy has for a long time used a parameter-injecting decorator to help resolve
mutually-dependent module imports, like this::

    @util.dependency_for("sqlalchemy.sql.dml")
    def insert(self, dml, *args, **kw):

Where the above function would be rewritten to no longer have the ``dml`` parameter
on the outside.  This would confuse code-linting tools into seeing a missing parameter
to functions.  A new approach has been implemented internally such that the function's
signature is no longer modified and the module object is procured inside the function
instead.


:ticket:`4656`

:ticket:`4689`


.. _change_1390:

Support for SQL Regular Expression operators
--------------------------------------------

A long awaited feature to add rudimentary support for database regular
expression operators, to complement the :meth:`_sql.ColumnOperators.like` and
:meth:`_sql.ColumnOperators.match` suites of operations.   The new features
include :meth:`_sql.ColumnOperators.regexp_match` implementing a regular
expression match like function, and :meth:`_sql.ColumnOperators.regexp_replace`
implementing a regular expression string replace function.

Supported backends include SQLite, PostgreSQL, MySQL / MariaDB, and Oracle.
The SQLite backend only supports "regexp_match" but not "regexp_replace".

The regular expression syntaxes and flags are **not backend agnostic**.
A future feature will allow multiple regular expression syntaxes to be
specified at once to switch between different backends on the fly.

For SQLite, Python's ``re.match()`` function with no additional arguments
is established as the implementation.

.. seealso::


    :meth:`_sql.ColumnOperators.regexp_match`

    :meth:`_sql.ColumnOperators.regexp_replace`

    :ref:`pysqlite_regexp` - SQLite implementation notes


:ticket:`1390`


.. _deprecation_20_mode:

SQLAlchemy 2.0 Deprecations Mode
---------------------------------

One of the primary goals of the 1.4 release is to provide a "transitional"
release so that applications may migrate to SQLAlchemy 2.0 gradually.   Towards
this end, a primary feature in release 1.4 is "2.0 deprecations mode", which is
a series of deprecation warnings that emit against every detectable API pattern
which will work differently in version 2.0.   The warnings all make use of the
:class:`_exc.RemovedIn20Warning` class. As these warnings affect foundational
patterns including the :func:`_sql.select` and :class:`_engine.Engine` constructs, even
simple applications can generate a lot of warnings until appropriate API
changes are made.   The warning mode is therefore turned off by default until
the developer enables the environment variable ``SQLALCHEMY_WARN_20=1``.

For a full walkthrough of using 2.0 Deprecations mode, see :ref:`migration_20_deprecations_mode`.

.. seealso::

  :ref:`migration_20_toplevel`

  :ref:`migration_20_deprecations_mode`



API and Behavioral Changes - Core
==================================

.. _change_4617:

A SELECT statement is no longer implicitly considered to be a FROM clause
--------------------------------------------------------------------------

This change is one of the larger conceptual changes in SQLAlchemy in many years,
however it is hoped that the end user impact is relatively small, as the change
more closely matches what databases like MySQL and PostgreSQL require in any case.

The most immediate noticeable impact is that a :func:`_expression.select` can no longer
be embedded inside of another :func:`_expression.select` directly, without explicitly
turning the inner :func:`_expression.select` into a subquery first.  This is historically
performed by using the :meth:`_expression.SelectBase.alias` method, which remains, however
is more explicitly suited by using a new method :meth:`_expression.SelectBase.subquery`;
both methods do the same thing.   The object returned is now :class:`.Subquery`,
which is very similar to the :class:`_expression.Alias` object and shares a common
base :class:`.AliasedReturnsRows`.

That is, this will now raise::

    stmt1 = select(user.c.id, user.c.name)
    stmt2 = select(addresses, stmt1).select_from(addresses.join(stmt1))

Raising::

    sqlalchemy.exc.ArgumentError: Column expression or FROM clause expected,
    got <...Select object ...>. To create a FROM clause from a <class
    'sqlalchemy.sql.selectable.Select'> object, use the .subquery() method.

The correct calling form is instead (noting also that :ref:`brackets are no
longer required for select() <change_5284>`)::

    sq1 = select(user.c.id, user.c.name).subquery()
    stmt2 = select(addresses, sq1).select_from(addresses.join(sq1))

Noting above that the :meth:`_expression.SelectBase.subquery` method is essentially
equivalent to using the :meth:`_expression.SelectBase.alias` method.


The rationale for this change is based on the following:

* In order to support the unification of :class:`_sql.Select` with
  :class:`_orm.Query`, the :class:`_sql.Select` object needs to have
  :meth:`_sql.Select.join` and :meth:`_sql.Select.outerjoin` methods that
  actually add JOIN criteria to the existing FROM clause, as is what users have
  always expected it to do in any case.    The previous behavior, having to
  align with what a :class:`.FromClause` would do, was that it would generate
  an unnamed subquery and then JOIN to it, which was a completely useless
  feature that only confused those users unfortunate enough to try this.  This
  change is discussed at :ref:`change_select_join`.

* The behavior of including a SELECT in the FROM clause of another SELECT
  without first creating an alias or subquery would be that it creates an
  unnamed subquery.   While standard SQL does support this syntax, in practice
  it is rejected by most databases.  For example, both the MySQL and PostgreSQL
  outright reject the usage of unnamed subqueries::

      # MySQL / MariaDB:

      MariaDB [(none)]> select * from (select 1);
      ERROR 1248 (42000): Every derived table must have its own alias


      # PostgreSQL:

      test=> select * from (select 1);
      ERROR:  subquery in FROM must have an alias
      LINE 1: select * from (select 1);
                            ^
      HINT:  For example, FROM (SELECT ...) [AS] foo.

  A database like SQLite accepts them, however it is still often the case that
  the names produced from such a subquery are too ambiguous to be useful::

      sqlite> CREATE TABLE a(id integer);
      sqlite> CREATE TABLE b(id integer);
      sqlite> SELECT * FROM a JOIN (SELECT * FROM b) ON a.id=id;
      Error: ambiguous column name: id
      sqlite> SELECT * FROM a JOIN (SELECT * FROM b) ON a.id=b.id;
      Error: no such column: b.id

      # use a name
      sqlite> SELECT * FROM a JOIN (SELECT * FROM b) AS anon_1 ON a.id=anon_1.id;

  ..

As :class:`_expression.SelectBase` objects are no longer
:class:`_expression.FromClause` objects, attributes like the ``.c`` attribute
as well as methods like ``.select()`` is now deprecated, as they imply implicit
production of a subquery. The ``.join()`` and ``.outerjoin()`` methods are now
:ref:`repurposed to append JOIN criteria to the existing query <change_select_join>` in a similar
way as that of :meth:`_orm.Query.join`, which is what users have always
expected these methods to do in any case.

In place of the ``.c`` attribute, a new attribute :attr:`_expression.SelectBase.selected_columns`
is added.  This attribute resolves to a column collection that is what most
people hope that ``.c`` does (but does not), which is to reference the columns
that are in the columns clause of the SELECT statement.   A common beginner mistake
is code such as the following::

    stmt = select(users)
    stmt = stmt.where(stmt.c.name == 'foo')

The above code appears intuitive and that it would generate
"SELECT * FROM users WHERE name='foo'", however veteran SQLAlchemy users will
recognize that it in fact generates a useless subquery resembling
"SELECT * FROM (SELECT * FROM users) WHERE name='foo'".

The new :attr:`_expression.SelectBase.selected_columns` attribute however **does** suit
the use case above, as in a case like the above it links directly to the columns
present in the ``users.c`` collection::

    stmt = select(users)
    stmt = stmt.where(stmt.selected_columns.name == 'foo')


:ticket:`4617`


.. _change_select_join:

select().join() and outerjoin() add JOIN criteria to the current query, rather than creating a subquery
-------------------------------------------------------------------------------------------------------

Towards the goal of unifying :class:`_orm.Query` and :class:`_sql.Select`,
particularly for :term:`2.0 style` use of :class:`_sql.Select`, it was critical
that there be a working :meth:`_sql.Select.join` method that behaves like the
:meth:`_orm.Query.join` method, adding additional entries to the FROM clause of
the existing SELECT and then returning the new :class:`_sql.Select` object for
further modification, instead of wrapping the object inside of an unnamed
subquery and returning a JOIN from that subquery, a behavior that has always
been virtually useless and completely misleading to users.

To allow this to be the case, :ref:`change_4617` was first implemented which
splits off :class:`_sql.Select` from having to be a :class:`_sql.FromClause`;
this removed the requirement that :meth:`_sql.Select.join` would need to
return a :class:`_sql.Join` object rather than a new version of that
:class:`_sql.Select` object that includes a new JOIN in its FROM clause.

From that point on, as the :meth:`_sql.Select.join` and :meth:`_sql.Select.outerjoin`
did have an existing behavior, the original plan was that these
methods would be deprecated, and the new "useful" version of
the methods would be available on an alternate, "future" :class:`_sql.Select`
object available as a separate import.

However, after some time working with this particular codebase, it was decided
that having two different kinds of :class:`_sql.Select` objects floating
around, each with 95% the same behavior except for some subtle difference
in how some of the methods behave was going to be more misleading and inconvenient
than simply making a hard change in how these two methods behave, given
that the existing behavior of :meth:`_sql.Select.join` and :meth:`_sql.Select.outerjoin`
is essentially never used and only causes confusion.

So it was decided, given how very useless the current behavior is, and how
extremely useful and important and useful the new behavior would be, to make a
**hard behavioral change** in this one area, rather than waiting another year
and having a more awkward API in the interim.   SQLAlchemy developers do not
take it lightly to make a completely breaking change like this, however this is
a very special case and it is extremely unlikely that the previous
implementation of these methods was being used;  as noted in
:ref:`change_4617`, major databases such as MySQL and PostgreSQL don't allow
for unnamed subqueries in any case and from a syntactical point of view it's
nearly impossible for a JOIN from an unnamed subquery to be useful since it's
very difficult to refer to the columns within it unambiguously.

With the new implementation, :meth:`_sql.Select.join` and
:meth:`_sql.Select.outerjoin` now behave very similarly to that of
:meth:`_orm.Query.join`, adding JOIN criteria to the existing statement by
matching to the left entity::

    stmt = select(user_table).join(addresses_table, user_table.c.id == addresses_table.c.user_id)

producing::

    SELECT user.id, user.name FROM user JOIN address ON user.id=address.user_id

As is the case for :class:`_sql.Join`, the ON clause is automatically determined
if feasible::

    stmt = select(user_table).join(addresses_table)

When ORM entities are used in the statement, this is essentially how ORM
queries are built up using :term:`2.0 style` invocation.  ORM entities will
assign a "plugin" to the statement internally such that ORM-related compilation
rules will take place when the statement is compiled into a SQL string. More
directly, the :meth:`_sql.Select.join` method can accommodate ORM
relationships, without breaking the hard separation between Core and ORM
internals::

    stmt = select(User).join(User.addresses)

Another new method :meth:`_sql.Select.join_from` is also added, which
allows easier specification of the left and right side of a join at once::

    stmt = select(Address.email_address, User.name).join_from(User, Address)

producing::

    SELECT address.email_address, user.name FROM user JOIN address ON user.id == address.user_id


.. _change_5526:

The URL object is now immutable
-------------------------------

The :class:`_engine.URL` object has been formalized such that it now presents
itself as a ``namedtuple`` with a fixed number of fields that are immutable. In
addition, the dictionary represented by the :attr:`_engine.URL.query` attribute
is also an immutable mapping.   Mutation of the :class:`_engine.URL` object was
not a formally supported or documented use case which led to some open-ended
use cases that made it very difficult to intercept incorrect usages, most
commonly mutation of the :attr:`_engine.URL.query` dictionary to include non-string elements.
It also led to all the common problems of allowing mutability in a fundamental
data object, namely unwanted mutations elsewhere leaking into code that didn't
expect the URL to change.  Finally, the namedtuple design is inspired by that
of Python's ``urllib.parse.urlparse()`` which returns the parsed object as a
named tuple.

The decision to change the API outright is based on a calculus weighing the
infeasibility of a deprecation path (which would involve changing the
:attr:`_engine.URL.query` dictionary to be a special dictionary that emits deprecation
warnings when any kind of standard library mutation methods are invoked, in
addition that when the dictionary would hold any kind of list of elements, the
list would also have to emit deprecation warnings on mutation) against the
unlikely use case of projects already mutating :class:`_engine.URL` objects in
the first place, as well as that small changes such as that of :ticket:`5341`
were creating backwards-incompatibility in any case.   The primary case for
mutation of a
:class:`_engine.URL` object is that of parsing plugin arguments within the
:class:`_engine.CreateEnginePlugin` extension point, itself a fairly recent
addition that based on Github code search is in use by two repositories,
neither of which are actually mutating the URL object.

The :class:`_engine.URL` object now provides a rich interface inspecting
and generating new :class:`_engine.URL` objects.  The
existing mechanism to create a :class:`_engine.URL` object, the
:func:`_engine.make_url` function, remains unchanged::

     >>> from sqlalchemy.engine import make_url
     >>> url = make_url("postgresql+psycopg2://user:pass@host/dbname")

For programmatic construction, code that may have been using the
:class:`_engine.URL` constructor or ``__init__`` method directly will
receive a deprecation warning if arguments are passed as keyword arguments
and not an exact 7-tuple.  The keyword-style constructor is now available
via the :meth:`_engine.URL.create` method::

    >>> from sqlalchemy.engine import URL
    >>> url = URL.create("postgresql", "user", "pass", host="host", database="dbname")
    >>> str(url)
    'postgresql://user:pass@host/dbname'


Fields can be altered typically using the :meth:`_engine.URL.set` method, which
returns a new :class:`_engine.URL` object with changes applied::

    >>> mysql_url = url.set(drivername="mysql+pymysql")
    >>> str(mysql_url)
    'mysql+pymysql://user:pass@host/dbname'

To alter the contents of the :attr:`_engine.URL.query` dictionary, methods
such as :meth:`_engine.URL.update_query_dict` may be used::

    >>> url.update_query_dict({"sslcert": '/path/to/crt'})
    postgresql://user:***@host/dbname?sslcert=%2Fpath%2Fto%2Fcrt

To upgrade code that is mutating these fields directly, a **backwards and
forwards compatible approach** is to use a duck-typing, as in the following
style::

    def set_url_drivername(some_url, some_drivername):
        # check for 1.4
        if hasattr(some_url, "set"):
            return some_url.set(drivername=some_drivername)
        else:
            # SQLAlchemy 1.3 or earlier, mutate in place
            some_url.drivername = some_drivername
            return some_url

    def set_ssl_cert(some_url, ssl_cert):
        # check for 1.4
        if hasattr(some_url, "update_query_dict"):
            return some_url.update_query_dict({"sslcert": ssl_cert})
        else:
            # SQLAlchemy 1.3 or earlier, mutate in place
            some_url.query["sslcert"] = ssl_cert
            return some_url

The query string retains its existing format as a dictionary of strings
to strings, using sequences of strings to represent multiple parameters.
For example::

    >>> from sqlalchemy.engine import make_url
    >>> url = make_url("postgresql://user:pass@host/dbname?alt_host=host1&alt_host=host2&sslcert=%2Fpath%2Fto%2Fcrt")
    >>> url.query
    immutabledict({'alt_host': ('host1', 'host2'), 'sslcert': '/path/to/crt'})

To work with the contents of the :attr:`_engine.URL.query` attribute such that all values are
normalized into sequences, use the :attr:`_engine.URL.normalized_query` attribute::

    >>> url.normalized_query
    immutabledict({'alt_host': ('host1', 'host2'), 'sslcert': ('/path/to/crt',)})

The query string can be appended to via methods such as :meth:`_engine.URL.update_query_dict`,
:meth:`_engine.URL.update_query_pairs`, :meth:`_engine.URL.update_query_string`::

    >>> url.update_query_dict({"alt_host": "host3"}, append=True)
    postgresql://user:***@host/dbname?alt_host=host1&alt_host=host2&alt_host=host3&sslcert=%2Fpath%2Fto%2Fcrt

.. seealso::

  :class:`_engine.URL`


Changes to CreateEnginePlugin
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :class:`_engine.CreateEnginePlugin` is also impacted by this change,
as the documentation for custom plugins indicated that the ``dict.pop()``
method should be used to remove consumed arguments from the URL object.  This
should now be achieved using the :meth:`_engine.CreateEnginePlugin.update_url`
method.  A backwards compatible approach would look like::

    from sqlalchemy.engine import CreateEnginePlugin

    class MyPlugin(CreateEnginePlugin):
        def __init__(self, url, kwargs):
            # check for 1.4 style
            if hasattr(CreateEnginePlugin, "update_url"):
                self.my_argument_one = url.query['my_argument_one']
                self.my_argument_two = url.query['my_argument_two']
            else:
                # legacy
                self.my_argument_one = url.query.pop('my_argument_one')
                self.my_argument_two = url.query.pop('my_argument_two')

            self.my_argument_three = kwargs.pop('my_argument_three', None)

        def update_url(self, url):
            # this method runs in 1.4 only and should be used to consume
            # plugin-specific arguments
            return url.difference_update_query(
                ["my_argument_one", "my_argument_two"]
            )

See the docstring at :class:`_engine.CreateEnginePlugin` for complete details
on how this class is used.

:ticket:`5526`


.. _change_5284:

select(), case() now accept positional expressions
---------------------------------------------------

As it may be seen elsewhere in this document, the :func:`_sql.select` construct will
now accept "columns clause" arguments positionally, rather than requiring they
be passed as a list::

    # new way, supports 2.0
    stmt = select(table.c.col1, table.c.col2, ...)

When sending the arguments positionally, no other keyword arguments are permitted.
In SQLAlchemy 2.0, the above calling style will be the only calling style
supported.

For the duration of 1.4, the previous calling style will still continue
to function, which passes the list of columns or other expressions as a list::

    # old way, still works in 1.4
    stmt = select([table.c.col1, table.c.col2, ...])

The above legacy calling style also accepts the old keyword arguments that have
since been removed from most narrative documentation.  The existence of these
keyword arguments is why the columns clause was passed as a list in the first place::

    # very much the old way, but still works in 1.4
    stmt = select([table.c.col1, table.c.col2, ...], whereclause=table.c.col1 == 5)

The detection between the two styles is based on whether or not the first
positional argument is a list.   There are unfortunately still likely some
usages that look like the following, where the keyword for the "whereclause"
is omitted::

    # very much the old way, but still works in 1.4
    stmt = select([table.c.col1, table.c.col2, ...], table.c.col1 == 5)

As part of this change, the :class:`.Select` construct also gains the 2.0-style
"future" API which includes an updated :meth:`.Select.join` method as well
as methods like :meth:`.Select.filter_by` and :meth:`.Select.join_from`.

In a related change, the :func:`_sql.case` construct has also been modified
to accept its list of WHEN clauses positionally, with a similar deprecation
track for the old calling style::

    stmt = select(users_table).where(
        case(
            (users_table.c.name == 'wendy', 'W'),
            (users_table.c.name == 'jack', 'J'),
            else_='E'
        )
    )

The convention for SQLAlchemy constructs accepting ``*args`` vs. a list of
values, as is the latter case for a construct like
:meth:`_sql.ColumnOperators.in_`, is that **positional arguments are used for
structural specification, lists are used for data specification**.


.. seealso::

    :ref:`migration_20_5284`

    :ref:`error_c9ae`


:ticket:`5284`

.. _change_4645:

All IN expressions render parameters for each value in the list on the fly (e.g. expanding parameters)
------------------------------------------------------------------------------------------------------

The "expanding IN" feature, first introduced in :ref:`change_3953`, has matured
enough such that it is clearly superior to the previous method of rendering IN
expressions.  As the approach was improved to handle empty lists of values, it
is now the only means that Core / ORM will use to render lists of IN
parameters.

The previous approach which has been present in SQLAlchemy since its first
release was that when a list of values were passed to the
:meth:`.ColumnOperators.in_` method, the list would be expanded into a series
of individual :class:`.BindParameter` objects at statement construction time.
This suffered from the limitation that it was not possible to vary the
parameter list at statement execution time based on the parameter dictionary,
which meant that string SQL statements could not be cached independently of
their parameters, nor could the parameter dictionary be fully used for
statements that included IN expressions generally.

In order to service the "baked query" feature described at
:ref:`baked_toplevel`, a cacheable version of IN was needed, which is what
brought about the "expanding IN" feature.  In contrast to the existing behavior
whereby the parameter list is expanded at statement construction time into
individual :class:`.BindParameter` objects, the feature instead uses a single
:class:`.BindParameter` that stores the list of values at once; when the
statement is executed by the :class:`_engine.Engine`, it is "expanded" on the fly into
individual bound parameter positions based on the parameters passed to the call
to :meth:`_engine.Connection.execute`, and the existing SQL string which may have been
retrieved from a previous execution is modified using a regular expression to
suit the current parameter set.   This allows for the same :class:`.Compiled`
object, which stores the rendered string statement, to be invoked multiple
times against different parameter sets that modify the list contents passed to
IN expressions, while still maintaining the behavior of individual scalar
parameters being passed to the DBAPI.  While some DBAPIs do support this
functionality directly, it is not generally available; the "expanding IN"
feature now supports the behavior consistently for all backends.

As a major focus of 1.4 is to allow for true statement caching in Core and ORM
without the awkwardness of the "baked" system, and since the "expanding IN"
feature represents a simpler approach to building expressions in any case,
it's now invoked automatically whenever a list of values is passed to
an IN expression::

    stmt = select(A.id, A.data).where(A.id.in_([1, 2, 3]))

The pre-execution string representation is::

    >>> print(stmt)
    SELECT a.id, a.data
    FROM a
    WHERE a.id IN ([POSTCOMPILE_id_1])

To render the values directly, use ``literal_binds`` as was the case previously::

    >>> print(stmt.compile(compile_kwargs={"literal_binds": True}))
    SELECT a.id, a.data
    FROM a
    WHERE a.id IN (1, 2, 3)

A new flag, "render_postcompile", is added as a helper to allow the current
bound value to be rendered as it would be passed to the database::

    >>> print(stmt.compile(compile_kwargs={"render_postcompile": True}))
    SELECT a.id, a.data
    FROM a
    WHERE a.id IN (:id_1_1, :id_1_2, :id_1_3)

Engine logging output shows the ultimate rendered statement as well::

    INFO sqlalchemy.engine.base.Engine SELECT a.id, a.data
    FROM a
    WHERE a.id IN (?, ?, ?)
    INFO sqlalchemy.engine.base.Engine (1, 2, 3)

As part of this change, the behavior of "empty IN" expressions, where the list
parameter is empty, is now standardized on use of the IN operator against a
so-called "empty set".  As there is no standard SQL syntax for empty sets, a
SELECT that returns no rows is used, tailored in specific ways for each backend
so that the database treats it as an empty set; this feature was first
introduced in version 1.3 and is described at :ref:`change_4271`.  The
:paramref:`_sa.create_engine.empty_in_strategy` parameter, introduced in version
1.2 as a means for migrating for how this case was treated for the previous IN
system, is now deprecated and this flag no longer has an effect; as described
in :ref:`change_3907`, this flag allowed a dialect to switch between the
original system of comparing a column against itself, which turned out to be a
huge performance issue, and a newer system of comparing "1 != 1" in
order to produce a "false" expression. The 1.3 introduced behavior which
now takes place in all cases is more correct than both approaches as the IN
operator is still used, and does not have the performance issue of the original
system.

In addition, the "expanding" parameter system has been generalized so that it
also services other dialect-specific use cases where a parameter cannot be
accommodated by the DBAPI or backing database; see :ref:`change_4808` for
details.

.. seealso::

    :ref:`change_4808`

    :ref:`change_4271`

    :class:`.BindParameter`

:ticket:`4645`

.. _change_4737:


Built-in FROM linting will warn for any potential cartesian products in a SELECT statement
------------------------------------------------------------------------------------------

As the Core expression language as well as the ORM are built on an "implicit
FROMs" model where a particular FROM clause is automatically added if any part
of the query refers to it, a common issue is the case where a SELECT statement,
either a top level statement or an embedded subquery, contains FROM elements
that are not joined to the rest of the FROM elements in the query, causing
what's referred to as a "cartesian product" in the result set, i.e. every
possible combination of rows from each FROM element not otherwise joined.  In
relational databases, this is nearly always an undesirable outcome as it
produces an enormous result set full of duplicated, uncorrelated data.

SQLAlchemy, for all of its great features, is particularly prone to this sort
of issue happening as a SELECT statement will have elements added to its FROM
clause automatically from any table seen in the other clauses. A typical
scenario looks like the following, where two tables are JOINed together,
however an additional entry in the WHERE clause that perhaps inadvertently does
not line up with these two tables will create an additional FROM entry::

    address_alias = aliased(Address)

    q = session.query(User).\
        join(address_alias, User.addresses).\
        filter(Address.email_address == 'foo')

The above query selects from a JOIN of ``User`` and ``address_alias``, the
latter of which is an alias of the ``Address`` entity.  However, the
``Address`` entity is used within the WHERE clause directly, so the above would
result in the SQL::

    SELECT
        users.id AS users_id, users.name AS users_name,
        users.fullname AS users_fullname,
        users.nickname AS users_nickname
    FROM addresses, users JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE addresses.email_address = :email_address_1

In the above SQL, we can see what SQLAlchemy developers term "the dreaded
comma", as we see "FROM addresses, users JOIN addresses" in the FROM clause
which is the classic sign of a cartesian product; where a query is making use
of JOIN in order to join FROM clauses together, however because one of them is
not joined, it uses a comma.      The above query will return a full set of
rows that join the "user" and "addresses" table together on the "id / user_id"
column, and will then apply all those rows into a cartesian product against
every row in the "addresses" table directly.   That is, if there are ten user
rows and 100 rows in addresses, the above query will return its expected result
rows, likely to be 100 as all address rows would be selected, multiplied by 100
again, so that the total result size would be 10000 rows.

The "table1, table2 JOIN table3" pattern is one that also occurs quite
frequently within the SQLAlchemy ORM due to either subtle mis-application of
ORM features particularly those related to joined eager loading or joined table
inheritance, as well as a result of SQLAlchemy ORM bugs within those same
systems.   Similar issues apply to SELECT statements that use "implicit joins",
where the JOIN keyword is not used and instead each FROM element is linked with
another one via the WHERE clause.

For some years there has been a recipe on the Wiki that applies a graph
algorithm to a :func:`_expression.select` construct at query execution time and inspects
the structure of the query for these un-linked FROM clauses, parsing through
the WHERE clause and all JOIN clauses to determine how FROM elements are linked
together and ensuring that all the FROM elements are connected in a single
graph. This recipe has now been adapted to be part of the :class:`.SQLCompiler`
itself where it now optionally emits a warning for a statement if this
condition is detected.   The warning is enabled using the
:paramref:`_sa.create_engine.enable_from_linting` flag and is enabled by default.
The computational overhead of the linter is very low, and additionally it only
occurs during statement compilation which means for a cached SQL statement it
only occurs once.

Using this feature, our ORM query above will emit a warning::

    >>> q.all()
    SAWarning: SELECT statement has a cartesian product between FROM
    element(s) "addresses_1", "users" and FROM element "addresses".
    Apply join condition(s) between each element to resolve.

The linter feature accommodates not just for tables linked together through the
JOIN clauses but also through the WHERE clause  Above, we can add a WHERE
clause to link the new ``Address`` entity with the previous ``address_alias``
entity and that will remove the warning::

    q = session.query(User).\
        join(address_alias, User.addresses).\
        filter(Address.email_address == 'foo').\
        filter(Address.id == address_alias.id)  # resolve cartesian products,
                                                # will no longer warn

The cartesian product warning considers **any** kind of link between two
FROM clauses to be a resolution, even if the end result set is still
wasteful, as the linter is intended only to detect the common case of a
FROM clause that is completely unexpected.  If the FROM clause is referred
to explicitly elsewhere and linked to the other FROMs, no warning is emitted::

    q = session.query(User).\
        join(address_alias, User.addresses).\
        filter(Address.email_address == 'foo').\
        filter(Address.id > address_alias.id)  # will generate a lot of rows,
                                               # but no warning

Full cartesian products are also allowed if they are explicitly stated; if we
wanted for example the cartesian product of ``User`` and ``Address``, we can
JOIN on :func:`.true` so that every row will match with every other; the
following query will return all rows and produce no warnings::

    from sqlalchemy import true

    # intentional cartesian product
    q = session.query(User).join(Address, true())  # intentional cartesian product

The warning is only generated by default when the statement is compiled by the
:class:`_engine.Connection` for execution; calling the :meth:`_expression.ClauseElement.compile`
method will not emit a warning unless the linting flag is supplied::

    >>> from sqlalchemy.sql import FROM_LINTING
    >>> print(q.statement.compile(linting=FROM_LINTING))
    SAWarning: SELECT statement has a cartesian product between FROM element(s) "addresses" and FROM element "users".  Apply join condition(s) between each element to resolve.
    SELECT users.id, users.name, users.fullname, users.nickname
    FROM addresses, users JOIN addresses AS addresses_1 ON users.id = addresses_1.user_id
    WHERE addresses.email_address = :email_address_1

:ticket:`4737`


.. _change_result_14_core:

New Result object
-----------------

A major goal of SQLAlchemy 2.0 is to unify how "results" are handled between
the ORM and Core.   Towards this goal, version 1.4 introduces new versions
of both the ``ResultProxy`` and ``RowProxy`` objects that have been part
of SQLAlchemy since the beginning.

The new objects are documented at :class:`_engine.Result` and :class:`_engine.Row`,
and are used not only for Core result sets but for :term:`2.0 style` results
within the ORM as well.

This result object is fully compatible with ``ResultProxy`` and includes many
new features, that are now applied to both Core and ORM results equally,
including methods such as:

:meth:`_engine.Result.one` - returns exactly a single row, or raises:

.. sourcecode::

    with engine.connect() as conn:
        row = conn.execute(table.select().where(table.c.id == 5)).one()


:meth:`_engine.Result.one_or_none` - same, but also returns None for no rows

:meth:`_engine.Result.all` - returns all rows

:meth:`_engine.Result.partitions` - fetches rows in chunks:

.. sourcecode::

    with engine.connect() as conn:
        result = conn.execute(
            table.select().order_by(table.c.id),
            execution_options={"stream_results": True}
        )
        for chunk in result.partitions(500):
            # process up to 500 records

:meth:`_engine.Result.columns` - allows slicing and reorganizing of rows:

.. sourcecode::

     with engine.connect() as conn:
        # requests x, y, z
        result = conn.execute(select(table.c.x, table.c.y, table.c.z))

        # iterate rows as y, x
        for y, x in result.columns("y", "x"):
            print("Y: %s  X: %s" % (y, x))

:meth:`_engine.Result.scalars` - returns lists of scalar objects, from the
first column by default but can also be selected:

.. sourcecode::

    result = session.execute(select(User).order_by(User.id))
    for user_obj in result.scalars():
        # ...

:meth:`_engine.Result.mappings` - instead of named-tuple rows, returns
dictionaries:

.. sourcecode::

     with engine.connect() as conn:
        result = conn.execute(select(table.c.x, table.c.y, table.c.z))

        for map_ in result.mappings():
            print("Y: %(y)s  X: %(x)s" % map_)

When using Core, the object returned by :meth:`_engine.Connection.execute` is
an instance of :class:`.CursorResult`, which continues to feature the same API
features as ``ResultProxy`` regarding inserted primary keys, defaults,
rowcounts, etc.   For ORM, a :class:`_result.Result` subclass will be returned
that performs translation of Core rows into ORM rows, and then allows all the
same operations to take place.

.. seealso::

    :ref:`migration_20_unify_select` - in the 2.0 migration documentation

:ticket:`5087`

:ticket:`4395`

:ticket:`4959`


.. _change_4710_core:

RowProxy is no longer a "proxy"; is now called Row and behaves like an enhanced named tuple
-------------------------------------------------------------------------------------------

The :class:`.RowProxy` class, which represents individual database result rows
in a Core result set, is now called :class:`.Row` and is no longer a "proxy"
object; what this means is that when the :class:`.Row` object is returned, the
row is a simple tuple that contains the data in its final form, already having
been processed by result-row handling functions associated with datatypes
(examples include turning a date string from the database into a ``datetime``
object, a JSON string into a Python ``json.loads()`` result, etc.).

The immediate rationale for this is so that the row can act more like a Python
named tuple, rather than a mapping, where the values in the tuple are the
subject of the ``__contains__`` operator on the tuple, rather than the keys.
With :class:`.Row` acting like a named tuple, it is then suitable for use as as
replacement for the ORM's :class:`.KeyedTuple` object, leading to an eventual
API where both the ORM and Core deliver result sets that  behave identically.
Unification of major patterns within ORM and Core is a major goal of SQLAlchemy
2.0, and release 1.4 aims to have most or all of the underlying architectural
patterns in place in order to support this process.   The note in
:ref:`change_4710_orm` describes the ORM's use of the :class:`.Row` class.

For release 1.4, the :class:`.Row` class provides an additional subclass
:class:`.LegacyRow`, which is used by Core and provides a backwards-compatible
version of :class:`.RowProxy` while emitting deprecation warnings for those API
features and behaviors that will be moved.  ORM :class:`_query.Query` now makes use
of :class:`.Row` directly as a replacement for :class:`.KeyedTuple`.

The :class:`.LegacyRow` class is a transitional class where the
``__contains__`` method is still testing against the keys, not the values,
while emitting a deprecation warning when the operation succeeds.
Additionally, all the other mapping-like methods on the previous
:class:`.RowProxy` are deprecated, including :meth:`.LegacyRow.keys`,
:meth:`.LegacyRow.items`, etc.  For mapping-like behaviors from a :class:`.Row`
object, including support for these methods as well as a key-oriented
``__contains__`` operator, the API going forward will be to first access a
special attribute :attr:`.Row._mapping`, which will then provide a complete
mapping interface to the row, rather than a tuple interface.

Rationale: To behave more like a named tuple rather than a mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The difference between a named tuple and a mapping as far as boolean operators
can be summarized.   Given a "named tuple" in pseudo code as::

    row = (id: 5,  name: 'some name')

The biggest cross-incompatible difference is the behavior of ``__contains__``::

    "id" in row          # True for a mapping, False for a named tuple
    "some name" in row   # False for a mapping, True for a named tuple

In 1.4, when a :class:`.LegacyRow` is returned by a Core result set, the above
``"id" in row`` comparison will continue to succeed, however a deprecation
warning will be emitted.   To use the "in" operator as a mapping, use the
:attr:`.Row._mapping` attribute::

    "id" in row._mapping

SQLAlchemy 2.0's result object will feature a ``.mappings()`` modifier so that
these mappings can be received directly::

    # using sqlalchemy.future package
    for row in result.mappings():
        row["id"]

Proxying behavior goes away, was also unnecessary in modern usage
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The refactor of :class:`.Row` to behave like a tuple requires that all
data values be fully available up front.  This is an internal behavior change
from that of :class:`.RowProxy`, where result-row processing functions would
be invoked at the point of accessing an element of the row, instead of
when the row was first fetched.     This means for example when retrieving
a datetime value from SQLite, the data for the row as present in the
:class:`.RowProxy` object would previously have looked like::

    row_proxy = (1, '2019-12-31 19:56:58.272106')

and then upon access via ``__getitem__``, the ``datetime.strptime()`` function
would be used on the fly to convert the above string date into a ``datetime``
object.     With the new architecture, the ``datetime()`` object is present
in the tuple when it is returned, the ``datetime.strptime()`` function
having been called just once up front::

    row = (1, datetime.datetime(2019, 12, 31, 19, 56, 58, 272106))

The :class:`.RowProxy` and :class:`.Row` objects in SQLAlchemy are where the
majority of SQLAlchemy's C extension code takes place.   This code has been
highly refactored to provide the new behavior in an efficient manner, and
overall performance has been improved as the design of :class:`.Row` is now
considerably simpler.

The rationale behind the previous  behavior assumed a usage model where a
result row might have dozens or hundreds of columns present, where most of
those columns would not be accessed, and for which a majority of those columns
would require some result-value processing function.  By invoking the
processing function only when needed, the goal was that lots of result
processing functions would not be necessary, thus increasing performance.

There are many reasons why the above assumptions do not hold:

1. the vast majority of row-processing functions called were to Unicode decode
   a bytestring into a Python Unicode string under Python 2.   This was right
   as Python Unicode was beginning to see use and before Python 3 existed.
   Once Python 3 was introduced, within a few years, all Python DBAPIs took
   on the proper role of supporting the delivering of Python Unicode objects directly, under
   both Python 2 and Python 3, as an option in the former case and as the only
   way forward in the latter case.  Eventually, in most cases it became
   the default for Python 2 as well.   SQLAlchemy's Python 2 support still
   enables explicit string-to-Unicode conversion for some DBAPIs such as
   cx_Oracle, however it is now performed at the DBAPI level rather than
   as a standard SQLAlchemy result row processing function.

2. The above string conversion, when it is used, was made to be extremely
   performant via the C extensions, so much so that even in 1.4, SQLAlchemy's
   byte-to-Unicode codec hook is plugged into cx_Oracle where it has been
   observed to be more performant than cx_Oracle's own hook; this meant that
   the overhead for converting all strings in a row was not as significant
   as it originally was in any case.

3. Row processing functions are not used in most other cases; the
   exceptions are SQLite's datetime support, JSON support for some backends,
   some numeric handlers such as string to ``Decimal``.   In the case of
   ``Decimal``, Python 3 also standardized on the highly performant ``cdecimal``
   implementation, which is not the case in Python 2 which continues to use
   the much less performant pure Python version.

4. Fetching full rows where only a few columns are needed is not common within
   real-world use cases  In the early days of SQLAlchemy, database code from other
   languages of the form "row = fetch('SELECT * FROM table')" was common;
   using SQLAlchemy's expression language however, code observed in the wild
   typically makes use of the specific columns needed.

.. seealso::

    :ref:`change_4710_orm`

:ticket:`4710`

.. _change_4753:

SELECT objects and derived FROM clauses allow for duplicate columns and column labels
-------------------------------------------------------------------------------------

This change allows that the :func:`_expression.select` construct now allows for duplicate
column labels as well as duplicate column objects themselves, so that result
tuples are organized and ordered in the identical way in that the columns were
selected.  The ORM :class:`_query.Query` already works this way, so this change
allows for greater cross-compatibility between the two, which is a key goal of
the 2.0 transition::

    >>> from sqlalchemy import column, select
    >>> c1, c2, c3, c4 = column('c1'), column('c2'), column('c3'), column('c4')
    >>> stmt = select(c1, c2, c3.label('c2'), c2, c4)
    >>> print(stmt)
    SELECT c1, c2, c3 AS c2, c2, c4

To support this change, the :class:`_expression.ColumnCollection` used by
:class:`_expression.SelectBase` as well as for derived FROM clauses such as subqueries
also support duplicate columns; this includes the new
:attr:`_expression.SelectBase.selected_columns` attribute, the deprecated ``SelectBase.c``
attribute, as well as the :attr:`_expression.FromClause.c` attribute seen on constructs
such as :class:`.Subquery` and :class:`_expression.Alias`::

    >>> list(stmt.selected_columns)
    [
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540bcca20; c1>,
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540bcc9e8; c2>,
        <sqlalchemy.sql.elements.Label object at 0x7fa540b3e2e8>,
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540bcc9e8; c2>,
        <sqlalchemy.sql.elements.ColumnClause at 0x7fa540897048; c4>
    ]

    >>> print(stmt.subquery().select())
    SELECT anon_1.c1, anon_1.c2, anon_1.c2, anon_1.c2, anon_1.c4
    FROM (SELECT c1, c2, c3 AS c2, c2, c4) AS anon_1

:class:`_expression.ColumnCollection` also allows access by integer index to support
when the string "key" is ambiguous::

    >>> stmt.selected_columns[2]
    <sqlalchemy.sql.elements.Label object at 0x7fa540b3e2e8>

To suit the use of :class:`_expression.ColumnCollection` in objects such as
:class:`_schema.Table` and :class:`.PrimaryKeyConstraint`, the old "deduplicating"
behavior which is more critical for these objects is preserved in a new class
:class:`.DedupeColumnCollection`.

The change includes that the familiar warning ``"Column %r on table %r being
replaced by %r, which has the same key.  Consider use_labels for select()
statements."`` is **removed**; the :meth:`_expression.Select.apply_labels` is still
available and is still used by the ORM for all SELECT operations, however it
does not imply deduplication of column objects, although it does imply
deduplication of implicitly generated labels::

    >>> from sqlalchemy import table
    >>> user = table('user', column('id'), column('name'))
    >>> stmt = select(user.c.id, user.c.name, user.c.id).apply_labels()
    >>> print(stmt)
    SELECT "user".id AS user_id, "user".name AS user_name, "user".id AS id_1
    FROM "user"

Finally, the change makes it easier to create UNION and other
:class:`_selectable.CompoundSelect` objects, by ensuring that the number and position
of columns in a SELECT statement mirrors what was given, in a use case such
as::

    >>> s1 = select(user, user.c.id)
    >>> s2 = select(c1, c2, c3)
    >>> from sqlalchemy import union
    >>> u = union(s1, s2)
    >>> print(u)
    SELECT "user".id, "user".name, "user".id
    FROM "user" UNION SELECT c1, c2, c3



:ticket:`4753`



.. _change_4449:

Improved column labeling for simple column expressions using CAST or similar
----------------------------------------------------------------------------

A user pointed out that the PostgreSQL database has a convenient behavior when
using functions like CAST against a named column, in that the result column name
is named the same as the inner expression::

    test=> SELECT CAST(data AS VARCHAR) FROM foo;

    data
    ------
     5
    (1 row)

This allows one to apply CAST to table columns while not losing the column
name (above using the name ``"data"``) in the result row.    Compare to
databases such as MySQL/MariaDB, as well as most others, where the column
name is taken from the full SQL expression and is not very portable::

    MariaDB [test]> SELECT CAST(data AS CHAR) FROM foo;
    +--------------------+
    | CAST(data AS CHAR) |
    +--------------------+
    | 5                  |
    +--------------------+
    1 row in set (0.003 sec)


In SQLAlchemy Core expressions, we never deal with a raw generated name like
the above, as SQLAlchemy applies auto-labeling to expressions like these, which
are up until now always a so-called "anonymous" expression::

    >>> print(select(cast(foo.c.data, String)))
    SELECT CAST(foo.data AS VARCHAR) AS anon_1     # old behavior
    FROM foo

These anonymous expressions were necessary as SQLAlchemy's
:class:`_engine.ResultProxy` made heavy use of result column names in order to match
up datatypes, such as the :class:`.String` datatype which used to have
result-row-processing behavior, to the correct column, so most importantly the
names had to be both easy to determine in a database-agnostic manner as well as
unique in all cases.    In SQLAlchemy 1.0 as part of :ticket:`918`, this
reliance on named columns in result rows (specifically the
``cursor.description`` element of the PEP-249 cursor) was scaled back to not be
necessary for most Core SELECT constructs; in release 1.4, the system overall
is becoming more comfortable with SELECT statements that have duplicate column
or label names such as in :ref:`change_4753`.  So we now emulate PostgreSQL's
reasonable behavior for simple modifications to a single column, most
prominently with CAST::

    >>> print(select(cast(foo.c.data, String)))
    SELECT CAST(foo.data AS VARCHAR) AS data
    FROM foo

For CAST against expressions that don't have a name, the previous logic is used
to generate the usual "anonymous" labels::

    >>> print(select(cast('hi there,' + foo.c.data, String)))
    SELECT CAST(:data_1 + foo.data AS VARCHAR) AS anon_1
    FROM foo

A :func:`.cast` against a :class:`.Label`, despite having to omit the label
expression as these don't render inside of a CAST, will nonetheless make use of
the given name::

    >>> print(select(cast(('hi there,' + foo.c.data).label('hello_data'), String)))
    SELECT CAST(:data_1 + foo.data AS VARCHAR) AS hello_data
    FROM foo

And of course as was always the case, :class:`.Label` can be applied to the
expression on the outside to apply an "AS <name>" label directly::

    >>> print(select(cast(('hi there,' + foo.c.data), String).label('hello_data')))
    SELECT CAST(:data_1 + foo.data AS VARCHAR) AS hello_data
    FROM foo


:ticket:`4449`

.. _change_4808:

New "post compile" bound parameters used for LIMIT/OFFSET in Oracle, SQL Server
-------------------------------------------------------------------------------

A major goal of the 1.4 series is to establish that all Core SQL constructs
are completely cacheable, meaning that a particular :class:`.Compiled`
structure will produce an identical SQL string regardless of any SQL parameters
used with it, which notably includes those used to specify the LIMIT and
OFFSET values, typically used for pagination and "top N" style results.

While SQLAlchemy has used bound parameters for LIMIT/OFFSET schemes for many
years, a few outliers remained where such parameters were not allowed, including
a SQL Server "TOP N" statement, such as::

    SELECT TOP 5 mytable.id, mytable.data FROM mytable

as well as with Oracle, where the FIRST_ROWS() hint (which SQLAlchemy will
use if the ``optimize_limits=True`` parameter is passed to
:func:`_sa.create_engine` with an Oracle URL) does not allow them,
but also that using bound parameters with ROWNUM comparisons has been reported
as producing slower query plans::

    SELECT anon_1.id, anon_1.data FROM (
        SELECT /*+ FIRST_ROWS(5) */
        anon_2.id AS id,
        anon_2.data AS data,
        ROWNUM AS ora_rn FROM (
            SELECT mytable.id, mytable.data FROM mytable
        ) anon_2
        WHERE ROWNUM <= :param_1
    ) anon_1 WHERE ora_rn > :param_2

In order to allow for all statements to be unconditionally cacheable at the
compilation level, a new form of bound parameter called a "post compile"
parameter has been added, which makes use of the same mechanism as that
of "expanding IN parameters".  This is a :func:`.bindparam` that behaves
identically to any other bound parameter except that parameter value will
be rendered literally into the SQL string before sending it to the DBAPI
``cursor.execute()`` method.   The new parameter is used internally by the
SQL Server and Oracle dialects, so that the drivers receive the literal
rendered value but the rest of SQLAlchemy can still consider this as a
bound parameter.   The above two statements when stringified using
``str(statement.compile(dialect=<dialect>))`` now look like::

    SELECT TOP [POSTCOMPILE_param_1] mytable.id, mytable.data FROM mytable

and::

    SELECT anon_1.id, anon_1.data FROM (
        SELECT /*+ FIRST_ROWS([POSTCOMPILE__ora_frow_1]) */
        anon_2.id AS id,
        anon_2.data AS data,
        ROWNUM AS ora_rn FROM (
            SELECT mytable.id, mytable.data FROM mytable
        ) anon_2
        WHERE ROWNUM <= [POSTCOMPILE_param_1]
    ) anon_1 WHERE ora_rn > [POSTCOMPILE_param_2]

The ``[POSTCOMPILE_<param>]`` format is also what is seen when an
"expanding IN" is used.

When viewing the SQL logging output, the final form of the statement will
be seen::

    SELECT anon_1.id, anon_1.data FROM (
        SELECT /*+ FIRST_ROWS(5) */
        anon_2.id AS id,
        anon_2.data AS data,
        ROWNUM AS ora_rn FROM (
            SELECT mytable.id AS id, mytable.data AS data FROM mytable
        ) anon_2
        WHERE ROWNUM <= 8
    ) anon_1 WHERE ora_rn > 3


The "post compile parameter" feature is exposed as public API through the
:paramref:`.bindparam.literal_execute` parameter, however is currently not
intended for general use.   The literal values are rendered using the
:meth:`.TypeEngine.literal_processor` of the underlying datatype, which in
SQLAlchemy has **extremely limited** scope, supporting only integers and simple
string values.

:ticket:`4808`

.. _change_4712:

Connection-level transactions can now be inactive based on subtransaction
-------------------------------------------------------------------------

A :class:`_engine.Connection` now includes the behavior where a :class:`.Transaction`
can be made inactive due to a rollback on an inner transaction, however the
:class:`.Transaction` will not clear until it is itself rolled back.

This is essentially a new error condition which will disallow statement
executions to proceed on a :class:`_engine.Connection` if an inner "sub" transaction
has been rolled back.  The behavior works very similarly to that of the
ORM :class:`.Session`, where if an outer transaction has been begun, it needs
to be rolled back to clear the invalid transaction; this behavior is described
in :ref:`faq_session_rollback`.

While the :class:`_engine.Connection` has had a less strict behavioral pattern than
the :class:`.Session`, this change was made as it helps to identify when
a subtransaction has rolled back the DBAPI transaction, however the external
code isn't aware of this and attempts to continue proceeding, which in fact
runs operations on a new transaction.   The "test harness" pattern described
at :ref:`session_external_transaction` is the common place for this to occur.

The "subtransaction" feature of Core and ORM is itself deprecated and will
no longer be present in version 2.0.   As a result, this new error condition
is itself temporary as it will no longer apply once subtransactions are removed.

In order to work with the 2.0 style behavior that does not include
subtransactions, use the :paramref:`_sa.create_engine.future` parameter
on :func:`_sa.create_engine`.

The error message is described in the errors page at :ref:`error_8s2a`.

.. _change_5367:

Enum and Boolean datatypes no longer default to "create constraint"
-------------------------------------------------------------------

The :paramref:`.Enum.create_constraint` and
:paramref:`.Boolean.create_constraint` parameters now default to False,
indicating when a so-called "non-native" version of these two datatypes is
created, a CHECK constraint will **not** be generated by default.   These
CHECK constraints present schema-management maintenance complexities that
should be opted in to, rather than being turned on by default.


To ensure that a CREATE CONSTRAINT is emitted for these types, set these
flags to ``True``::

  class Spam(Base):
      __tablename__ = "spam"
      id = Column(Integer, primary_key=True)
      boolean = Column(Boolean(create_constraint=True))
      enum = Column(Enum("a", "b", "c", create_constraint=True))


:ticket:`5367`

New Features - ORM
==================

.. _change_4826:

Raiseload for Columns
---------------------

The "raiseload" feature, which raises :class:`.InvalidRequestError` when an
unloaded attribute is accessed, is now available for column-oriented attributes
using the :paramref:`.orm.defer.raiseload` parameter of :func:`.defer`. This
works in the same manner as that of the :func:`.raiseload` option used by
relationship loading::

    book = session.query(Book).options(defer(Book.summary, raiseload=True)).first()

    # would raise an exception
    book.summary

To configure column-level raiseload on a mapping, the
:paramref:`.deferred.raiseload` parameter of :func:`.deferred` may be used.  The
:func:`.undefer` option may then be used at query time to eagerly load
the attribute::

    class Book(Base):
        __tablename__ = 'book'

        book_id = Column(Integer, primary_key=True)
        title = Column(String(200), nullable=False)
        summary = deferred(Column(String(2000)), raiseload=True)
        excerpt = deferred(Column(Text), raiseload=True)

    book_w_excerpt = session.query(Book).options(undefer(Book.excerpt)).first()

It was originally considered that the existing :func:`.raiseload` option that
works for :func:`_orm.relationship` attributes be expanded to also support column-oriented
attributes.    However, this would break the "wildcard" behavior of :func:`.raiseload`,
which is documented as allowing one to prevent all relationships from loading::

    session.query(Order).options(
        joinedload(Order.items), raiseload('*'))

Above, if we had expanded :func:`.raiseload` to accommodate for columns  as
well, the wildcard would also prevent columns from loading and thus be  a
backwards incompatible change; additionally, it's not clear if
:func:`.raiseload` covered both column expressions and relationships, how one
would achieve the  effect above of only blocking relationship loads, without
new API being added.   So to keep things simple, the option for columns
remains on :func:`.defer`:

    :func:`.raiseload` - query option to raise for relationship loads

    :paramref:`.orm.defer.raiseload` - query option to raise for column expression loads


As part of this change, the behavior of "deferred" in conjunction with
attribute expiration has changed.   Previously, when an object would be marked
as expired, and then unexpired via the access of one of the expired attributes,
attributes which were mapped as "deferred" at the mapper level would also load.
This has been changed such that an attribute that is deferred in the mapping
will never "unexpire", it only loads when accessed as part of the deferral
loader.

An attribute that is not mapped as "deferred", however was deferred at query
time via the :func:`.defer` option, will be reset when the object or attribute
is expired; that is, the deferred option is removed. This is the same behavior
as was present previously.


.. seealso::

    :ref:`deferred_raiseload`

:ticket:`4826`

.. _change_5263:

ORM Batch inserts with psycopg2 now batch statements with RETURNING in most cases
---------------------------------------------------------------------------------

The change in :ref:`change_5401` adds support for "executemany" + "RETURNING"
at the same time in Core, which is now enabled for the psycopg2 dialect
by default using the psycopg2 ``execute_values()`` extension.   The ORM flush
process now makes use of this feature such that the retrieval of newly generated
primary key values and server defaults can be achieved while not losing the
performance benefits of being able to batch INSERT statements together.  Additionally,
psycopg2's ``execute_values()`` extension itself provides a five-fold performance
improvement over psycopg2's default "executemany" implementation, by rewriting
an INSERT statement to include many "VALUES" expressions all in one statement
rather than invoking the same statement repeatedly, as psycopg2 lacks the ability
to PREPARE the statement ahead of time as would normally be expected for this
approach to be performant.

SQLAlchemy includes a :ref:`performance suite <examples_performance>` within
its examples, where we can compare the times generated for the "batch_inserts"
runner against 1.3 and 1.4, revealing a 3x-5x speedup for most flavors
of batch insert::

    # 1.3
    $ python -m examples.performance bulk_inserts --dburl postgresql://scott:tiger@localhost/test
    test_flush_no_pk : (100000 iterations); total time 14.051527 sec
    test_bulk_save_return_pks : (100000 iterations); total time 15.002470 sec
    test_flush_pk_given : (100000 iterations); total time 7.863680 sec
    test_bulk_save : (100000 iterations); total time 6.780378 sec
    test_bulk_insert_mappings :  (100000 iterations); total time 5.363070 sec
    test_core_insert : (100000 iterations); total time 5.362647 sec

    # 1.4 with enhancement
    $ python -m examples.performance bulk_inserts --dburl postgresql://scott:tiger@localhost/test
    test_flush_no_pk : (100000 iterations); total time 3.820807 sec
    test_bulk_save_return_pks : (100000 iterations); total time 3.176378 sec
    test_flush_pk_given : (100000 iterations); total time 4.037789 sec
    test_bulk_save : (100000 iterations); total time 2.604446 sec
    test_bulk_insert_mappings : (100000 iterations); total time 1.204897 sec
    test_core_insert : (100000 iterations); total time 0.958976 sec

Note that the ``execute_values()`` extension modifies the INSERT statement in the psycopg2
layer, **after** it's been logged by SQLAlchemy.  So with SQL logging, one will see the
parameter sets batched together, but the joining of multiple "values" will not be visible
on the application side::

    2020-06-27 19:08:18,166 INFO sqlalchemy.engine.Engine INSERT INTO a (data) VALUES (%(data)s) RETURNING a.id
    2020-06-27 19:08:18,166 INFO sqlalchemy.engine.Engine [generated in 0.00698s] ({'data': 'data 1'}, {'data': 'data 2'}, {'data': 'data 3'}, {'data': 'data 4'}, {'data': 'data 5'}, {'data': 'data 6'}, {'data': 'data 7'}, {'data': 'data 8'}  ... displaying 10 of 4999 total bound parameter sets ...  {'data': 'data 4998'}, {'data': 'data 4999'})
    2020-06-27 19:08:18,254 INFO sqlalchemy.engine.Engine COMMIT

The ultimate INSERT statement can be seen by enabling statement logging on the PostgreSQL side::

    2020-06-27 19:08:18.169 EDT [26960] LOG:  statement: INSERT INTO a (data)
    VALUES ('data 1'),('data 2'),('data 3'),('data 4'),('data 5'),('data 6'),('data
    7'),('data 8'),('data 9'),('data 10'),('data 11'),('data 12'),
    ... ('data 999'),('data 1000') RETURNING a.id

    2020-06-27 19:08:18.175 EDT
    [26960] LOG:  statement: INSERT INTO a (data) VALUES ('data 1001'),('data
    1002'),('data 1003'),('data 1004'),('data 1005 '),('data 1006'),('data
    1007'),('data 1008'),('data 1009'),('data 1010'),('data 1011'), ...

The feature batches rows into groups of 1000 by default which can be affected
using the ``executemany_values_page_size`` argument documented at
:ref:`psycopg2_executemany_mode`.

:ticket:`5263`


.. _change_orm_update_returning_14:

ORM Bulk Update and Delete use RETURNING for "fetch" strategy when available
----------------------------------------------------------------------------

An ORM bulk update or delete that uses the "fetch" strategy::

    sess.query(User).filter(User.age > 29).update(
        {"age": User.age - 10}, synchronize_session="fetch"
    )

Will now use RETURNING if the backend database supports it; this currently
includes PostgreSQL and SQL Server (the Oracle dialect does not support RETURNING
of multiple rows)::

    UPDATE users SET age_int=(users.age_int - %(age_int_1)s) WHERE users.age_int > %(age_int_2)s RETURNING users.id
    [generated in 0.00060s] {'age_int_1': 10, 'age_int_2': 29}
    Col ('id',)
    Row (2,)
    Row (4,)

For backends that do not support RETURNING of multiple rows, the previous approach
of emitting SELECT for the primary keys beforehand is still used::

    SELECT users.id FROM users WHERE users.age_int > %(age_int_1)s
    [generated in 0.00043s] {'age_int_1': 29}
    Col ('id',)
    Row (2,)
    Row (4,)
    UPDATE users SET age_int=(users.age_int - %(age_int_1)s) WHERE users.age_int > %(age_int_2)s
    [generated in 0.00102s] {'age_int_1': 10, 'age_int_2': 29}

One of the intricate challenges of this change is to support cases such as the
horizontal sharding extension, where a single bulk update or delete may be
multiplexed among backends some of which support RETURNING and some don't.   The
new 1.4 execution architecture supports this case so that the "fetch" strategy
can be left intact with a graceful degrade to using a SELECT, rather than having
to add a new "returning" strategy that would not be backend-agnostic.

As part of this change, the "fetch" strategy is also made much more efficient
in that it will no longer expire the objects located which match the rows,
for Python expressions used in the SET clause which can be evaluated in
Python; these are instead assigned
directly onto the object in the same way as the "evaluate" strategy.  Only
for SQL expressions that can't be evaluated does it fall back to expiring
the attributes.   The "evaluate" strategy has also been enhanced to fall back
to "expire" for a value that cannot be evaluated.


Behavioral Changes - ORM
========================

.. _change_4710_orm:

The "KeyedTuple" object returned by Query is replaced by Row
-------------------------------------------------------------

As discussed at :ref:`change_4710_core`, the Core :class:`.RowProxy` object
is now replaced by a class called :class:`.Row`.    The base :class:`.Row`
object now behaves more fully like a named tuple, and as such it is now
used as the basis for tuple-like results returned by the :class:`_query.Query`
object, rather than the previous "KeyedTuple" class.

The rationale is so that by SQLAlchemy 2.0, both Core and ORM SELECT statements
will return result rows using the same :class:`.Row` object which behaves  like
a named tuple.  Dictionary-like functionality is available from :class:`.Row`
via the :attr:`.Row._mapping` attribute.   In the interim, Core result sets
will make use of a :class:`.Row` subclass :class:`.LegacyRow` which maintains
the previous dict/tuple hybrid behavior for backwards compatibility while the
:class:`.Row` class will be used directly for ORM tuple results returned
by the :class:`_query.Query` object.

Effort has been made to get most of the featureset of :class:`.Row` to be
available within the ORM, meaning that access by string name as well
as entity / column should work::

    row = s.query(User, Address).join(User.addresses).first()

    row._mapping[User]  # same as row[0]
    row._mapping[Address]  # same as row[1]
    row._mapping["User"]  # same as row[0]
    row._mapping["Address"]  # same as row[1]

    u1 = aliased(User)
    row = s.query(u1).only_return_tuples(True).first()
    row._mapping[u1]  # same as row[0]


    row = (
        s.query(User.id, Address.email_address)
        .join(User.addresses)
        .first()
    )

    row._mapping[User.id]  # same as row[0]
    row._mapping["id"]  # same as row[0]
    row._mapping[users.c.id]  # same as row[0]

.. seealso::

    :ref:`change_4710_core`

:ticket:`4710`.

.. _change_5074:

Session features new "autobegin" behavior
-----------------------------------------

The :class:`.Session` object's default behavior of ``autocommit=False``
historically has meant that there is always a :class:`.SessionTransaction`
object in play, associated with the :class:`.Session` via the
:attr:`.Session.transaction` attribute.   When the given
:class:`.SessionTransaction` was complete, due to a commit, rollback, or close,
it was immediately replaced with a new one.  The :class:`.SessionTransaction`
by itself does not imply the usage of any connection-oriented resources, so
this long-standing behavior has a particular elegance to it in that the state
of :attr:`.Session.transaction` is always predictable as non-None.

However, as part of the initiative in :ticket:`5056` to greatly reduce
reference cycles, this assumption means that calling upon
:meth:`.Session.close` results in a :class:`.Session` object that still has
reference cycles and is more expensive to clean up, not to mention that there
is a small overhead in constructing the :class:`.SessionTransaction`
object, which meant that there would be unnecessary overhead created
for a :class:`.Session` that for example invoked :meth:`.Session.commit`
and then :meth:`.Session.close`.

As such, it was decided that :meth:`.Session.close` should leave the internal
state of ``self.transaction``, now referred to internally as
``self._transaction``, as None, and that a new :class:`.SessionTransaction`
should only be created when needed.  For consistency and code coverage, this
behavior was also expanded to include all the points at which "autobegin" is
expected, not just when :meth:`.Session.close` were called.

In particular, this causes a behavioral change for applications which
subscribe to the :meth:`.SessionEvents.after_transaction_create` event hook;
previously, this event would be emitted when the :class:`.Session` were  first
constructed, as well as for most actions that closed the previous transaction
and would emit :meth:`.SessionEvents.after_transaction_end`.  The new behavior
is that :meth:`.SessionEvents.after_transaction_create` is emitted on demand,
when the :class:`.Session` has not yet created a  new
:class:`.SessionTransaction` object and mapped objects are associated with the
:class:`.Session` through methods like :meth:`.Session.add` and
:meth:`.Session.delete`, when  the :attr:`.Session.transaction` attribute is
called upon, when the :meth:`.Session.flush` method has tasks to complete, etc.

Besides the change in when the :meth:`.SessionEvents.after_transaction_create`
event is emitted, the change should have no other user-visible impact on the
:class:`.Session` object's behavior; the :class:`.Session` will continue to have
the behavior that it remains usable for new operations after :meth:`.Session.close`
is called, and the sequencing of how the :class:`.Session` interacts with the
:class:`_engine.Engine` and the database itself should also remain unaffected, since
these operations were already operating in an on-demand fashion.

:ticket:`5074`

.. _change_5237_14:

Viewonly relationships don't synchronize backrefs
-------------------------------------------------

In :ticket:`5149` in 1.3.14, SQLAlchemy began emitting a warning when the
:paramref:`_orm.relationship.backref` or :paramref:`_orm.relationship.back_populates`
keywords would be used at the same time as the :paramref:`_orm.relationship.viewonly`
flag on the target relationship.  This was because a "viewonly" relationship does
not actually persist changes made to it, which could cause some misleading
behaviors to occur.  However, in :ticket:`5237`, we sought to refine this
behavior as there are legitimate use cases to have backrefs set up on
viewonly relationships, including that back populates attributes are used
in some cases by the relationship lazy loaders to determine that an additional
eager load in the other direction is not necessary, as well as that back
populates can be used for mapper introspection and that :func:`_orm.backref`
can be a convenient way to set up bi-directional relationships.

The solution then was to make the "mutation" that occurs from a backref
an optional thing, using the :paramref:`_orm.relationship.sync_backref`
flag.  In 1.4 the value of :paramref:`_orm.relationship.sync_backref` defaults
to False for a relationship target that also sets :paramref:`_orm.relationship.viewonly`.
This indicates that any changes made to a relationship with
viewonly will not impact the state of the other side or of the :class:`_orm.Session`
in any way::


    class User(Base):
        # ...

        addresses = relationship(Address, backref=backref("user", viewonly=True))

    class Address(Base):
        # ...


    u1 = session.query(User).filter_by(name="x").first()

    a1 = Address()
    a1.user = u1

Above, the ``a1`` object will **not** be added to the ``u1.addresses``
collection, nor will the ``a1`` object be added to the session.  Previously,
both of these things would be true.   The warning that
:paramref:`.relationship.sync_backref` should be set to ``False`` when
:paramref:`.relationship.viewonly` is ``False`` is no longer emitted as this is
now the default behavior.

:ticket:`5237`

.. _change_5150:

cascade_backrefs behavior deprecated for removal in 2.0
-------------------------------------------------------

SQLAlchemy has long had a behavior of cascading objects into the
:class:`_orm.Session` based on backref assignment.   Given ``User`` below
already in a :class:`_orm.Session`, assigning it to the ``Address.user``
attribute of an ``Address`` object, assuming a bidirectional relationship
is set up, would mean that the ``Address`` also gets put into the
:class:`_orm.Session` at that point::

    u1 = User()
    session.add(u1)

    a1 = Address()
    a1.user = u1  # <--- adds "a1" to the Session

The above behavior was an unintended side effect of backref behavior, in that
since ``a1.user`` implies ``u1.addresses.append(a1)``, ``a1`` would get
cascaded into the :class:`_orm.Session`.  This remains the default behavior
throughout 1.4.     At some point, a new flag :paramref:`_orm.relationship.cascade_backrefs`
was added to disable to above behavior, as it can be surprising and also gets in
the way of some operations where the object would be placed in the :class:`_orm.Session`
too early and get prematurely flushed.

In 2.0, the default behavior will be that "cascade_backrefs" is False, and
additionally there will be no "True" behavior as this is not generally a desirable
behavior.    When 2.0 deprecation warnings are enabled, a warning will be emitted
when a "backref cascade" actually takes place.    To get the new behavior, either
set :paramref:`_orm.relationship.cascade_backrefs` to ``False`` on the target
relationship, as is already supported in 1.3 and earlier, or alternatively make
use of the :paramref:`_orm.Session.future` flag to :term:`2.0-style` mode::

    Session = sessionmaker(engine, future=True)

    with Session() as session:
      u1 = User()
      session.add(u1)

      a1 = Address()
      a1.user = u1  # <--- will not add "a1" to the Session



:ticket:`5150`

.. _change_1763:

Eager loaders emit during unexpire operations
---------------------------------------------

A long sought behavior was that when an expired object is accessed, configured
eager loaders will run in order to eagerly load relationships on the expired
object when the object is refreshed or otherwise unexpired.   This behavior has
now been added, so that joinedloaders will add inline JOINs as usual, and
selectin/subquery loaders will run an "immediateload" operation for a given
relationship, when an expired object is unexpired or an object is refreshed::

    >>> a1 = session.query(A).options(joinedload(A.bs)).first()
    >>> a1.data = 'new data'
    >>> session.commit()

Above, the ``A`` object was loaded with a ``joinedload()`` option associated
with it in order to eagerly load the ``bs`` collection.    After the
``session.commit()``, the state of the object is expired.  Upon accessing
the ``.data`` column attribute, the object is refreshed and this will now
include the joinedload operation as well::

    >>> a1.data
    SELECT a.id AS a_id, a.data AS a_data, b_1.id AS b_1_id, b_1.a_id AS b_1_a_id
    FROM a LEFT OUTER JOIN b AS b_1 ON a.id = b_1.a_id
    WHERE a.id = ?

The behavior applies both to loader strategies applied to the
:func:`_orm.relationship` directly, as well as with options used with
:meth:`_query.Query.options`, provided that the object was originally loaded by that
query.

For the "secondary" eager loaders "selectinload" and "subqueryload", the SQL
strategy for these loaders is not necessary in order to eagerly load attributes
on a single object; so they will instead invoke the "immediateload" strategy in
a refresh scenario, which resembles the query emitted by "lazyload", emitted as
an additional query::

    >>> a1 = session.query(A).options(selectinload(A.bs)).first()
    >>> a1.data = 'new data'
    >>> session.commit()
    >>> a1.data
    SELECT a.id AS a_id, a.data AS a_data
    FROM a
    WHERE a.id = ?
    (1,)
    SELECT b.id AS b_id, b.a_id AS b_a_id
    FROM b
    WHERE ? = b.a_id
    (1,)

Note that a loader option does not apply to an object that was introduced
into the :class:`.Session` in a different way.  That is, if the ``a1`` object
were just persisted in this :class:`.Session`, or was loaded with a different
query before the eager option had been applied, then the object doesn't have
an eager load option associated with it.  This is not a new concept, however
users who are looking for the eagerload on refresh behavior may find this
to be more noticeable.

:ticket:`1763`

.. _change_4519:

Accessing an uninitialized collection attribute on a transient object no longer mutates __dict__
-------------------------------------------------------------------------------------------------

It has always been SQLAlchemy's behavior that accessing mapped attributes on a
newly created object returns an implicitly generated value, rather than raising
``AttributeError``, such as ``None`` for scalar attributes or ``[]`` for a
list-holding relationship::

    >>> u1 = User()
    >>> u1.name
    None
    >>> u1.addresses
    []

The rationale for the above behavior was originally to make ORM objects easier
to work with.  Since an ORM object represents an empty row when first created
without any state, it is intuitive that its un-accessed attributes would
resolve to ``None`` (or SQL NULL) for scalars and to empty collections for
relationships.   In particular, it makes possible an extremely common pattern
of being able to mutate the new collection without manually creating and
assigning an empty collection first::

    >>> u1 = User()
    >>> u1.addresses.append(Address())  # no need to assign u1.addresses = []

Up until version 1.0 of SQLAlchemy, the behavior of this initialization  system
for both scalar attributes as well as collections would be that the ``None`` or
empty collection would be *populated* into the object's  state, e.g.
``__dict__``.  This meant that the following two operations were equivalent::

    >>> u1 = User()
    >>> u1.name = None  # explicit assignment

    >>> u2 = User()
    >>> u2.name  # implicit assignment just by accessing it
    None

Where above, both ``u1`` and ``u2`` would have the value ``None`` populated
in the value of the ``name`` attribute.  Since this is a SQL NULL, the ORM
would skip including these values within an INSERT so that SQL-level defaults
take place, if any, else the value defaults to NULL on the database side.

In version 1.0 as part of :ref:`migration_3061`, this behavior was refined so
that the ``None`` value was no longer populated into ``__dict__``, only
returned.   Besides removing the mutating side effect of a getter operation,
this change also made it possible to set columns that did have server defaults
to the value NULL by actually assigning ``None``, which was now distinguished
from just reading it.

The change however did not accommodate for collections, where returning an
empty collection that is not assigned meant that this mutable collection would
be different each time and also would not be able to correctly accommodate for
mutating operations (e.g. append, add, etc.) called upon it.    While the
behavior continued to generally not get in anyone's way, an edge case was
eventually identified in :ticket:`4519` where this empty collection could be
harmful, which is when the object is merged into a session::

    >>> u1 = User(id=1)  # create an empty User to merge with id=1 in the database
    >>> merged1 = session.merge(u1)  # value of merged1.addresses is unchanged from that of the DB

    >>> u2 = User(id=2) # create an empty User to merge with id=2 in the database
    >>> u2.addresses
    []
    >>> merged2 = session.merge(u2)  # value of merged2.addresses has been emptied in the DB

Above, the ``.addresses`` collection on ``merged1`` will contain all the
``Address()`` objects that were already in the database.   ``merged2`` will
not; because it has an empty list implicitly assigned, the ``.addresses``
collection will be erased.   This is an example of where this mutating side
effect can actually mutate the database itself.

While it was considered that perhaps the attribute system should begin using
strict "plain Python" behavior, raising ``AttributeError`` in all cases for
non-existent attributes on non-persistent objects and requiring that  all
collections be explicitly assigned, such a change would likely be too extreme
for the vast number of applications that have relied upon this  behavior for
many years, leading to a complex rollout / backwards compatibility problem as
well as the likelihood that workarounds to restore the old behavior would
become prevalent, thus rendering the whole change ineffective in any case.

The change then is to keep the default producing behavior, but to finally make
the non-mutating behavior of scalars a reality for collections as well, via the
addition of additional mechanics in the collection system.  When accessing the
empty attribute, the new collection is created and associated with the state,
however is not added to ``__dict__`` until it is actually mutated::

    >>> u1 = User()
    >>> l1 = u1.addresses  # new list is created, associated with the state
    >>> assert u1.addresses is l1  # you get the same list each time you access it
    >>> assert "addresses" not in u1.__dict__  # but it won't go into __dict__ until it's mutated
    >>> from sqlalchemy import inspect
    >>> inspect(u1).attrs.addresses.history
    History(added=None, unchanged=None, deleted=None)

When the list is changed, then it becomes part of the tracked changes to
be persisted to the database::

    >>> l1.append(Address())
    >>> assert "addresses" in u1.__dict__
    >>> inspect(u1).attrs.addresses.history
    History(added=[<__main__.Address object at 0x7f49b725eda0>], unchanged=[], deleted=[])

This change is expected to have *nearly* no impact on existing applications
in any way, except that it has been observed that some applications may be
relying upon the implicit assignment of this collection, such as to assert that
the object contains certain values based on its ``__dict__``::

    >>> u1 = User()
    >>> u1.addresses
    []
    # this will now fail, would pass before
    >>> assert {k: v for k, v in u1.__dict__.items() if not k.startswith("_")} == {"addresses": []}

or to ensure that the collection won't require a lazy load to proceed, the
(admittedly awkward) code below will now also fail::

    >>> u1 = User()
    >>> u1.addresses
    []
    >>> s.add(u1)
    >>> s.flush()
    >>> s.close()
    >>> u1.addresses  # <-- will fail, .addresses is not loaded and object is detached

Applications that rely upon the implicit mutating behavior of collections will
need to be changed so that they assign the desired collection explicitly::

    >>> u1.addresses = []

:ticket:`4519`

.. _change_4662:

The "New instance conflicts with existing identity" error is now a warning
---------------------------------------------------------------------------

SQLAlchemy has always had logic to detect when an object in the :class:`.Session`
to be inserted has the same primary key as an object that is already present::

    class Product(Base):
        __tablename__ = 'product'

        id = Column(Integer, primary_key=True)

    session = Session(engine)

    # add Product with primary key 1
    session.add(Product(id=1))
    session.flush()

    # add another Product with same primary key
    session.add(Product(id=1))
    s.commit()  # <-- will raise FlushError

The change is that the :class:`.FlushError` is altered to be only a warning::

    sqlalchemy/orm/persistence.py:408: SAWarning: New instance <Product at 0x7f1ff65e0ba8> with identity key (<class '__main__.Product'>, (1,), None) conflicts with persistent instance <Product at 0x7f1ff60a4550>


Subsequent to that, the condition will attempt to insert the row into the
database which will emit :class:`.IntegrityError`, which is the same error that
would be raised if the primary key identity was not already present in the
:class:`.Session`::

    sqlalchemy.exc.IntegrityError: (sqlite3.IntegrityError) UNIQUE constraint failed: product.id

The rationale is to allow code that is using :class:`.IntegrityError` to catch
duplicates to function regardless of the existing state of the
:class:`.Session`, as is often done using savepoints::


    # add another Product with same primary key
    try:
        with session.begin_nested():
            session.add(Product(id=1))
    except exc.IntegrityError:
        print("row already exists")

The above logic was not fully feasible earlier, as in the case that the
``Product`` object with the existing identity were already in the
:class:`.Session`, the code would also have to catch :class:`.FlushError`,
which additionally is not filtered for the specific condition of integrity
issues.   With the change, the above block behaves consistently with the
exception of the warning also being emitted.

Since the logic in question deals with the primary key, all databases emit an
integrity error in the case of primary key conflicts on INSERT.    The case
where an error would not be raised, that would have earlier, is the extremely
unusual scenario of a mapping that defines a primary key on the mapped
selectable that is more restrictive than what is actually configured in the
database schema, such as when mapping to joins of tables or when defining
additional columns as part of a composite primary key that is not actually
constrained in the database schema. However, these situations also work  more
consistently in that the INSERT would theoretically proceed whether or not the
existing identity were still in the database.  The warning can also be
configured to raise an exception using the Python warnings filter.


:ticket:`4662`

.. _change_4994:

Persistence-related cascade operations disallowed with viewonly=True
---------------------------------------------------------------------

When a :func:`_orm.relationship` is set as ``viewonly=True`` using the
:paramref:`_orm.relationship.viewonly` flag, it indicates this relationship should
only be used to load data from the database, and should not be mutated
or involved in a persistence operation.   In order to ensure this contract
works successfully, the relationship can no longer specify
:paramref:`_orm.relationship.cascade` settings that make no sense in terms of
"viewonly".

The primary targets here are the "delete, delete-orphan"  cascades, which
through 1.3 continued to impact persistence even if viewonly were True, which
is a bug; even if viewonly were True, an object would still cascade these
two operations onto the related object if the parent were deleted or the
object were detached.   Rather than modify the cascade operations to check
for viewonly, the configuration of both of these together is simply
disallowed::

    class User(Base):
        # ...

        # this is now an error
        addresses = relationship(
            "Address", viewonly=True, cascade="all, delete-orphan")

The above will raise::

    sqlalchemy.exc.ArgumentError: Cascade settings
    "delete, delete-orphan, merge, save-update" apply to persistence
    operations and should not be combined with a viewonly=True relationship.

Applications that have this issue should be emitting a warning as of
SQLAlchemy 1.3.12, and for the above error the solution is to remove
the cascade settings for a viewonly relationship.


:ticket:`4993`
:ticket:`4994`

.. _change_5122:

Stricter behavior when querying inheritance mappings using custom queries
-------------------------------------------------------------------------

This change applies to the scenario where a joined- or single- table
inheritance subclass entity is being queried, given a completed SELECT subquery
to select from.   If the given subquery returns rows that do not correspond to
the requested polymorphic identity or identities, an error is raised.
Previously, this condition would pass silently under joined table inheritance,
returning an invalid subclass, and under single table inheritance, the
:class:`_query.Query` would be adding additional criteria against the subquery to
limit the results which could inappropriately interfere with the intent of the
query.

Given the example mapping of ``Employee``, ``Engineer(Employee)``, ``Manager(Employee)``,
in the 1.3 series if we were to emit the following query against a joined
inheritance mapping::

    s = Session(e)

    s.add_all([Engineer(), Manager()])

    s.commit()

    print(
        s.query(Manager).select_entity_from(s.query(Employee).subquery()).all()
    )


The subquery selects both the ``Engineer`` and the ``Manager`` rows, and
even though the outer query is against ``Manager``, we get a non ``Manager``
object back::

    SELECT anon_1.type AS anon_1_type, anon_1.id AS anon_1_id
    FROM (SELECT employee.type AS type, employee.id AS id
    FROM employee) AS anon_1
    2020-01-29 18:04:13,524 INFO sqlalchemy.engine.base.Engine ()
    [<__main__.Engineer object at 0x7f7f5b9a9810>, <__main__.Manager object at 0x7f7f5b9a9750>]

The new behavior is that this condition raises an error::

    sqlalchemy.exc.InvalidRequestError: Row with identity key
    (<class '__main__.Employee'>, (1,), None) can't be loaded into an object;
    the polymorphic discriminator column '%(140205120401296 anon)s.type'
    refers to mapped class Engineer->engineer, which is not a sub-mapper of
    the requested mapped class Manager->manager

The above error only raises if the primary key columns of that entity are
non-NULL.  If there's no primary key for a given entity in a row, no attempt
to construct an entity is made.

In the case of single inheritance mapping, the change in behavior is slightly
more involved;   if ``Engineer`` and ``Manager`` above are mapped with
single table inheritance, in 1.3 the following query would be emitted and
only a ``Manager`` object is returned::

    SELECT anon_1.type AS anon_1_type, anon_1.id AS anon_1_id
    FROM (SELECT employee.type AS type, employee.id AS id
    FROM employee) AS anon_1
    WHERE anon_1.type IN (?)
    2020-01-29 18:08:32,975 INFO sqlalchemy.engine.base.Engine ('manager',)
    [<__main__.Manager object at 0x7ff1b0200d50>]

The :class:`_query.Query` added the "single table inheritance" criteria to the
subquery, editorializing on the intent that was originally set up by it.
This behavior was added in version 1.0 in :ticket:`3891`, and creates a
behavioral inconsistency between "joined" and "single" table inheritance,
and additionally modifies the intent of the given query, which may intend
to return additional rows where the columns that correspond to the inheriting
entity are NULL, which is a valid use case.    The behavior is now equivalent
to that of joined table inheritance, where it is assumed that the subquery
returns the correct rows and an error is raised if an unexpected polymorphic
identity is encountered::

    SELECT anon_1.type AS anon_1_type, anon_1.id AS anon_1_id
    FROM (SELECT employee.type AS type, employee.id AS id
    FROM employee) AS anon_1
    2020-01-29 18:13:10,554 INFO sqlalchemy.engine.base.Engine ()
    Traceback (most recent call last):
    # ...
    sqlalchemy.exc.InvalidRequestError: Row with identity key
    (<class '__main__.Employee'>, (1,), None) can't be loaded into an object;
    the polymorphic discriminator column '%(140700085268432 anon)s.type'
    refers to mapped class Engineer->employee, which is not a sub-mapper of
    the requested mapped class Manager->employee

The correct adjustment to the situation as presented above which worked on 1.3
is to adjust the given subquery to correctly filter the rows based on the
discriminator column::

    print(
        s.query(Manager).select_entity_from(
            s.query(Employee).filter(Employee.discriminator == 'manager').
            subquery()).all()
    )

    SELECT anon_1.type AS anon_1_type, anon_1.id AS anon_1_id
    FROM (SELECT employee.type AS type, employee.id AS id
    FROM employee
    WHERE employee.type = ?) AS anon_1
    2020-01-29 18:14:49,770 INFO sqlalchemy.engine.base.Engine ('manager',)
    [<__main__.Manager object at 0x7f70e13fca90>]


:ticket:`5122`

Dialect Changes
===============

psycopg2 version 2.7 or higher is required for the PostgreSQL psycopg2 dialect
------------------------------------------------------------------------------

The psycopg2 dialect relies upon many features of psycopg2 released
in the past few years.  To simplify the dialect, version 2.7, released
in March, 2017 is now the minimum version required.

.. _change_5401:

psycopg2 dialect features "execute_values" with RETURNING for INSERT statements by default
------------------------------------------------------------------------------------------

The first half of a significant performance enhancement for PostgreSQL when
using both Core and ORM, the psycopg2 dialect now uses
``psycopg2.extras.execute_values()`` by default for compiled INSERT statements
and also implements RETURNING support in this mode.   The other half of this
change is :ref:`change_5263` which allows the ORM to take advantage of
RETURNING with executemany (i.e. batching of INSERT statements) so that ORM
bulk inserts with psycopg2 are up to 400% faster depending on specifics.

This extension method allows many rows to be INSERTed within a single
statement, using an extended VALUES clause for the statement.  While
SQLAlchemy's :func:`_sql.insert` construct already supports this syntax via
the :meth:`_sql.Insert.values` method, the extension method allows the
construction of the VALUES clause to occur dynamically when the statement
is executed as an "executemany" execution, which is what occurs when one
passes a list of parameter dictionaries to :meth:`_engine.Connection.execute`.
It also occurs beyond the cache boundary so that the INSERT statement may
be cached before the VALUES are rendered.

A quick test of the ``execute_values()`` approach using the
``bulk_inserts.py`` script in the :ref:`examples_performance` example
suite reveals an approximate **fivefold performance increase**::

    $ python -m examples.performance bulk_inserts --test test_core_insert --num 100000 --dburl postgresql://scott:tiger@localhost/test

    # 1.3
    test_core_insert : A single Core INSERT construct inserting mappings in bulk. (100000 iterations); total time 5.229326 sec

    # 1.4
    test_core_insert : A single Core INSERT construct inserting mappings in bulk. (100000 iterations); total time 0.944007 sec

Support for the "batch" extension was added in version 1.2 in
:ref:`change_4109`, and enhanced to include support for the ``execute_values``
extension in 1.3 in :ticket:`4623`.  In 1.4 the ``execute_values`` extension is
now being turned on by default for INSERT statements; the "batch" extension
for UPDATE and DELETE remains off by default.

In addition, the ``execute_values`` extension function supports returning the
rows that are generated by RETURNING as an aggregated list.  The psycopg2
dialect will now retrieve this list if the given :func:`_sql.insert` construct
requests returning via the :meth:`.Insert.returning` method or similar methods
intended to return generated defaults; the rows are then installed in the
result so that they are retrieved as though they came from the cursor
directly.   This allows tools like the ORM to use batched inserts in all cases,
which is expected to provide a dramatic performance improvement.


The ``executemany_mode`` feature of the psycopg2 dialect has been revised
with the following changes:

* A new mode ``"values_only"`` is added.  This mode uses the very performant
  ``psycopg2.extras.execute_values()`` extension method for compiled INSERT
  statements run with executemany(), but does not use ``execute_batch()`` for
  UPDATE and DELETE statements.  This new mode is now the default setting for
  the psycopg2 dialect.

* The existing ``"values"`` mode is now named ``"values_plus_batch"``.  This mode
  will use ``execute_values`` for INSERT statements and ``execute_batch``
  for UPDATE and DELETE statements.  The mode is not enabled by default
  because it disables the proper functioning of ``cursor.rowcount`` with
  UPDATE and DELETE statements executed with ``executemany()``.

* RETURNING support is enabled for ``"values_only"`` and ``"values"`` for
  INSERT statements.  The psycopg2 dialect will receive the rows back
  from psycopg2 using the fetch=True flag and install them into the result
  set as though they came directly from the cursor (which they ultimately did,
  however psycopg2's extension function has aggregated multiple batches into
  one list).

* The default "page_size" setting for ``execute_values`` has been increased
  from 100 to 1000.   The default remains at 100 for the ``execute_batch``
  function.  These parameters may both be modified as was the case before.

* The ``use_batch_mode`` flag that was part of the 1.2 version of the feature
  is removed; the behavior remains controllable via the ``executemany_mode``
  flag added in 1.3.

* The Core engine and dialect has been enhanced to support executemany
  plus returning mode, currently only available with psycopg2, by providing
  new :attr:`_engine.CursorResult.inserted_primary_key_rows` and
  :attr:`_engine.CursorResult.returned_default_rows` accessors.

.. seealso::

    :ref:`psycopg2_executemany_mode`


:ticket:`5401`


.. _change_4895:

Removed "join rewriting" logic from SQLite dialect; updated imports
-------------------------------------------------------------------

Dropped support for right-nested join rewriting to support old SQLite
versions prior to 3.7.16, released in 2013.   It is not expected that
any modern Python versions rely upon this limitation.

The behavior was first introduced in 0.9 and was part of the larger change of
allowing for right nested joins as described at :ref:`feature_joins_09`.
However the SQLite workaround produced many regressions in the 2013-2014
period due to its complexity. In 2016, the dialect was modified so that the
join rewriting logic would only occur for SQLite versions prior to 3.7.16 after
bisection was used to  identify where SQLite fixed its support for this
construct, and no further issues were reported against the behavior (even
though some bugs were found internally).    It is now anticipated that there
are little to no Python builds for Python 2.7 or 3.5 and above (the supported
Python versions) which would include a SQLite version prior to 3.7.17, and
the behavior is only necessary only in more complex ORM joining scenarios.
A warning is now emitted if the installed SQLite version is older than
3.7.16.

In related changes, the module imports for SQLite no longer attempt to
import the "pysqlite2" driver on Python 3 as this driver does not exist
on Python 3; a very old warning for old pysqlite2 versions is also dropped.

:ticket:`4895`


.. _change_4976:

Added Sequence support for MariaDB 10.3
----------------------------------------

The MariaDB database as of 10.3 supports sequences.   SQLAlchemy's MySQL
dialect now implements support for the :class:`.Sequence` object against this
database, meaning "CREATE SEQUENCE" DDL will be emitted for a
:class:`.Sequence` that is present in a :class:`_schema.Table` or :class:`_schema.MetaData`
collection in the same way as it works for backends such as PostgreSQL, Oracle,
when the dialect's server version check has confirmed the database is MariaDB
10.3 or greater.    Additionally, the :class:`.Sequence` will act as a
column default and primary key generation object when used in these ways.

Since this change will impact the assumptions both for DDL as well as the
behavior of INSERT statements for an application that is currently deployed
against MariaDB 10.3 which also happens to make explicit use the
:class:`.Sequence` construct within its table definitions, it is important to
note that :class:`.Sequence` supports a flag :paramref:`.Sequence.optional`
which is used to limit the scenarios in which the :class:`.Sequence` to take
effect. When "optional" is used on a :class:`.Sequence` that is present in the
integer primary key column of a table::

    Table(
        "some_table", metadata,
        Column("id", Integer, Sequence("some_seq", optional=True), primary_key=True)
    )

The above :class:`.Sequence` is only used for DDL and INSERT statements if the
target database does not support any other means of generating integer primary
key values for the column.  That is, the Oracle database above would use the
sequence, however the PostgreSQL and MariaDB 10.3 databases would not. This may
be important for an existing application that is upgrading to SQLAlchemy 1.4
which may not have emitted DDL for this :class:`.Sequence` against its backing
database, as an INSERT statement will fail if it seeks to use a sequence that
was not created.


.. seealso::

    :ref:`defaults_sequences`

:ticket:`4976`


.. _change_4235:

Added Sequence support distinct from IDENTITY to SQL Server
-----------------------------------------------------------

The :class:`.Sequence` construct is now fully functional with Microsoft
SQL Server.  When applied to a :class:`.Column`, the DDL for the table will
no longer include IDENTITY keywords and instead will rely upon "CREATE SEQUENCE"
to ensure a sequence is present which will then be used for INSERT statements
on the table.

The :class:`.Sequence` prior to version 1.3 was used to control parameters for
the IDENTITY column in SQL Server; this usage emitted deprecation warnings
throughout 1.3 and is now removed in 1.4.  For control of parameters for an
IDENTITY column, the ``mssql_identity_start`` and ``mssql_identity_increment``
parameters should be used; see the MSSQL dialect documentation linked below.


.. seealso::

    :ref:`mssql_identity`

:ticket:`4235`

:ticket:`4633`
