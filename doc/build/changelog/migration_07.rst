=============================
What's New in SQLAlchemy 0.7?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.6,
    last released May 5, 2012, and SQLAlchemy version 0.7,
    undergoing maintenance releases as of October, 2012.

    Document date: July 27, 2011

Introduction
============

This guide introduces what's new in SQLAlchemy version 0.7,
and also documents changes which affect users migrating
their applications from the 0.6 series of SQLAlchemy to 0.7.

To as great a degree as possible, changes are made in such a
way as to not break compatibility with applications built
for 0.6.   The changes that are necessarily not backwards
compatible are very few, and all but one, the change to
mutable attribute defaults, should affect an exceedingly
small portion of applications - many of the changes regard
non-public APIs and undocumented hacks some users may have
been attempting to use.

A second, even smaller class of non-backwards-compatible
changes is also documented. This class of change regards
those features and behaviors that have been deprecated at
least since version 0.5 and have been raising warnings since
their deprecation. These changes would only affect
applications that are still using 0.4- or early 0.5-style
APIs. As the project matures, we have fewer and fewer of
these kinds of changes with 0.x level releases, which is a
product of our API having ever fewer features that are less
than ideal for the use cases they were meant to solve.

An array of existing functionalities have been superseded in
SQLAlchemy 0.7.  There's not much difference between the
terms "superseded" and "deprecated", except that the former
has a much weaker suggestion of the old feature would ever
be removed. In 0.7, features like ``synonym`` and
``comparable_property``, as well as all the ``Extension``
and other event classes, have been superseded.  But these
"superseded" features have been re-implemented such that
their implementations live mostly outside of core ORM code,
so their continued "hanging around" doesn't impact
SQLAlchemy's ability to further streamline and refine its
internals, and we expect them to remain within the API for
the foreseeable future.

New Features
============

New Event System
----------------

SQLAlchemy started early with the ``MapperExtension`` class,
which provided hooks into the persistence cycle of mappers.
As SQLAlchemy quickly became more componentized, pushing
mappers into a more focused configurational role, many more
"extension", "listener", and "proxy" classes popped up to
solve various activity-interception use cases in an ad-hoc
fashion.   Part of this was driven by the divergence of
activities; ``ConnectionProxy`` objects wanted to provide a
system of rewriting statements and parameters;
``AttributeExtension`` provided a system of replacing
incoming values, and ``DDL`` objects had events that could
be switched off of dialect-sensitive callables.

0.7 re-implements virtually all of these plugin points with
a new, unified approach, which retains all the
functionalities of the different systems, provides more
flexibility and less boilerplate, performs better, and
eliminates the need to learn radically different APIs for
each event subsystem.  The pre-existing classes
``MapperExtension``, ``SessionExtension``,
``AttributeExtension``, ``ConnectionProxy``,
``PoolListener`` as well as the ``DDLElement.execute_at``
method are deprecated and now implemented in terms of the
new system - these APIs remain fully functional and are
expected to remain in place for the foreseeable future.

The new approach uses named events and user-defined
callables to associate activities with events. The API's
look and feel was driven by such diverse sources as JQuery,
Blinker, and Hibernate, and was also modified further on
several occasions during conferences with dozens of users on
Twitter, which appears to have a much higher response rate
than the mailing list for such questions.

It also features an open-ended system of target
specification that allows events to be associated with API
classes, such as for all ``Session`` or ``Engine`` objects,
with specific instances of API classes, such as for a
specific ``Pool`` or ``Mapper``, as well as for related
objects like a user- defined class that's mapped, or
something as specific as a certain attribute on instances of
a particular subclass of a mapped parent class. Individual
listener subsystems can apply wrappers to incoming user-
defined listener functions which modify how they are called
- an mapper event can receive either the instance of the
object being operated upon, or its underlying
``InstanceState`` object. An attribute event can opt whether
or not to have the responsibility of returning a new value.

Several systems now build upon the new event API, including
the new "mutable attributes" API as well as composite
attributes. The greater emphasis on events has also led to
the introduction of a handful of new events, including
attribute expiration and refresh operations, pickle
loads/dumps operations, completed mapper construction
operations.

.. seealso::

  :ref:`event_toplevel`

:ticket:`1902`

Hybrid Attributes, implements/supersedes synonym(), comparable_property()
-------------------------------------------------------------------------

The "derived attributes" example has now been turned into an
official extension.   The typical use case for ``synonym()``
is to provide descriptor access to a mapped column; the use
case for ``comparable_property()`` is to be able to return a
``PropComparator`` from any descriptor.   In practice, the
approach of "derived" is easier to use, more extensible, is
implemented in a few dozen lines of pure Python with almost
no imports, and doesn't require the ORM core to even be
aware of it.   The feature is now known as the "Hybrid
Attributes" extension.

``synonym()`` and ``comparable_property()`` are still part
of the ORM, though their implementations have been moved
outwards, building on an approach that is similar to that of
the hybrid extension, so that the core ORM
mapper/query/property modules aren't really aware of them
otherwise.

.. seealso::

  :ref:`hybrids_toplevel`

:ticket:`1903`

Speed Enhancements
------------------

As is customary with all major SQLA releases, a wide pass
through the internals to reduce overhead and callcounts has
been made which further reduces the work needed in common
scenarios. Highlights of this release include:

* The flush process will now bundle INSERT statements into
  batches fed   to ``cursor.executemany()``, for rows where
  the primary key is already   present.   In particular this
  usually applies to the "child" table on a joined   table
  inheritance configuration, meaning the number of calls to
  ``cursor.execute``   for a large bulk insert of joined-
  table objects can be cut in half, allowing   native DBAPI
  optimizations to take place for those statements passed
  to ``cursor.executemany()`` (such as re-using a prepared
  statement).

* The codepath invoked when accessing a many-to-one
  reference to a related object   that's already loaded has
  been greatly simplified.  The identity map is checked
  directly without the need to generate a new ``Query``
  object first, which is   expensive in the context of
  thousands of in-memory many-to-ones being accessed.   The
  usage of constructed-per-call "loader" objects is also no
  longer used for   the majority of lazy attribute loads.

* The rewrite of composites allows a shorter codepath when
  mapper internals   access mapped attributes within a
  flush.

* New inlined attribute access functions replace the
  previous usage of   "history" when the "save-update" and
  other cascade operations need to   cascade among the full
  scope of datamembers associated with an attribute.   This
  reduces the overhead of generating a new ``History``
  object for this speed-critical   operation.

* The internals of the ``ExecutionContext``, the object
  corresponding to a statement   execution, have been
  inlined and simplified.

* The ``bind_processor()`` and ``result_processor()``
  callables generated by types   for each statement
  execution are now cached (carefully, so as to avoid memory
  leaks for ad-hoc types and dialects) for the lifespan of
  that type, further   reducing per-statement call overhead.

* The collection of "bind processors" for a particular
  ``Compiled`` instance of   a statement is also cached on
  the ``Compiled`` object, taking further   advantage of the
  "compiled cache" used by the flush process to re-use the
  same   compiled form of INSERT, UPDATE, DELETE statements.

A demonstration of callcount reduction including a sample
benchmark script is at
http://techspot.zzzeek.org/2010/12/12/a-tale-of-three-
profiles/

Composites Rewritten
--------------------

The "composite" feature has been rewritten, like
``synonym()`` and ``comparable_property()``, to use a
lighter weight implementation based on descriptors and
events, rather than building into the ORM internals.  This
allowed the removal of some latency from the mapper/unit of
work internals, and simplifies the workings of composite.
The composite attribute now no longer conceals the
underlying columns it builds upon, which now remain as
regular attributes.  Composites can also act as a proxy for
``relationship()`` as well as ``Column()`` attributes.

The major backwards-incompatible change of composites is
that they no longer use the ``mutable=True`` system to
detect in-place mutations.   Please use the `Mutation
Tracking <http://www.sqlalchemy.org/docs/07/orm/extensions/m
utable.html>`_ extension to establish in-place change events
to existing composite usage.

.. seealso::

  :ref:`mapper_composite`

  :ref:`mutable_toplevel`

:ticket:`2008` :ticket:`2024`

More succinct form of query.join(target, onclause)
--------------------------------------------------

The default method of issuing ``query.join()`` to a target
with an explicit onclause is now:

::

    query.join(SomeClass, SomeClass.id==ParentClass.some_id)

In 0.6, this usage was considered to be an error, because
``join()`` accepts multiple arguments corresponding to
multiple JOIN clauses - the two-argument form needed to be
in a tuple to disambiguate between single-argument and two-
argument join targets.  In the middle of 0.6 we added
detection and an error message for this specific calling
style, since it was so common.  In 0.7, since we are
detecting the exact pattern anyway, and since having to type
out a tuple for no reason is extremely annoying, the non-
tuple method now becomes the "normal" way to do it.  The
"multiple JOIN" use case is exceedingly rare compared to the
single join case, and multiple joins these days are more
clearly represented by multiple calls to ``join()``.

The tuple form will remain for backwards compatibility.

Note that all the other forms of ``query.join()`` remain
unchanged:

::

    query.join(MyClass.somerelation)
    query.join("somerelation")
    query.join(MyTarget)
    # ... etc

`Querying with Joins
<http://www.sqlalchemy.org/docs/07/orm/tutorial.html
#querying-with-joins>`_

:ticket:`1923`

.. _07_migration_mutation_extension:

Mutation event extension, supersedes "mutable=True"
---------------------------------------------------

A new extension, :ref:`mutable_toplevel`, provides a
mechanism by which user-defined datatypes can provide change
events back to the owning parent or parents.   The extension
includes an approach for scalar database values, such as
those managed by :class:`.PickleType`, ``postgresql.ARRAY``, or
other custom ``MutableType`` classes, as well as an approach
for ORM "composites", those configured using :func:`~.sqlalchemy.orm.composite`.

.. seealso::

    :ref:`mutable_toplevel`

NULLS FIRST / NULLS LAST operators
----------------------------------

These are implemented as an extension to the ``asc()`` and
``desc()`` operators, called ``nullsfirst()`` and
``nullslast()``.

.. seealso::

    :func:`.nullsfirst`

    :func:`.nullslast`

:ticket:`723`

select.distinct(), query.distinct() accepts \*args for PostgreSQL DISTINCT ON
-----------------------------------------------------------------------------

This was already available by passing a list of expressions
to the ``distinct`` keyword argument of ``select()``, the
``distinct()`` method of ``select()`` and ``Query`` now
accept positional arguments which are rendered as DISTINCT
ON when a PostgreSQL backend is used.

`distinct() <http://www.sqlalchemy.org/docs/07/core/expressi
on_api.html#sqlalchemy.sql.expression.Select.distinct>`_

`Query.distinct() <http://www.sqlalchemy.org/docs/07/orm/que
ry.html#sqlalchemy.orm.query.Query.distinct>`_

:ticket:`1069`

``Index()`` can be placed inline inside of ``Table``, ``__table_args__``
------------------------------------------------------------------------

The Index() construct can be created inline with a Table
definition, using strings as column names, as an alternative
to the creation of the index outside of the Table.  That is:

::

    Table('mytable', metadata,
            Column('id',Integer, primary_key=True),
            Column('name', String(50), nullable=False),
            Index('idx_name', 'name')
    )

The primary rationale here is for the benefit of declarative
``__table_args__``, particularly when used with mixins:

::

    class HasNameMixin(object):
        name = Column('name', String(50), nullable=False)
        @declared_attr
        def __table_args__(cls):
            return (Index('name'), {})

    class User(HasNameMixin, Base):
        __tablename__ = 'user'
        id = Column('id', Integer, primary_key=True)

`Indexes <http://www.sqlalchemy.org/docs/07/core/schema.html
#indexes>`_

Window Function SQL Construct
-----------------------------

A "window function" provides to a statement information
about the result set as it's produced. This allows criteria
against various things like "row number", "rank" and so
forth. They are known to be supported at least by
PostgreSQL, SQL Server and Oracle, possibly others.

The best introduction to window functions is on PostgreSQL's
site, where window functions have been supported since
version 8.4:

http://www.postgresql.org/docs/9.0/static/tutorial-
window.html

SQLAlchemy provides a simple construct typically invoked via
an existing function clause, using the ``over()`` method,
which accepts ``order_by`` and ``partition_by`` keyword
arguments. Below we replicate the first example in PG's
tutorial:

::

    from sqlalchemy.sql import table, column, select, func

    empsalary = table('empsalary',
                    column('depname'),
                    column('empno'),
                    column('salary'))

    s = select([
            empsalary,
            func.avg(empsalary.c.salary).
                  over(partition_by=empsalary.c.depname).
                  label('avg')
        ])

    print(s)

SQL:

::

    SELECT empsalary.depname, empsalary.empno, empsalary.salary,
    avg(empsalary.salary) OVER (PARTITION BY empsalary.depname) AS avg
    FROM empsalary

`sqlalchemy.sql.expression.over <http://www.sqlalchemy.org/d
ocs/07/core/expression_api.html#sqlalchemy.sql.expression.ov
er>`_

:ticket:`1844`

execution_options() on Connection accepts "isolation_level" argument
--------------------------------------------------------------------

This sets the transaction isolation level for a single
``Connection``, until that ``Connection`` is closed and its
underlying DBAPI resource returned to the connection pool,
upon which the isolation level is reset back to the default.
The default isolation level is set using the
``isolation_level`` argument to ``create_engine()``.

Transaction isolation support is currently only supported by
the PostgreSQL and SQLite backends.

`execution_options() <http://www.sqlalchemy.org/docs/07/core
/connections.html#sqlalchemy.engine.base.Connection.executio
n_options>`_

:ticket:`2001`

``TypeDecorator`` works with integer primary key columns
--------------------------------------------------------

A ``TypeDecorator`` which extends the behavior of
``Integer`` can be used with a primary key column.  The
"autoincrement" feature of ``Column`` will now recognize
that the underlying database column is still an integer so
that lastrowid mechanisms continue to function.   The
``TypeDecorator`` itself will have its result value
processor applied to newly generated primary keys, including
those received by the DBAPI ``cursor.lastrowid`` accessor.

:ticket:`2005` :ticket:`2006`

``TypeDecorator`` is present in the "sqlalchemy" import space
-------------------------------------------------------------

No longer need to import this from ``sqlalchemy.types``,
it's now mirrored in ``sqlalchemy``.

New Dialects
------------

Dialects have been added:

* a MySQLdb driver for the Drizzle database:


  `Drizzle <http://www.sqlalchemy.org/docs/07/dialects/drizz
  le.html>`_

* support for the pymysql DBAPI:


  `pymsql Notes
  <http://www.sqlalchemy.org/docs/07/dialects/mysql.html
  #module-sqlalchemy.dialects.mysql.pymysql>`_

* psycopg2 now works with Python 3


Behavioral Changes (Backwards Compatible)
=========================================

C Extensions Build by Default
-----------------------------

This is as of 0.7b4.   The exts will build if cPython 2.xx
is detected.   If the build fails, such as on a windows
install, that condition is caught and the non-C install
proceeds.    The C exts won't build if Python 3 or Pypy is
used.

Query.count() simplified, should work virtually always
------------------------------------------------------

The very old guesswork which occurred within
``Query.count()`` has been modernized to use
``.from_self()``.  That is, ``query.count()`` is now
equivalent to:

::

    query.from_self(func.count(literal_column('1'))).scalar()

Previously, internal logic attempted to rewrite the columns
clause of the query itself, and upon detection of a
"subquery" condition, such as a column-based query that
might have aggregates in it, or a query with DISTINCT, would
go through a convoluted process of rewriting the columns
clause.   This logic failed in complex conditions,
particularly those involving joined table inheritance, and
was long obsolete by the more comprehensive ``.from_self()``
call.

The SQL emitted by ``query.count()`` is now always of the
form:

::

    SELECT count(1) AS count_1 FROM (
        SELECT user.id AS user_id, user.name AS user_name from user
    ) AS anon_1

that is, the original query is preserved entirely inside of
a subquery, with no more guessing as to how count should be
applied.

:ticket:`2093`

To emit a non-subquery form of count()
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

MySQL users have already reported that the MyISAM engine not
surprisingly falls over completely with this simple change.
Note that for a simple ``count()`` that optimizes for DBs
that can't handle simple subqueries, ``func.count()`` should
be used:

::

    from sqlalchemy import func
    session.query(func.count(MyClass.id)).scalar()

or for ``count(*)``:

::

    from sqlalchemy import func, literal_column
    session.query(func.count(literal_column('*'))).select_from(MyClass).scalar()

LIMIT/OFFSET clauses now use bind parameters
--------------------------------------------

The LIMIT and OFFSET clauses, or their backend equivalents
(i.e. TOP, ROW NUMBER OVER, etc.), use bind parameters for
the actual values, for all backends which support it (most
except for Sybase).  This allows better query optimizer
performance as the textual string for multiple statements
with differing LIMIT/OFFSET are now identical.

:ticket:`805`

Logging enhancements
--------------------

Vinay Sajip has provided a patch to our logging system such
that the "hex string" embedded in logging statements for
engines and pools is no longer needed to allow the ``echo``
flag to work correctly.  A new system that uses filtered
logging objects allows us to maintain our current behavior
of ``echo`` being local to individual engines without the
need for additional identifying strings local to those
engines.

:ticket:`1926`

Simplified polymorphic_on assignment
------------------------------------

The population of the ``polymorphic_on`` column-mapped
attribute, when used in an inheritance scenario, now occurs
when the object is constructed, i.e. its ``__init__`` method
is called, using the init event.  The attribute then behaves
the same as any other column-mapped attribute.   Previously,
special logic would fire off during flush to populate this
column, which prevented any user code from modifying its
behavior.   The new approach improves upon this in three
ways: 1. the polymorphic identity is now present on the
object as soon as its constructed; 2. the polymorphic
identity can be changed by user code without any difference
in behavior from any other column-mapped attribute; 3. the
internals of the mapper during flush are simplified and no
longer need to make special checks for this column.

:ticket:`1895`

contains_eager() chains across multiple paths (i.e. "all()")
------------------------------------------------------------

The ```contains_eager()```` modifier now will chain itself
for a longer path without the need to emit individual
````contains_eager()``` calls. Instead of:

::

    session.query(A).options(contains_eager(A.b), contains_eager(A.b, B.c))

you can say:

::

    session.query(A).options(contains_eager(A.b, B.c))

:ticket:`2032`

Flushing of orphans that have no parent is allowed
--------------------------------------------------

We've had a long standing behavior that checks for a so-
called "orphan" during flush, that is, an object which is
associated with a ``relationship()`` that specifies "delete-
orphan" cascade, has been newly added to the session for an
INSERT, and no parent relationship has been established.
This check was added years ago to accommodate some test
cases which tested the orphan behavior for consistency.   In
modern SQLA, this check is no longer needed on the Python
side.   The equivalent behavior of the "orphan check" is
accomplished by making the foreign key reference to the
object's parent row NOT NULL, where the database does its
job of establishing data consistency in the same way SQLA
allows most other operations to do.   If the object's parent
foreign key is nullable, then the row can be inserted.   The
"orphan" behavior runs when the object was persisted with a
particular parent, and is then disassociated with that
parent, leading to a DELETE statement emitted for it.

:ticket:`1912`

Warnings generated when collection members, scalar referents not part of the flush
----------------------------------------------------------------------------------

Warnings are now emitted when related objects referenced via
a loaded ``relationship()`` on a parent object marked as
"dirty" are not present in the current ``Session``.

The ``save-update`` cascade takes effect when objects are
added to the ``Session``, or when objects are first
associated with a parent, so that an object and everything
related to it are usually all present in the same
``Session``.  However, if ``save-update`` cascade is
disabled for a particular ``relationship()``, then this
behavior does not occur, and the flush process does not try
to correct for it, instead staying consistent to the
configured cascade behavior.   Previously, when such objects
were detected during the flush, they were silently skipped.
The new behavior is that a warning is emitted, for the
purposes of alerting to a situation that more often than not
is the source of unexpected behavior.

:ticket:`1973`

Setup no longer installs a Nose plugin
--------------------------------------

Since we moved to nose we've used a plugin that installs via
setuptools, so that the ``nosetests`` script would
automatically run SQLA's plugin code, necessary for our
tests to have a full environment.  In the middle of 0.6, we
realized that the import pattern here meant that Nose's
"coverage" plugin would break, since "coverage" requires
that it be started before any modules to be covered are
imported; so in the middle of 0.6 we made the situation
worse by adding a separate ``sqlalchemy-nose`` package to
the build to overcome this.

In 0.7 we've done away with trying to get ``nosetests`` to
work automatically, since the SQLAlchemy module would
produce a large number of nose configuration options for all
usages of ``nosetests``, not just the SQLAlchemy unit tests
themselves, and the additional ``sqlalchemy-nose`` install
was an even worse idea, producing an extra package in Python
environments.   The ``sqla_nose.py`` script in 0.7 is now
the only way to run the tests with nose.

:ticket:`1949`

Non-``Table``-derived constructs can be mapped
----------------------------------------------

A construct that isn't against any ``Table`` at all, like a
function, can be mapped.

::

    from sqlalchemy import select, func
    from sqlalchemy.orm import mapper

    class Subset(object):
        pass
    selectable = select(["x", "y", "z"]).select_from(func.some_db_function()).alias()
    mapper(Subset, selectable, primary_key=[selectable.c.x])

:ticket:`1876`

aliased() accepts ``FromClause`` elements
-----------------------------------------

This is a convenience helper such that in the case a plain
``FromClause``, such as a ``select``, ``Table`` or ``join``
is passed to the ``orm.aliased()`` construct, it passes
through to the ``.alias()`` method of that from construct
rather than constructing an ORM level ``AliasedClass``.

:ticket:`2018`

Session.connection(), Session.execute() accept 'bind'
-----------------------------------------------------

This is to allow execute/connection operations to
participate in the open transaction of an engine explicitly.
It also allows custom subclasses of ``Session`` that
implement their own ``get_bind()`` method and arguments to
use those custom arguments with both the ``execute()`` and
``connection()`` methods equally.

`Session.connection <http://www.sqlalchemy.org/docs/07/orm/s
ession.html#sqlalchemy.orm.session.Session.connection>`_
`Session.execute <http://www.sqlalchemy.org/docs/07/orm/sess
ion.html#sqlalchemy.orm.session.Session.execute>`_

:ticket:`1996`

Standalone bind parameters in columns clause auto-labeled.
----------------------------------------------------------

Bind parameters present in the "columns clause" of a select
are now auto-labeled like other "anonymous" clauses, which
among other things allows their "type" to be meaningful when
the row is fetched, as in result row processors.

SQLite - relative file paths are normalized through os.path.abspath()
---------------------------------------------------------------------

This so that a script that changes the current directory
will continue to target the same location as subsequent
SQLite connections are established.

:ticket:`2036`

MS-SQL - ``String``/``Unicode``/``VARCHAR``/``NVARCHAR``/``VARBINARY`` emit "max" for no length
-----------------------------------------------------------------------------------------------

On the MS-SQL backend, the String/Unicode types, and their
counterparts VARCHAR/ NVARCHAR, as well as VARBINARY
(:ticket:`1833`) emit "max" as the length when no length is
specified. This makes it more compatible with PostgreSQL's
VARCHAR type which is similarly unbounded when no length
specified.   SQL Server defaults the length on these types
to '1' when no length is specified.

Behavioral Changes (Backwards Incompatible)
===========================================

Note again, aside from the default mutability change, most
of these changes are \*extremely minor* and will not affect
most users.

``PickleType`` and ARRAY mutability turned off by default
---------------------------------------------------------

This change refers to the default behavior of the ORM when
mapping columns that have either the ``PickleType`` or
``postgresql.ARRAY`` datatypes.  The ``mutable`` flag is now
set to ``False`` by default. If an existing application uses
these types and depends upon detection of in-place
mutations, the type object must be constructed with
``mutable=True`` to restore the 0.6 behavior:

::

    Table('mytable', metadata,
        # ....

        Column('pickled_data', PickleType(mutable=True))
    )

The ``mutable=True`` flag is being phased out, in favor of
the new `Mutation Tracking <http://www.sqlalchemy.org/docs/0
7/orm/extensions/mutable.html>`_ extension.  This extension
provides a mechanism by which user-defined datatypes can
provide change events back to the owning parent or parents.

The previous approach of using ``mutable=True`` does not
provide for change events - instead, the ORM must scan
through all mutable values present in a session and compare
them against their original value for changes every time
``flush()`` is called, which is a very time consuming event.
This is a holdover from the very early days of SQLAlchemy
when ``flush()`` was not automatic and the history tracking
system was not nearly as sophisticated as it is now.

Existing applications which use ``PickleType``,
``postgresql.ARRAY`` or other ``MutableType`` subclasses,
and require in-place mutation detection, should migrate to
the new mutation tracking system, as ``mutable=True`` is
likely to be deprecated in the future.

:ticket:`1980`

Mutability detection of ``composite()`` requires the Mutation Tracking Extension
--------------------------------------------------------------------------------

So-called "composite" mapped attributes, those configured
using the technique described at `Composite Column Types
<http://www.sqlalchemy.org/docs/07/orm/mapper_config.html
#composite-column-types>`_, have been re-implemented such
that the ORM internals are no longer aware of them (leading
to shorter and more efficient codepaths in critical
sections).   While composite types are generally intended to
be treated as immutable value objects, this was never
enforced.   For applications that use composites with
mutability, the `Mutation Tracking <http://www.sqlalchemy.or
g/docs/07/orm/extensions/mutable.html>`_ extension offers a
base class which establishes a mechanism for user-defined
composite types to send change event messages back to the
owning parent or parents of each object.

Applications which use composite types and rely upon in-
place mutation detection of these objects should either
migrate to the "mutation tracking" extension, or change the
usage of the composite types such that in-place changes are
no longer needed (i.e., treat them as immutable value
objects).

SQLite - the SQLite dialect now uses ``NullPool`` for file-based databases
--------------------------------------------------------------------------

This change is **99.999% backwards compatible**, unless you
are using temporary tables across connection pool
connections.

A file-based SQLite connection is blazingly fast, and using
``NullPool`` means that each call to ``Engine.connect``
creates a new pysqlite connection.

Previously, the ``SingletonThreadPool`` was used, which
meant that all connections to a certain engine in a thread
would be the same connection.   It's intended that the new
approach is more intuitive, particularly when multiple
connections are used.

``SingletonThreadPool`` is still the default engine when a
``:memory:`` database is used.

Note that this change **breaks temporary tables used across
Session commits**, due to the way SQLite handles temp
tables. See the note at
http://www.sqlalchemy.org/docs/dialects/sqlite.html#using-
temporary-tables-with-sqlite if temporary tables beyond the
scope of one pool connection are desired.

:ticket:`1921`

``Session.merge()`` checks version ids for versioned mappers
------------------------------------------------------------

Session.merge() will check the version id of the incoming
state against that of the database, assuming the mapping
uses version ids and incoming state has a version_id
assigned, and raise StaleDataError if they don't match.
This is the correct behavior, in that if incoming state
contains a stale version id, it should be assumed the state
is stale.

If merging data into a versioned state, the version id
attribute can be left undefined, and no version check will
take place.

This check was confirmed by examining what Hibernate does -
both the ``merge()`` and the versioning features were
originally adapted from Hibernate.

:ticket:`2027`

Tuple label names in Query Improved
-----------------------------------

This improvement is potentially slightly backwards
incompatible for an application that relied upon the old
behavior.

Given two mapped classes ``Foo`` and ``Bar`` each with a
column ``spam``:

::


    qa = session.query(Foo.spam)
    qb = session.query(Bar.spam)

    qu = qa.union(qb)

The name given to the single column yielded by ``qu`` will
be ``spam``.  Previously it would be something like
``foo_spam`` due to the way the ``union`` would combine
things, which is inconsistent with the name ``spam`` in the
case of a non-unioned query.

:ticket:`1942`

Mapped column attributes reference the most specific column first
-----------------------------------------------------------------

This is a change to the behavior involved when a mapped
column attribute references multiple columns, specifically
when dealing with an attribute on a joined-table subclass
that has the same name as that of an attribute on the
superclass.

Using declarative, the scenario is this:

::

    class Parent(Base):
        __tablename__ = 'parent'
        id = Column(Integer, primary_key=True)

    class Child(Parent):
       __tablename__ = 'child'
        id = Column(Integer, ForeignKey('parent.id'), primary_key=True)

Above, the attribute ``Child.id`` refers to both the
``child.id`` column as well as ``parent.id`` - this due to
the name of the attribute.  If it were named differently on
the class, such as ``Child.child_id``, it then maps
distinctly to ``child.id``, with ``Child.id`` being the same
attribute as ``Parent.id``.

When the ``id`` attribute is made to reference both
``parent.id`` and ``child.id``, it stores them in an ordered
list.   An expression such as ``Child.id`` then refers to
just *one* of those columns when rendered. Up until 0.6,
this column would be ``parent.id``.  In 0.7, it is the less
surprising ``child.id``.

The legacy of this behavior deals with behaviors and
restrictions of the ORM that don't really apply anymore; all
that was needed was to reverse the order.

A primary advantage of this approach is that it's now easier
to construct ``primaryjoin`` expressions that refer to the
local column:

::

    class Child(Parent):
       __tablename__ = 'child'
        id = Column(Integer, ForeignKey('parent.id'), primary_key=True)
        some_related = relationship("SomeRelated",
                        primaryjoin="Child.id==SomeRelated.child_id")

    class SomeRelated(Base):
       __tablename__ = 'some_related'
        id = Column(Integer, primary_key=True)
        child_id = Column(Integer, ForeignKey('child.id'))

Prior to 0.7 the ``Child.id`` expression would reference
``Parent.id``, and it would be necessary to map ``child.id``
to a distinct attribute.

It also means that a query like this one changes its
behavior:

::

    session.query(Parent).filter(Child.id > 7)

In 0.6, this would render:

::

    SELECT parent.id AS parent_id
    FROM parent
    WHERE parent.id > :id_1

in 0.7, you get:

::

    SELECT parent.id AS parent_id
    FROM parent, child
    WHERE child.id > :id_1

which you'll note is a cartesian product - this behavior is
now equivalent to that of any other attribute that is local
to ``Child``.   The ``with_polymorphic()`` method, or a
similar strategy of explicitly joining the underlying
``Table`` objects, is used to render a query against all
``Parent`` objects with criteria against ``Child``, in the
same manner as that of 0.5 and 0.6:

::

    print(s.query(Parent).with_polymorphic([Child]).filter(Child.id > 7))

Which on both 0.6 and 0.7 renders:

::

    SELECT parent.id AS parent_id, child.id AS child_id
    FROM parent LEFT OUTER JOIN child ON parent.id = child.id
    WHERE child.id > :id_1

Another effect of this change is that a joined-inheritance
load across two tables will populate from the child table's
value, not that of the parent table. An unusual case is that
a query against "Parent" using ``with_polymorphic="*"``
issues a query against "parent", with a LEFT OUTER JOIN to
"child".  The row is located in "Parent", sees the
polymorphic identity corresponds to "Child", but suppose the
actual row in "child" has been *deleted*.  Due to this
corruption, the row comes in with all the columns
corresponding to "child" set to NULL - this is now the value
that gets populated, not the one in the parent table.

:ticket:`1892`

Mapping to joins with two or more same-named columns requires explicit declaration
----------------------------------------------------------------------------------

This is somewhat related to the previous change in
:ticket:`1892`.   When mapping to a join, same-named columns
must be explicitly linked to mapped attributes, i.e. as
described in `Mapping a Class Against Multiple Tables <http:
//www.sqlalchemy.org/docs/07/orm/mapper_config.html#mapping-
a-class-against-multiple-tables>`_.

Given two tables ``foo`` and ``bar``, each with a primary
key column ``id``, the following now produces an error:

::


    foobar = foo.join(bar, foo.c.id==bar.c.foo_id)
    mapper(FooBar, foobar)

This because the ``mapper()`` refuses to guess what column
is the primary representation of ``FooBar.id`` - is it
``foo.c.id`` or is it ``bar.c.id`` ?   The attribute must be
explicit:

::


    foobar = foo.join(bar, foo.c.id==bar.c.foo_id)
    mapper(FooBar, foobar, properties={
        'id':[foo.c.id, bar.c.id]
    })

:ticket:`1896`

Mapper requires that polymorphic_on column be present in the mapped selectable
------------------------------------------------------------------------------

This is a warning in 0.6, now an error in 0.7.   The column
given for ``polymorphic_on`` must be in the mapped
selectable.  This to prevent some occasional user errors
such as:

::

    mapper(SomeClass, sometable, polymorphic_on=some_lookup_table.c.id)

where above the polymorphic_on needs to be on a
``sometable`` column, in this case perhaps
``sometable.c.some_lookup_id``.   There are also some
"polymorphic union" scenarios where similar mistakes
sometimes occur.

Such a configuration error has always been "wrong", and the
above mapping doesn't work as specified - the column would
be ignored.  It is however potentially backwards
incompatible in the rare case that an application has been
unknowingly relying upon this behavior.

:ticket:`1875`

``DDL()`` constructs now escape percent signs
---------------------------------------------

Previously, percent signs in ``DDL()`` strings would have to
be escaped, i.e. ``%%`` depending on DBAPI, for those DBAPIs
that accept ``pyformat`` or ``format`` binds (i.e. psycopg2,
mysql-python), which was inconsistent versus ``text()``
constructs which did this automatically.  The same escaping
now occurs for ``DDL()`` as for ``text()``.

:ticket:`1897`

``Table.c`` / ``MetaData.tables`` refined a bit, don't allow direct mutation
----------------------------------------------------------------------------

Another area where some users were tinkering around in such
a way that doesn't actually work as expected, but still left
an exceedingly small chance that some application was
relying upon this behavior, the construct returned by the
``.c`` attribute on ``Table`` and the ``.tables`` attribute
on ``MetaData`` is explicitly non-mutable.    The "mutable"
version of the construct is now private.   Adding columns to
``.c`` involves using the ``append_column()`` method of
``Table``, which ensures things are associated with the
parent ``Table`` in the appropriate way; similarly,
``MetaData.tables`` has a contract with the ``Table``
objects stored in this dictionary, as well as a little bit
of new bookkeeping in that a ``set()`` of all schema names
is tracked, which is satisfied only by using the public
``Table`` constructor as well as ``Table.tometadata()``.

It is of course possible that the ``ColumnCollection`` and
``dict`` collections consulted by these attributes could
someday implement events on all of their mutational methods
such that the appropriate bookkeeping occurred upon direct
mutation of the collections, but until someone has the
motivation to implement all that along with dozens of new
unit tests, narrowing the paths to mutation of these
collections will ensure no application is attempting to rely
upon usages that are currently not supported.

:ticket:`1893` :ticket:`1917`

server_default consistently returns None for all inserted_primary_key values
----------------------------------------------------------------------------

Established consistency when server_default is present on an
Integer PK column. SQLA doesn't pre-fetch these, nor do they
come back in cursor.lastrowid (DBAPI). Ensured all backends
consistently return None in result.inserted_primary_key for
these - some backends may have returned a value previously.
Using a server_default on a primary key column is extremely
unusual.   If a special function or SQL expression is used
to generate primary key defaults, this should be established
as a Python-side "default" instead of server_default.

Regarding reflection for this case, reflection of an int PK
col with a server_default sets the "autoincrement" flag to
False, except in the case of a PG SERIAL col where we
detected a sequence default.

:ticket:`2020` :ticket:`2021`

The ``sqlalchemy.exceptions`` alias in sys.modules is removed
-------------------------------------------------------------

For a few years we've added the string
``sqlalchemy.exceptions`` to ``sys.modules``, so that a
statement like "``import sqlalchemy.exceptions``" would
work.   The name of the core exceptions module has been
``exc`` for a long time now, so the recommended import for
this module is:

::

    from sqlalchemy import exc

The ``exceptions`` name is still present in "``sqlalchemy``"
for applications which might have said ``from sqlalchemy
import exceptions``, but they should also start using the
``exc`` name.

Query Timing Recipe Changes
---------------------------

While not part of SQLAlchemy itself, it's worth mentioning
that the rework of the ``ConnectionProxy`` into the new
event system means it is no longer appropriate for the
"Timing all Queries" recipe.  Please adjust query-timers to
use the ``before_cursor_execute()`` and
``after_cursor_execute()`` events, demonstrated in the
updated recipe UsageRecipes/Profiling.

Deprecated API
==============

Default constructor on types will not accept arguments
------------------------------------------------------

Simple types like ``Integer``, ``Date`` etc. in the core
types module don't accept arguments.  The default
constructor that accepts/ignores a catchall ``\*args,
\**kwargs`` is restored as of 0.7b4/0.7.0, but emits a
deprecation warning.

If arguments are being used with a core type like
``Integer``, it may be that you intended to use a dialect
specific type, such as ``sqlalchemy.dialects.mysql.INTEGER``
which does accept a "display_width" argument for example.

compile_mappers() renamed configure_mappers(), simplified configuration internals
---------------------------------------------------------------------------------

This system slowly morphed from something small, implemented
local to an individual mapper, and poorly named into
something that's more of a global "registry-" level function
and poorly named, so we've fixed both by moving the
implementation out of ``Mapper`` altogether and renaming it
to ``configure_mappers()``.   It is of course normally not
needed for an application to call ``configure_mappers()`` as
this process occurs on an as-needed basis, as soon as the
mappings are needed via attribute or query access.

:ticket:`1966`

Core listener/proxy superseded by event listeners
-------------------------------------------------

``PoolListener``, ``ConnectionProxy``,
``DDLElement.execute_at`` are superseded by
``event.listen()``, using the ``PoolEvents``,
``EngineEvents``, ``DDLEvents`` dispatch targets,
respectively.

ORM extensions superseded by event listeners
--------------------------------------------

``MapperExtension``, ``AttributeExtension``,
``SessionExtension`` are superseded by ``event.listen()``,
using the ``MapperEvents``/``InstanceEvents``,
``AttributeEvents``, ``SessionEvents``, dispatch targets,
respectively.

Sending a string to 'distinct' in select() for MySQL should be done via prefixes
--------------------------------------------------------------------------------

This obscure feature allows this pattern with the MySQL
backend:

::

    select([mytable], distinct='ALL', prefixes=['HIGH_PRIORITY'])

The ``prefixes`` keyword or ``prefix_with()`` method should
be used for non-standard or unusual prefixes:

::

    select([mytable]).prefix_with('HIGH_PRIORITY', 'ALL')

``useexisting`` superseded by ``extend_existing`` and ``keep_existing``
-----------------------------------------------------------------------

The ``useexisting`` flag on Table has been superseded by a
new pair of flags ``keep_existing`` and ``extend_existing``.
``extend_existing`` is equivalent to ``useexisting`` - the
existing Table is returned, and additional constructor
elements are added. With ``keep_existing``, the existing
Table is returned, but additional constructor elements are
not added - these elements are only applied when the Table
is newly created.

Backwards Incompatible API Changes
==================================

Callables passed to ``bindparam()`` don't get evaluated - affects the Beaker example
------------------------------------------------------------------------------------

:ticket:`1950`

Note this affects the Beaker caching example, where the
workings of the ``_params_from_query()`` function needed a
slight adjustment. If you're using code from the Beaker
example, this change should be applied.

types.type_map is now private, types._type_map
----------------------------------------------

We noticed some users tapping into this dictionary inside of
``sqlalchemy.types`` as a shortcut to associating Python
types with SQL types. We can't guarantee the contents or
format of this dictionary, and additionally the business of
associating Python types in a one-to-one fashion has some
grey areas that should are best decided by individual
applications, so we've underscored this attribute.

:ticket:`1870`

Renamed the ``alias`` keyword arg of standalone ``alias()`` function to ``name``
--------------------------------------------------------------------------------

This so that the keyword argument ``name`` matches that of
the ``alias()`` methods on all ``FromClause`` objects as
well as the ``name`` argument on ``Query.subquery()``.

Only code that uses the standalone ``alias()`` function, and
not the method bound functions, and passes the alias name
using the explicit keyword name ``alias``, and not
positionally, would need modification here.

Non-public ``Pool`` methods underscored
---------------------------------------

All methods of ``Pool`` and subclasses which are not
intended for public use have been renamed with underscores.
That they were not named this way previously was a bug.

Pooling methods now underscored or removed:

``Pool.create_connection()`` ->
``Pool._create_connection()``

``Pool.do_get()`` -> ``Pool._do_get()``

``Pool.do_return_conn()`` -> ``Pool._do_return_conn()``

``Pool.do_return_invalid()`` -> removed, was not used

``Pool.return_conn()`` -> ``Pool._return_conn()``

``Pool.get()`` -> ``Pool._get()``, public API is
``Pool.connect()``

``SingletonThreadPool.cleanup()`` -> ``_cleanup()``

``SingletonThreadPool.dispose_local()`` -> removed, use
``conn.invalidate()``

:ticket:`1982`

Previously Deprecated, Now Removed
==================================

Query.join(), Query.outerjoin(), eagerload(), eagerload_all(), others no longer allow lists of attributes as arguments
----------------------------------------------------------------------------------------------------------------------

Passing a list of attributes or attribute names to
``Query.join``, ``eagerload()``, and similar has been
deprecated since 0.5:

::

    # old way, deprecated since 0.5
    session.query(Houses).join([Houses.rooms, Room.closets])
    session.query(Houses).options(eagerload_all([Houses.rooms, Room.closets]))

These methods all accept \*args as of the 0.5 series:

::

    # current way, in place since 0.5
    session.query(Houses).join(Houses.rooms, Room.closets)
    session.query(Houses).options(eagerload_all(Houses.rooms, Room.closets))

``ScopedSession.mapper`` is removed
-----------------------------------

This feature provided a mapper extension which linked class-
based functionality with a particular ``ScopedSession``, in
particular providing the behavior such that new object
instances would be automatically associated with that
session.   The feature was overused by tutorials and
frameworks which led to great user confusion due to its
implicit behavior, and was deprecated in 0.5.5.   Techniques
for replicating its functionality are at
[wiki:UsageRecipes/SessionAwareMapper]

