==============================
What's New in SQLAlchemy 1.1?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 1.0,
    at the moment the current release series of SQLAlchemy,
    and SQLAlchemy version 1.1, which is the current development
    series of SQLAlchemy.

    As the 1.1 series is under development, issues that are targeted
    at this series can be seen under the
    `1.1 milestone <https://bitbucket.org/zzzeek/sqlalchemy/issues?milestone=1.1>`_.
    Please note that the set of issues within the milestone is not fixed;
    some issues may be moved to later milestones in order to allow
    for a timely release.

    Document last updated: December 4, 2015

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.1,
and also documents changes which affect users migrating
their applications from the 1.0 series of SQLAlchemy to 1.1.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.

Platform / Installer Changes
============================

Setuptools is now required for install
--------------------------------------

SQLAlchemy's ``setup.py`` file has for many years supported operation
both with Setuptools installed and without; supporting a "fallback" mode
that uses straight Distutils.  As a Setuptools-less Python environment is
now unheard of, and in order to support the featureset of Setuptools
more fully, in particular to support py.test's integration with it,
``setup.py`` now depends on Setuptools fully.

.. seealso::

	:ref:`installation`

:ticket:`3489`

Enabling / Disabling C Extension builds is only via environment variable
------------------------------------------------------------------------

The C Extensions build by default during install as long as it is possible.
To disable C extension builds, the ``DISABLE_SQLALCHEMY_CEXT`` environment
variable was made available as of SQLAlchemy 0.8.6 / 0.9.4.  The previous
approach of using the ``--without-cextensions`` argument has been removed,
as it relies on deprecated features of setuptools.

.. seealso::

	:ref:`c_extensions`

:ticket:`3500`


New Features and Improvements - ORM
===================================

.. _change_2677:

New Session lifecycle events
----------------------------

The :class:`.Session` has long supported events that allow some degree
of tracking of state changes to objects, including
:meth:`.SessionEvents.before_attach`, :meth:`.SessionEvents.after_attach`,
and :meth:`.SessionEvents.before_flush`.  The Session documentation also
documents major object states at :ref:`session_object_states`.  However,
there has never been system of tracking objects specifically as they
pass through these transitions.  Additionally, the status of "deleted" objects
has historically been murky as the objects act somewhere between
the "persistent" and "detached" states.

To clean up this area and allow the realm of session state transition
to be fully transparent, a new series of events have been added that
are intended to cover every possible way that an object might transition
between states, and additionally the "deleted" status has been given
its own official state name within the realm of session object states.

New State Transition Events
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transitions between all states of an object such as :term:`persistent`,
:term:`pending` and others can now be intercepted in terms of a
session-level event intended to cover a specific transition.
Transitions as objects move into a :class:`.Session`, move out of a
:class:`.Session`, and even all the transitions which occur when the
transaction is rolled back using :meth:`.Session.rollback`
are explicitly present in the interface of :class:`.SessionEvents`.

In total, there are **ten new events**.  A summary of these events is in a
newly written documentation section :ref:`session_lifecycle_events`.


New Object State "deleted" is added, deleted objects no longer "persistent"
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The :term:`persistent` state of an object in the :class:`.Session` has
always been documented as an object that has a valid database identity;
however in the case of objects that were deleted within a flush, they
have always been in a grey area where they are not really "detached"
from the :class:`.Session` yet, because they can still be restored
within a rollback, but are not really "persistent" because their database
identity has been deleted and they aren't present in the identity map.

To resolve this grey area given the new events, a new object state
:term:`deleted` is introduced.  This state exists between the "persistent" and
"detached" states.  An object that is marked for deletion via
:meth:`.Session.delete` remains in the "persistent" state until a flush
proceeds; at that point, it is removed from the identity map, moves
to the "deleted" state, and the :meth:`.SessionEvents.persistent_to_deleted`
hook is invoked.  If the :class:`.Session` object's transaction is rolled
back, the object is restored as persistent; the
:meth:`.SessionEvents.deleted_to_persistent` transition is called.  Otherwise
if the :class:`.Session` object's transaction is committed,
the :meth:`.SessionEvents.deleted_to_detached` transition is invoked.

Additionally, the :attr:`.InstanceState.persistent` accessor **no longer returns
True** for an object that is in the new "deleted" state; instead, the
:attr:`.InstanceState.deleted` accessor has been enhanced to reliably
report on this new state.   When the object is detached, the :attr:`.InstanceState.deleted`
returns False and the :attr:`.InstanceState.detached` accessor is True
instead.  To determine if an object was deleted either in the current
transaction or in a previous transaction, use the
:attr:`.InstanceState.was_deleted` accessor.

Strong Identity Map is Deprecated
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One of the inspirations for the new series of transition events was to enable
leak-proof tracking of objects as they move in and out of the identity map,
so that a "strong reference" may be maintained mirroring the object
moving in and out of this map.  With this new capability, there is no longer
any need for the :paramref:`.Session.weak_identity_map` parameter and the
corresponding :class:`.StrongIdentityMap` object.  This option has remained
in SQLAlchemy for many years as the "strong-referencing" behavior used to be
the only behavior available, and many applications were written to assume
this behavior.   It has long been recommended that strong-reference tracking
of objects not be an intrinsic job of the :class:`.Session` and instead
be an application-level construct built as needed by the application; the
new event model allows even the exact behavior of the strong identity map
to be replicated.   See :ref:`session_referencing_behavior` for a new
recipe illustrating how to replace the strong identity map.

:ticket:`2677`

.. _change_3499:

Changes regarding "unhashable" types
------------------------------------

The :class:`.Query` object has a well-known behavior of "deduping"
returned rows that contain at least one ORM-mapped entity (e.g., a
full mapped object, as opposed to individual column values). The
primary purpose of this is so that the handling of entities works
smoothly in conjunction with the identity map, including to
accommodate for the duplicate entities normally represented within
joined eager loading, as well as when joins are used for the purposes
of filtering on additional columns.

This deduplication relies upon the hashability of the elements within
the row.  With the introduction of Postgresql's special types like
:class:`.postgresql.ARRAY`, :class:`.postgresql.HSTORE` and
:class:`.postgresql.JSON`, the experience of types within rows being
unhashable and encountering problems here is more prevalent than
it was previously.

In fact, SQLAlchemy has since version 0.8 included a flag on datatypes that
are noted as "unhashable", however this flag was not used consistently
on built in types.  As described in :ref:`change_3499_postgresql`, this
flag is now set consistently for all of Postgresql's "structural" types.

The "unhashable" flag is also set on the :class:`.NullType` type,
as :class:`.NullType` is used to refer to any expression of unknown
type.

Additionally, the treatment of a so-called "unhashable" type is slightly
different than its been in previous releases; internally we are using
the ``id()`` function to get a "hash value" from these structures, just
as we would any ordinary mapped object.   This replaces the previous
approach which applied a counter to the object.

:ticket:`3499`

.. _change_3321:

Specific checks added for passing mapped classes, instances as SQL literals
---------------------------------------------------------------------------

The typing system now has specific checks for passing of SQLAlchemy
"inspectable" objects in contexts where they would otherwise be handled as
literal values.   Any SQLAlchemy built-in object that is legal to pass as a
SQL value includes a method ``__clause_element__()`` which provides a
valid SQL expression for that object.  For SQLAlchemy objects that
don't provide this, such as mapped classes, mappers, and mapped
instances, a more informative error message is emitted rather than
allowing the DBAPI to receive the object and fail later.  An example
is illustrated below, where a string-based attribute ``User.name`` is
compared to a full instance of ``User()``, rather than against a
string value::

    >>> some_user = User()
    >>> q = s.query(User).filter(User.name == some_user)
    ...
    sqlalchemy.exc.ArgumentError: Object <__main__.User object at 0x103167e90> is not legal as a SQL literal value

The exception is now immediate when the comparison is made between
``User.name == some_user``.  Previously, a comparison like the above
would produce a SQL expression that would only fail once resolved
into a DBAPI execution call; the mapped ``User`` object would
ultimately become a bound parameter that would be rejected by the
DBAPI.

Note that in the above example, the expression fails because
``User.name`` is a string-based (e.g. column oriented) attribute.
The change does *not* impact the usual case of comparing a many-to-one
relationship attribute to an object, which is handled distinctly::

    >>> # Address.user refers to the User mapper, so
    >>> # this is of course still OK!
    >>> q = s.query(Address).filter(Address.user == some_user)


:ticket:`3321`

.. _change_3250:

New options allowing explicit persistence of NULL over a default
----------------------------------------------------------------

Related to the new JSON-NULL support added to Postgresql as part of
:ref:`change_3514`, the base :class:`.TypeEngine` class now supports
a method :meth:`.TypeEngine.evaluates_none` which allows a positive set
of the ``None`` value on an attribute to be persisted as NULL, rather than
omitting the column from the INSERT statement, which has the effect of using
the column-level default.  This allows a mapper-level
configuration of the existing object-level technique of assigning
:func:`.sql.null` to the attribute.

.. seealso::

    :ref:`session_forcing_null`

:ticket:`3250`


.. _change_3582:

Further Fixes to single-table inheritance querying
--------------------------------------------------

Continuing from 1.0's :ref:`migration_3177`, the :class:`.Query` should
no longer inappropriately add the "single inheritance" criteria when the
query is against a subquery expression such as an exists::

    class Widget(Base):
        __tablename__ = 'widget'
        id = Column(Integer, primary_key=True)
        type = Column(String)
        data = Column(String)
        __mapper_args__ = {'polymorphic_on': type}


    class FooWidget(Widget):
        __mapper_args__ = {'polymorphic_identity': 'foo'}

    q = session.query(FooWidget).filter(FooWidget.data == 'bar').exists()

    session.query(q).all()

Produces::

    SELECT EXISTS (SELECT 1
    FROM widget
    WHERE widget.data = :data_1 AND widget.type IN (:type_1)) AS anon_1

The IN clause on the inside is appropriate, in order to limit to FooWidget
objects, however previously the IN clause would also be generated a second
time on the outside of the subquery.

:ticket:`3582`


.. _change_3601:

Session.merge resolves pending conflicts the same as persistent
---------------------------------------------------------------

The :meth:`.Session.merge` method will now track the identities of objects given
within a graph to maintain primary key uniqueness before emitting an INSERT.
When duplicate objects of the same identity are encountered, non-primary-key
attributes are **overwritten** as the objects are encountered, which is
essentially non-deterministic.   This behavior matches that of how persistent
objects, that is objects that are already located in the database via
primary key, are already treated, so this behavior is more internally
consistent.

Given::

    u1 = User(id=7, name='x')
    u1.orders = [
        Order(description='o1', address=Address(id=1, email_address='a')),
        Order(description='o2', address=Address(id=1, email_address='b')),
        Order(description='o3', address=Address(id=1, email_address='c'))
    ]

    sess = Session()
    sess.merge(u1)

Above, we merge a ``User`` object with three new ``Order`` objects, each referring to
a distinct ``Address`` object, however each is given the same primary key.
The current behavior of :meth:`.Session.merge` is to look in the identity
map for this ``Address`` object, and use that as the target.   If the object
is present, meaning that the database already has a row for ``Address`` with
primary key "1", we can see that the ``email_address`` field of the ``Address``
will be overwritten three times, in this case with the values a, b and finally
c.

However, if the ``Address`` row for primary key "1" were not present, :meth:`.Session.merge`
would instead create three separate ``Address`` instances, and we'd then get
a primary key conflict upon INSERT.  The new behavior is that the proposed
primary key for these ``Address`` objects are tracked in a separate dictionary
so that we merge the state of the three proposed ``Address`` objects onto
one ``Address`` object to be inserted.

It may have been preferable if the original case emitted some kind of warning
that conflicting data were present in a single merge-tree, however the
non-deterministic merging of values has been the behavior for many
years for the persistent case; it now matches for the pending case.   A
feature that warns for conflicting values could still be feasible for both
cases but would add considerable performance overhead as each column value
would have to be compared during the merge.


:ticket:`3601`

New Features and Improvements - Core
====================================

.. _change_3216:

The ``.autoincrement`` directive is no longer implicitly enabled for a composite primary key column
---------------------------------------------------------------------------------------------------

SQLAlchemy has always had the convenience feature of enabling the backend database's
"autoincrement" feature for a single-column integer primary key; by "autoincrement"
we mean that the database column will include whatever DDL directives the
database provides in order to indicate an auto-incrementing integer identifier,
such as the SERIAL keyword on Postgresql or AUTO_INCREMENT on MySQL, and additionally
that the dialect will recieve these generated values from the execution
of a :meth:`.Table.insert` construct using techniques appropriate to that
backend.

What's changed is that this feature no longer turns on automatically for a
*composite* primary key; previously, a table definition such as::

    Table(
        'some_table', metadata,
        Column('x', Integer, primary_key=True),
        Column('y', Integer, primary_key=True)
    )

Would have "autoincrement" semantics applied to the ``'x'`` column, only
because it's first in the list of primary key columns.  In order to
disable this, one would have to turn off ``autoincrement`` on all columns::

    # old way
    Table(
        'some_table', metadata,
        Column('x', Integer, primary_key=True, autoincrement=False),
        Column('y', Integer, primary_key=True, autoincrement=False)
    )

With the new behavior, the composite primary key will not have autoincrement
semantics unless a column is marked explcitly with ``autoincrement=True``::

    # column 'y' will be SERIAL/AUTO_INCREMENT/ auto-generating
    Table(
        'some_table', metadata,
        Column('x', Integer, primary_key=True),
        Column('y', Integer, primary_key=True, autoincrement=True)
    )

In order to anticipate some potential backwards-incompatible scenarios,
the :meth:`.Table.insert` construct will perform more thorough checks
for missing primary key values on composite primary key columns that don't
have autoincrement set up; given a table such as::

    Table(
        'b', metadata,
        Column('x', Integer, primary_key=True),
        Column('y', Integer, primary_key=True)
    )

An INSERT emitted with no values for this table will produce the exception::

    CompileError: Column 'b.x' is marked as a member of the primary
    key for table 'b', but has no Python-side or server-side default
    generator indicated, nor does it indicate 'autoincrement=True',
    and no explicit value is passed.  Primary key columns may not
    store NULL. Note that as of SQLAlchemy 1.1, 'autoincrement=True'
    must be indicated explicitly for composite (e.g. multicolumn)
    primary keys if AUTO_INCREMENT/SERIAL/IDENTITY behavior is
    expected for one of the columns in the primary key. CREATE TABLE
    statements are impacted by this change as well on most backends.

For a column that is receiving primary key values from a server-side
default or something less common such as a trigger, the presence of a
value generator can be indicated using :class:`.FetchedValue`::

    Table(
        'b', metadata,
        Column('x', Integer, primary_key=True, server_default=FetchedValue()),
        Column('y', Integer, primary_key=True, server_default=FetchedValue())
    )

For the very unlikely case where a composite primary key is actually intended
to store NULL in one or more of its columns (only supported on SQLite and MySQL),
specify the column with ``nullable=True``::

    Table(
        'b', metadata,
        Column('x', Integer, primary_key=True),
        Column('y', Integer, primary_key=True, nullable=True)
    )

In a related change, the ``autoincrement`` flag may be set to True
on a column that has a client-side or server-side default.  This typically
will not have much impact on the behavior of the column during an INSERT.


.. seealso::

    :ref:`change_mysql_3216`

:ticket:`3216`

.. _change_2528:

A UNION or similar of SELECTs with LIMIT/OFFSET/ORDER BY now parenthesizes the embedded selects
-----------------------------------------------------------------------------------------------

An issue that, like others, was long driven by SQLite's lack of capabilities
has now been enhanced to work on all supporting backends.   We refer to a query that
is a UNION of SELECT statements that themselves contain row-limiting or ordering
features which include LIMIT, OFFSET, and/or ORDER BY::

    (SELECT x FROM table1 ORDER BY y LIMIT 1) UNION
    (SELECT x FROM table2 ORDER BY y LIMIT 2)

The above query requires parenthesis within each sub-select in order to
group the sub-results correctly.  Production of the above statement in
SQLAlchemy Core looks like::

    stmt1 = select([table1.c.x]).order_by(table1.c.y).limit(1)
    stmt2 = select([table1.c.x]).order_by(table2.c.y).limit(2)

    stmt = union(stmt1, stmt2)

Previously, the above construct would not produce parenthesization for the
inner SELECT statements, producing a query that fails on all backends.

The above formats will **continue to fail on SQLite**; additionally, the format
that includes ORDER BY but no LIMIT/SELECT will **continue to fail on Oracle**.
This is not a backwards-incompatible change, because the queries fail without
the parentheses as well; with the fix, the queries at least work on all other
databases.

In all cases, in order to produce a UNION of limited SELECT statements that
also works on SQLite and in all cases on Oracle, the
subqueries must be a SELECT of an ALIAS::

    stmt1 = select([table1.c.x]).order_by(table1.c.y).limit(1).alias().select()
    stmt2 = select([table2.c.x]).order_by(table2.c.y).limit(2).alias().select()

    stmt = union(stmt1, stmt2)

This workaround works on all SQLAlchemy versions.  In the ORM, it looks like::

    stmt1 = session.query(Model1).order_by(Model1.y).limit(1).subquery().select()
    stmt2 = session.query(Model2).order_by(Model2.y).limit(1).subquery().select()

    stmt = session.query(Model1).from_statement(stmt1.union(stmt2))

The behavior here has many parallels to the "join rewriting" behavior
introduced in SQLAlchemy 0.9 in :ref:`feature_joins_09`; however in this case
we have opted not to add new rewriting behavior to accommodate this
case for SQLite.
The existing rewriting behavior is very complicated already, and the case of
UNIONs with parenthesized SELECT statements is much less common than the
"right-nested-join" use case of that feature.

:ticket:`2528`

.. _change_3516:

Array support added to Core; new ANY and ALL operators
------------------------------------------------------

Along with the enhancements made to the Postgresql :class:`.ARRAY`
type described in :ref:`change_3503`, the base class of :class:`.ARRAY`
itself has been moved to Core in a new class :class:`.types.Array`.

Arrays are part of the SQL standard, as are several array-oriented functions
such as ``array_agg()`` and ``unnest()``.  In support of these constructs
for not just PostgreSQL but also potentially for other array-capable backends
in the future such as DB2, the majority of array logic for SQL expressions
is now in Core.   The :class:`.Array` type still **only works on
Postgresql**, however it can be used directly, supporting special array
use cases such as indexed access, as well as support for the ANY and ALL::

    mytable = Table("mytable", metadata,
            Column("data", Array(Integer, dimensions=2))
        )

    expr = mytable.c.data[5][6]

    expr = mytable.c.data[5].any(12)

In support of ANY and ALL, the :class:`.Array` type retains the same
:meth:`.Array.Comparator.any` and :meth:`.Array.Comparator.all` methods
from the PostgreSQL type, but also exports these operations to new
standalone operator functions :func:`.sql.expression.any_` and
:func:`.sql.expression.all_`.  These two functions work in more
of the traditional SQL way, allowing a right-side expression form such
as::

    from sqlalchemy import any_, all_

    select([mytable]).where(12 == any_(mytable.c.data[5]))

For the PostgreSQL-specific operators "contains", "contained_by", and
"overlaps", one should continue to use the :class:`.postgresql.ARRAY`
type directly, which provides all functionality of the :class:`.Array`
type as well.

The :func:`.sql.expression.any_` and :func:`.sql.expression.all_` operators
are open-ended at the Core level, however their interpretation by backend
databases is limited.  On the Postgresql backend, the two operators
**only accept array values**.  Whereas on the MySQL backend, they
**only accept subquery values**.  On MySQL, one can use an expression
such as::

    from sqlalchemy import any_, all_

    subq = select([mytable.c.value])
    select([mytable]).where(12 > any_(subq))


:ticket:`3516`

.. _change_3132:

New Function features, "WITHIN GROUP", array_agg and set aggregate functions
----------------------------------------------------------------------------

With the new :class:`.Array` type we can also implement a pre-typed
function for the ``array_agg()`` SQL function that returns an array,
which is now available using :class:`.array_agg`::

    from sqlalchemy import func
    stmt = select([func.array_agg(table.c.value)])

A Postgresql element for an aggregate ORDER BY is also added via
:class:`.postgresql.aggregate_order_by`::

    from sqlalchemy.dialects.postgresql import aggregate_order_by
    expr = func.array_agg(aggregate_order_by(table.c.a, table.c.b.desc()))
    stmt = select([expr])

Producing::

    SELECT array_agg(table1.a ORDER BY table1.b DESC) AS array_agg_1 FROM table1

The PG dialect itself also provides an :func:`.postgresql.array_agg` wrapper to
ensure the :class:`.postgresql.ARRAY` type::

    from sqlalchemy.dialects.postgresql import array_agg
    stmt = select([array_agg(table.c.value).contains('foo')])


Additionally, functions like ``percentile_cont()``, ``percentile_disc()``,
``rank()``, ``dense_rank()`` and others that require an ordering via
``WITHIN GROUP (ORDER BY <expr>)`` are now available via the
:meth:`.FunctionElement.within_group` modifier::

    from sqlalchemy import func
    stmt = select([
        department.c.id,
        func.percentile_cont(0.5).within_group(
            department.c.salary.desc()
        )
    ])

The above statement would produce SQL similar to::

  SELECT department.id, percentile_cont(0.5)
  WITHIN GROUP (ORDER BY department.salary DESC)

Placeholders with correct return types are now provided for these functions,
and include :class:`.percentile_cont`, :class:`.percentile_disc`,
:class:`.rank`, :class:`.dense_rank`, :class:`.mode`, :class:`.percent_rank`,
and :class:`.cume_dist`.

:ticket:`3132` :ticket:`1370`

.. _change_2919:

TypeDecorator now works with Enum, Boolean, "schema" types automatically
------------------------------------------------------------------------

The :class:`.SchemaType` types include types such as :class:`.Enum`
and :class:`.Boolean` which, in addition to corresponding to a database
type, also generate either a CHECK constraint or in the case of Postgresql
ENUM a new CREATE TYPE statement, will now work automatically with
:class:`.TypeDecorator` recipes.  Previously, a :class:`.TypeDecorator` for
an :class:`.postgresql.ENUM` had to look like this::

    # old way
    class MyEnum(TypeDecorator, SchemaType):
        impl = postgresql.ENUM('one', 'two', 'three', name='myenum')

        def _set_table(self, table):
            self.impl._set_table(table)

The :class:`.TypeDecorator` now propagates those additional events so it
can be done like any other type::

    # new way
    class MyEnum(TypeDecorator):
        impl = postgresql.ENUM('one', 'two', 'three', name='myenum')


:ticket:`2919`

.. _change_3531:

The type_coerce function is now a persistent SQL element
--------------------------------------------------------

The :func:`.expression.type_coerce` function previously would return
an object either of type :class:`.BindParameter` or :class:`.Label`, depending
on the input.  An effect this would have was that in the case where expression
transformations were used, such as the conversion of an element from a
:class:`.Column` to a :class:`.BindParameter` that's critical to ORM-level
lazy loading, the type coercion information would not be used since it would
have been lost already.

To improve this behavior, the function now returns a persistent
:class:`.TypeCoerce` container around the given expression, which itself
remains unaffected; this construct is evaluated explicitly by the
SQL compiler.  This allows for the coercion of the inner expression
to be maintained no matter how the statement is modified, including if
the contained element is replaced with a different one, as is common
within the ORM's lazy loading feature.

The test case illustrating the effect makes use of a heterogeneous
primaryjoin condition in conjunction with custom types and lazy loading.
Given a custom type that applies a CAST as a "bind expression"::

    class StringAsInt(TypeDecorator):
        impl = String

        def column_expression(self, col):
            return cast(col, Integer)

        def bind_expression(self, value):
            return cast(value, String)

Then, a mapping where we are equating a string "id" column on one
table to an integer "id" column on the other::

    class Person(Base):
        __tablename__ = 'person'
        id = Column(StringAsInt, primary_key=True)

        pets = relationship(
            'Pets',
            primaryjoin=(
                'foreign(Pets.person_id)'
                '==cast(type_coerce(Person.id, Integer), Integer)'
            )
        )

    class Pets(Base):
        __tablename__ = 'pets'
        id = Column('id', Integer, primary_key=True)
        person_id = Column('person_id', Integer)

Above, in the :paramref:`.relationship.primaryjoin` expression, we are
using :func:`.type_coerce` to handle bound parameters passed via
lazyloading as integers, since we already know these will come from
our ``StringAsInt`` type which maintains the value as an integer in
Python. We are then using :func:`.cast` so that as a SQL expression,
the VARCHAR "id"  column will be CAST to an integer for a regular non-
converted join as with :meth:`.Query.join` or :func:`.orm.joinedload`.
That is, a joinedload of ``.pets`` looks like::

    SELECT person.id AS person_id, pets_1.id AS pets_1_id,
           pets_1.person_id AS pets_1_person_id
    FROM person
    LEFT OUTER JOIN pets AS pets_1
    ON pets_1.person_id = CAST(person.id AS INTEGER)

Without the CAST in the ON clause of the join, strongly-typed databases
such as Postgresql will refuse to implicitly compare the integer and fail.

The lazyload case of ``.pets`` relies upon replacing
the ``Person.id`` column at load time with a bound parameter, which receives
a Python-loaded value.  This replacement is specifically where the intent
of our :func:`.type_coerce` function would be lost.  Prior to the change,
this lazy load comes out as::

    SELECT pets.id AS pets_id, pets.person_id AS pets_person_id
    FROM pets
    WHERE pets.person_id = CAST(CAST(%(param_1)s AS VARCHAR) AS INTEGER)
    {'param_1': 5}

Where above, we see that our in-Python value of ``5`` is CAST first
to a VARCHAR, then back to an INTEGER in SQL; a double CAST which works,
but is nevertheless not what we asked for.

With the change, the :func:`.type_coerce` function maintains a wrapper
even after the column is swapped out for a bound parameter, and the query now
looks like::

    SELECT pets.id AS pets_id, pets.person_id AS pets_person_id
    FROM pets
    WHERE pets.person_id = CAST(%(param_1)s AS INTEGER)
    {'param_1': 5}

Where our outer CAST that's in our primaryjoin still takes effect, but the
needless CAST that's in part of the ``StringAsInt`` custom type is removed
as intended by the :func:`.type_coerce` function.


:ticket:`3531`


Key Behavioral Changes - ORM
============================


Key Behavioral Changes - Core
=============================


Dialect Improvements and Changes - Postgresql
=============================================

.. _change_3499_postgresql:

ARRAY and JSON types now correctly specify "unhashable"
-------------------------------------------------------

As described in :ref:`change_3499`, the ORM relies upon being able to
produce a hash function for column values when a query's selected entities
mixes full ORM entities with column expressions.   The ``hashable=False``
flag is now correctly set on all of PG's "data structure" types, including
:class:`.ARRAY` and :class:`.JSON`.  The :class:`.JSONB` and :class:`.HSTORE`
types already included this flag.  For :class:`.ARRAY`,
this is conditional based on the :paramref:`.postgresql.ARRAY.as_tuple`
flag, however it should no longer be necessary to set this flag
in order to have an array value present in a composed ORM row.

.. seealso::

    :ref:`change_3499`

    :ref:`change_3503`

:ticket:`3499`

.. _change_3503:

Correct SQL Types are Established from Indexed Access of ARRAY, JSON, HSTORE
-----------------------------------------------------------------------------

For all three of :class:`~.postgresql.ARRAY`, :class:`~.postgresql.JSON` and :class:`.HSTORE`,
the SQL type assigned to the expression returned by indexed access, e.g.
``col[someindex]``, should be correct in all cases.

This includes:

* The SQL type assigned to indexed access of an :class:`~.postgresql.ARRAY` takes into
  account the number of dimensions configured.   An :class:`~.postgresql.ARRAY` with three
  dimensions will return a SQL expression with a type of :class:`~.postgresql.ARRAY` of
  one less dimension.  Given a column with type ``ARRAY(Integer, dimensions=3)``,
  we can now perform this expression::

      int_expr = col[5][6][7]   # returns an Integer expression object

  Previously, the indexed access to ``col[5]`` would return an expression of
  type :class:`.Integer` where we could no longer perform indexed access
  for the remaining dimensions, unless we used :func:`.cast` or :func:`.type_coerce`.

* The :class:`~.postgresql.JSON` and :class:`~.postgresql.JSONB` types now mirror what Postgresql
  itself does for indexed access.  This means that all indexed access for
  a :class:`~.postgresql.JSON` or :class:`~.postgresql.JSONB` type returns an expression that itself
  is *always* :class:`~.postgresql.JSON` or :class:`~.postgresql.JSONB` itself, unless the
  :attr:`~.postgresql.JSON.Comparator.astext` modifier is used.   This means that whether
  the indexed access of the JSON structure ultimately refers to a string,
  list, number, or other JSON structure, Postgresql always considers it
  to be JSON itself unless it is explicitly cast differently.   Like
  the :class:`~.postgresql.ARRAY` type, this means that it is now straightforward
  to produce JSON expressions with multiple levels of indexed access::

    json_expr = json_col['key1']['attr1'][5]

* The "textual" type that is returned by indexed access of :class:`.HSTORE`
  as well as the "textual" type that is returned by indexed access of
  :class:`~.postgresql.JSON` and :class:`~.postgresql.JSONB` in conjunction with the
  :attr:`~.postgresql.JSON.Comparator.astext` modifier is now configurable; it defaults
  to :class:`.Text` in both cases but can be set to a user-defined
  type using the :paramref:`.postgresql.JSON.astext_type` or
  :paramref:`.postgresql.HSTORE.text_type` parameters.

.. seealso::

  :ref:`change_3503_cast`

:ticket:`3499`
:ticket:`3487`

.. _change_3503_cast:

The JSON cast() operation now requires ``.astext`` is called explicitly
------------------------------------------------------------------------

As part of the changes in :ref:`change_3503`, the workings of the
:meth:`.ColumnElement.cast` operator on :class:`.postgresql.JSON` and
:class:`.postgresql.JSONB` no longer implictly invoke the
:attr:`.JSON.Comparator.astext` modifier; Postgresql's JSON/JSONB types
support CAST operations to each other without the "astext" aspect.

This means that in most cases, an application that was doing this::

    expr = json_col['somekey'].cast(Integer)

Will now need to change to this::

    expr = json_col['somekey'].astext.cast(Integer)



.. _change_3514:

Postgresql JSON "null" is inserted as expected with ORM operations, regardless of column default present
-----------------------------------------------------------------------------------------------------------

The :class:`.JSON` type has a flag :paramref:`.JSON.none_as_null` which
when set to True indicates that the Python value ``None`` should translate
into a SQL NULL rather than a JSON NULL value.  This flag defaults to False,
which means that the column should *never* insert SQL NULL or fall back
to a default unless the :func:`.null` constant were used.  However, this would
fail in the ORM under two circumstances; one is when the column also contained
a default or server_default value, a positive value of ``None`` on the mapped
attribute would still result in the column-level default being triggered,
replacing the ``None`` value::

    obj = MyObject(json_value=None)
    session.add(obj)
    session.commit()   # would fire off default / server_default, not encode "'none'"

The other is when the :meth:`.Session.bulk_insert_mappings`
method were used, ``None`` would be ignored in all cases::

    session.bulk_insert_mappings(
        MyObject,
        [{"json_value": None}])  # would insert SQL NULL and/or trigger defaults

The :class:`.JSON` type now implements the
:attr:`.TypeEngine.should_evaluate_none` flag,
indicating that ``None`` should not be ignored here; it is configured
automatically based on the value of :paramref:`.JSON.none_as_null`.
Thanks to :ticket:`3061`, we can differentiate when the value ``None`` is actively
set by the user versus when it was never set at all.

If the attribute is not set at all, then column level defaults *will*
fire off and/or SQL NULL will be inserted as expected, as was the behavior
previously.  Below, the two variants are illustrated::

    obj = MyObject(json_value=None)
    session.add(obj)
    session.commit()   # *will not* fire off column defaults, will insert JSON 'null'

    obj = MyObject()
    session.add(obj)
    session.commit()   # *will* fire off column defaults, and/or insert SQL NULL

:ticket:`3514`

.. seealso::

      :ref:`change_3250`

      :ref:`change_3514_jsonnull`

.. _change_3514_jsonnull:

New JSON.NULL Constant Added
----------------------------

To ensure that an application can always have full control at the value level
of whether a :class:`.postgresql.JSON` or :class:`.postgresql.JSONB` column
should receive a SQL NULL or JSON ``"null"`` value, the constant
:attr:`.postgresql.JSON.NULL` has been added, which in conjunction with
:func:`.null` can be used to determine fully between SQL NULL and
JSON ``"null"``, regardless of what :paramref:`.JSON.none_as_null` is set
to::

    from sqlalchemy import null
    from sqlalchemy.dialects.postgresql import JSON

    obj1 = MyObject(json_value=null())  # will *always* insert SQL NULL
    obj2 = MyObject(json_value=JSON.NULL)  # will *always* insert JSON string "null"

    session.add_all([obj1, obj2])
    session.commit()

.. seealso::

    :ref:`change_3514`

:ticket:`3514`

.. _change_2729:

ARRAY with ENUM will now emit CREATE TYPE for the ENUM
------------------------------------------------------

A table definition like the following will now emit CREATE TYPE
as expected::

    enum = Enum(
        'manager', 'place_admin', 'carwash_admin',
        'parking_admin', 'service_admin', 'tire_admin',
        'mechanic', 'carwasher', 'tire_mechanic', name="work_place_roles")

    class WorkPlacement(Base):
        __tablename__ = 'work_placement'
        id = Column(Integer, primary_key=True)
        roles = Column(ARRAY(enum))


    e = create_engine("postgresql://scott:tiger@localhost/test", echo=True)
    Base.metadata.create_all(e)

emits::

    CREATE TYPE work_place_roles AS ENUM (
        'manager', 'place_admin', 'carwash_admin', 'parking_admin',
        'service_admin', 'tire_admin', 'mechanic', 'carwasher',
        'tire_mechanic')

    CREATE TABLE work_placement (
        id SERIAL NOT NULL,
        roles work_place_roles[],
        PRIMARY KEY (id)
    )


:ticket:`2729`

Dialect Improvements and Changes - MySQL
=============================================

.. _change_mysql_3216:

No more generation of an implicit KEY for composite primary key w/ AUTO_INCREMENT
---------------------------------------------------------------------------------

The MySQL dialect had the behavior such that if a composite primary key
on an InnoDB table featured AUTO_INCREMENT on one of its columns which was
not the first column, e.g.::

    t = Table(
        'some_table', metadata,
        Column('x', Integer, primary_key=True, autoincrement=False),
        Column('y', Integer, primary_key=True, autoincrement=True),
        mysql_engine='InnoDB'
    )

DDL such as the following would be generated::

    CREATE TABLE some_table (
        x INTEGER NOT NULL,
        y INTEGER NOT NULL AUTO_INCREMENT,
        PRIMARY KEY (x, y),
        KEY idx_autoinc_y (y)
    )ENGINE=InnoDB

Note the above "KEY" with an auto-generated name; this is a change that
found its way into the dialect many years ago in response to the issue that
the AUTO_INCREMENT would otherwise fail on InnoDB without this additional KEY.

This workaround has been removed and replaced with the much better system
of just stating the AUTO_INCREMENT column *first* within the primary key::

    CREATE TABLE some_table (
        x INTEGER NOT NULL,
        y INTEGER NOT NULL AUTO_INCREMENT,
        PRIMARY KEY (y, x)
    )ENGINE=InnoDB

Along with the change :ref:`change_3216`, composite primary keys with
or without auto increment are now easier to specify;
:paramref:`.Column.autoincrement`
now defaults to the value ``"auto"`` and the ``autoincrement=False``
directives are no longer needed::

    t = Table(
        'some_table', metadata,
        Column('x', Integer, primary_key=True),
        Column('y', Integer, primary_key=True, autoincrement=True),
        mysql_engine='InnoDB'
    )



Dialect Improvements and Changes - SQLite
=============================================

.. _change_sqlite_schemas:

Improved Support for Remote Schemas
------------------------------------

The SQLite dialect now implements :meth:`.Inspector.get_schema_names`
and additionally has improved support for tables and indexes that are
created and reflected from a remote schema, which in SQLite is a
database that is assigned a name via the ``ATTACH`` statement; previously,
the ``CREATE INDEX`` DDL didn't work correctly for a schema-bound table
and the :meth:`.Inspector.get_foreign_keys` method will now indicate the
given schema in the results.  Cross-schema foreign keys aren't supported.


Dialect Improvements and Changes - SQL Server
=============================================

.. _change_3504:

String / varlength types no longer represent "max" explicitly on reflection
---------------------------------------------------------------------------

When reflecting a type such as :class:`.String`, :class:`.Text`, etc.
which includes a length, an "un-lengthed" type under SQL Server would
copy the "length" parameter as the value ``"max"``::

    >>> from sqlalchemy import create_engine, inspect
    >>> engine = create_engine('mssql+pyodbc://scott:tiger@ms_2008', echo=True)
    >>> engine.execute("create table s (x varchar(max), y varbinary(max))")
    >>> insp = inspect(engine)
    >>> for col in insp.get_columns("s"):
    ...     print col['type'].__class__, col['type'].length
    ...
    <class 'sqlalchemy.sql.sqltypes.VARCHAR'> max
    <class 'sqlalchemy.dialects.mssql.base.VARBINARY'> max

The "length" parameter in the base types is expected to be an integer value
or None only; None indicates unbounded length which the SQL Server dialect
interprets as "max".   The fix then is so that these lengths come
out as None, so that the type objects work in non-SQL Server contexts::

    >>> for col in insp.get_columns("s"):
    ...     print col['type'].__class__, col['type'].length
    ...
    <class 'sqlalchemy.sql.sqltypes.VARCHAR'> None
    <class 'sqlalchemy.dialects.mssql.base.VARBINARY'> None

Applications which may have been relying on a direct comparison of the "length"
value to the string "max" should consider the value of ``None`` to mean
the same thing.

:ticket:`3504`

.. _change_3434:

The legacy_schema_aliasing flag is now set to False
---------------------------------------------------

SQLAlchemy 1.0.5 introduced the ``legacy_schema_aliasing`` flag to the
MSSQL dialect, allowing so-called "legacy mode" aliasing to be turned off.
This aliasing attempts to turn schema-qualified tables into aliases;
given a table such as::

    account_table = Table(
        'account', metadata,
        Column('id', Integer, primary_key=True),
        Column('info', String(100)),
        schema="customer_schema"
    )

The legacy mode of behavior will attempt to turn a schema-qualified table
name into an alias::

    >>> eng = create_engine("mssql+pymssql://mydsn", legacy_schema_aliasing=True)
    >>> print(account_table.select().compile(eng))
    SELECT account_1.id, account_1.info
    FROM customer_schema.account AS account_1

However, this aliasing has been shown to be unnecessary and in many cases
produces incorrect SQL.

In SQLAlchemy 1.1, the ``legacy_schema_aliasing`` flag now defaults to
False, disabling this mode of behavior and allowing the MSSQL dialect to behave
normally with schema-qualified tables.  For applications which may rely
on this behavior, set the flag back to True.


:ticket:`3434`

Dialect Improvements and Changes - Oracle
=============================================
