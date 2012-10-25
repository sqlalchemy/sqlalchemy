==============================
What's New in SQLAlchemy 0.8?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.7,
    undergoing maintenance releases as of October, 2012,
    and SQLAlchemy version 0.8, which is expected for release
    in late 2012.

    Document date: October 25, 2012

Introduction
============

This guide introduces what's new in SQLAlchemy version 0.8,
and also documents changes which affect users migrating
their applications from the 0.7 series of SQLAlchemy to 0.8.

SQLAlchemy releases are closing in on 1.0, and each new
version since 0.5 features fewer major usage changes.   Most
applications that are settled into modern 0.7 patterns
should be movable to 0.8 with no changes. Applications that
use 0.6 and even 0.5 patterns should be directly migratable
to 0.8 as well, though larger applications may want to test
with each interim version.

Platform Support
================

Targeting Python 2.5 and Up Now
-------------------------------

SQLAlchemy 0.8 will target Python 2.5 and forward;
compatibility for Python 2.4 is being dropped.

The internals will be able to make usage of Python ternaries
(that is, ``x if y else z``) which will improve things
versus the usage of ``y and x or z``, which naturally has
been the source of some bugs, as well as context managers
(that is, ``with:``) and perhaps in some cases
``try:/except:/else:`` blocks which will help with code
readability.

SQLAlchemy will eventually drop 2.5 support as well - when
2.6 is reached as the baseline, SQLAlchemy will move to use
2.6/3.3 in-place compatibility, removing the usage of the
``2to3`` tool and maintaining a source base that works with
Python 2 and 3 at the same time.

New Features
============

.. _feature_relationship_08:

Rewritten :func:`.relationship` mechanics
-----------------------------------------

0.8 features a much improved and capable system regarding
how :func:`.relationship` determines how to join between two
entities.  The new system includes these features:

* The ``primaryjoin`` argument is **no longer needed** when
  constructing a :func:`.relationship`   against a class that
  has multiple foreign key paths to the target.  Only the
  ``foreign_keys``   argument is needed to specify those
  columns which should be included:

  ::


        class Parent(Base):
            __tablename__ = 'parent'
            id = Column(Integer, primary_key=True)
            child_id_one = Column(Integer, ForeignKey('child.id'))
            child_id_two = Column(Integer, ForeignKey('child.id'))

            child_one = relationship("Child", foreign_keys=child_id_one)
            child_two = relationship("Child", foreign_keys=child_id_two)

        class Child(Base):
            __tablename__ = 'child'
            id = Column(Integer, primary_key=True)

* relationships against self-referential, composite foreign
  keys where **a column points to itself**   are now
  supported.   The canonical case is as follows:

  ::

        class Folder(Base):
            __tablename__ = 'folder'
            __table_args__ = (
              ForeignKeyConstraint(
                  ['account_id', 'parent_id'],
                  ['folder.account_id', 'folder.folder_id']),
            )

            account_id = Column(Integer, primary_key=True)
            folder_id = Column(Integer, primary_key=True)
            parent_id = Column(Integer)
            name = Column(String)

            parent_folder = relationship("Folder",
                                backref="child_folders",
                                remote_side=[account_id, folder_id]
                          )

  Above, the ``Folder`` refers to its parent ``Folder``
  joining from ``account_id`` to itself, and ``parent_id``
  to ``folder_id``.  When SQLAlchemy constructs an auto-
  join, no longer can it assume all columns on the "remote"
  side are aliased, and all columns on the "local" side are
  not - the ``account_id`` column is **on both sides**.   So
  the internal relationship mechanics were totally rewritten
  to support an entirely different system whereby two copies
  of ``account_id`` are generated, each containing different
  *annotations* to determine their role within the
  statement.  Note the join condition within a basic eager
  load:

  ::

        SELECT
            folder.account_id AS folder_account_id,
            folder.folder_id AS folder_folder_id,
            folder.parent_id AS folder_parent_id,
            folder.name AS folder_name,
            folder_1.account_id AS folder_1_account_id,
            folder_1.folder_id AS folder_1_folder_id,
            folder_1.parent_id AS folder_1_parent_id,
            folder_1.name AS folder_1_name
        FROM folder
            LEFT OUTER JOIN folder AS folder_1
            ON
                folder_1.account_id = folder.account_id
                AND folder.folder_id = folder_1.parent_id

        WHERE folder.folder_id = ? AND folder.account_id = ?

* Thanks to the new relationship mechanics, new
  **annotation** functions :func:`.foreign` and :func:`.remote`
  are provided   which can be used
  to create ``primaryjoin`` conditions involving any kind of
  SQL function, CAST,  or other construct that wraps the
  target column.  Previously, a semi-public argument
  ``_local_remote_pairs`` would be used to tell
  :func:`.relationship` unambiguously what columns   should be
  considered as corresponding to the mapping - the
  annotations make the point   more directly, such as below
  where ``Parent`` joins to ``Child`` by matching the
  ``Parent.name`` column converted to lower case to that of
  the ``Child.name_upper`` column:

  ::


        class Parent(Base):
            __tablename__ = 'parent'
            id = Column(Integer, primary_key=True)
            name = Column(String)
            children = relationship("Child",
                    primaryjoin="Parent.name==foreign(func.lower(Child.name_upper))"
                )

        class Child(Base):
            __tablename__ = 'child'
            id = Column(Integer, primary_key=True)
            name_upper = Column(String)

.. seealso::

    :func:`.foreign`

    :func:`.remote`

    :func:`.relationship`

:ticket:`1401` :ticket:`610`

New Class Inspection System
---------------------------

Lots of SQLAlchemy users are writing systems that require
the ability to inspect the attributes of a mapped class,
including being able to get at the primary key columns,
object relationships, plain attributes, and so forth,
typically for the purpose of building data-marshalling
systems, like JSON/XML conversion schemes and of course form
libraries galore.

Originally, the :class:`.Table` and :class:`.Column` model were the
original inspection points, which have a well-documented
system.  While SQLAlchemy ORM models are also fully
introspectable, this has never been a fully stable and
supported feature, and users tended to not have a clear idea
how to get at this information.

0.8 has a plan to produce a consistent, stable and fully
documented API for this purpose, which would provide an
inspection system that works on classes, instances, and
possibly other things as well.   While many elements of this
system are already available, the plan is to lock down the
API including various accessors available from such objects
as :class:`.Mapper`, :class:`.InstanceState`, and :class:`.MapperProperty`:

::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        name_syn = synonym(name)
        addresses = relationship(Address)

    # universal entry point is inspect()
    >>> b = inspect(User)

    # column collection
    >>> b.columns
    [<id column>, <name column>]

    # its a ColumnCollection
    >>> b.columns.id
    <id column>

    # i.e. from mapper
    >>> b.primary_key
    (<id column>, )

    # ColumnProperty
    >>> b.attr.id.columns
    [<id column>]

    # get only column attributes
    >>> b.column_attrs
    [<id prop>, <name prop>]

    # its a namespace
    >>> b.column_attrs.id
    <id prop>

    # get only relationships
    >>> b.relationships
    [<addresses prop>]

    # its a namespace
    >>> b.relationships.addresses
    <addresses prop>

    # point inspect() at a class level attribute,
    # basically returns ".property"
    >>> b = inspect(User.addresses)
    >>> b
    <addresses prop>

    # mapper
    >>> b.mapper
    <Address mapper>

    # None columns collection, just like columnprop has empty mapper
    >>> b.columns
    None

    # the parent
    >>> b.parent
    <User mapper>

    # __clause_element__()
    >>> b.expression
    User.id==Address.user_id

    >>> inspect(User.id).expression
    <id column with ORM annotations>

    # inspect works on instances !
    >>> u1 = User(id=3, name='x')
    >>> b = inspect(u1)

    # what's b here ?  probably InstanceState
    >>> b
    <InstanceState>

    >>> b.attr.keys()
    ['id', 'name', 'name_syn', 'addresses']

    # attribute interface
    >>> b.attr.id
    <magic attribute inspect thing>

    # value
    >>> b.attr.id.value
    3

    # history
    >>> b.attr.id.history
    <history object>

    >>> b.attr.id.history.unchanged
    3

    >>> b.attr.id.history.deleted
    None

    # lets assume the object is persistent
    >>> s = Session()
    >>> s.add(u1)
    >>> s.commit()

    # big one - the primary key identity !  always
    # works in query.get()
    >>> b.identity
    [3]

    # the mapper level key
    >>> b.identity_key
    (User, [3])

    >>> b.persistent
    True

    >>> b.transient
    False

    >>> b.deleted
    False

    >>> b.detached
    False

    >>> b.session
    <session>

.. seealso::

    :ref:`core_inspection_toplevel`

:ticket:`2208`

Fully extensible, type-level operator support in Core
-----------------------------------------------------

The Core has to date never had any system of adding support
for new SQL operators to Column and other expression
constructs, other than the :meth:`.ColumnOperators.op` method
which is "just enough" to make things work. There has also
never been any system in place for Core which allows the
behavior of existing operators to be overridden.   Up until
now, the only way operators could be flexibly redefined was
in the ORM layer, using :func:`.column_property` given a
``comparator_factory`` argument.   Third party libraries
like GeoAlchemy therefore were forced to be ORM-centric and
rely upon an array of hacks to apply new opertions as well
as to get them to propagate correctly.

The new operator system in Core adds the one hook that's
been missing all along, which is to associate new and
overridden operators with *types*.   Since after all, it's
not really a column, CAST operator, or SQL function that
really drives what kinds of operations are present, it's the
*type* of the expression.   The implementation details are
minimal - only a few extra methods are added to the core
:class:`.ColumnElement` type so that it consults it's
:class:`.TypeEngine` object for an optional set of operators.
New or revised operations can be associated with any type,
either via subclassing of an existing type, by using
:class:`.TypeDecorator`, or "globally across-the-board" by
attaching a new :class:`.TypeEngine.Comparator` object to an existing type
class.

For example, to add logarithm support to :class:`.Numeric` types:

::


    from sqlalchemy.types import Numeric
    from sqlalchemy.sql import func

    class CustomNumeric(Numeric):
        class comparator_factory(Numeric.Comparator):
            def log(self, other):
                return func.log(self.expr, other)

The new type is usable like any other type:

::


    data = Table('data', metadata,
              Column('id', Integer, primary_key=True),
              Column('x', CustomNumeric(10, 5)),
              Column('y', CustomNumeric(10, 5))
         )

    stmt = select([data.c.x.log(data.c.y)]).where(data.c.x.log(2) < value)
    print conn.execute(stmt).fetchall()


New features which should come from this immediately are
support for Postgresql's HSTORE type, which is ready to go
in a separate library which may be merged, as well as all
the special operations associated with Postgresql's ARRAY
type.    It also paves the way for existing types to acquire
lots more operators that are specific to those types, such
as more string, integer and date operators.

    .. seealso::

        `Postgresql HSTORE <https://bitbucket.org/audriusk/hstore>`_ - support for HSTORE in SQLAlchemy

:ticket:`2547`

New with_polymorphic() feature, can be used anywhere
----------------------------------------------------

The :meth:`.Query.with_polymorphic` method allows the user to
specify which tables should be present when querying against
a joined-table entity.   Unfortunately the method is awkward
and only applies to the first entity in the list, and
otherwise has awkward behaviors both in usage as well as
within the internals.  A new enhancement to the
:func:`.aliased` construct has been added called
:func:`.with_polymorphic` which allows any entity to be
"aliased" into a "polymorphic" version of itself, freely
usable anywhere:

::

    from sqlalchemy.orm import with_polymorphic
    palias = with_polymorphic(Person, [Engineer, Manager])
    session.query(Company).\
                join(palias, Company.employees).\
                filter(or_(Engineer.language=='java', Manager.hair=='pointy'))

.. seealso::

    :ref:`with_polymorphic` - newly updated documentation for polymorphic
    loading control.

:ticket:`2333`

of_type() works with alias(), with_polymorphic(), any(), has(), joinedload(), subqueryload(), contains_eager()
--------------------------------------------------------------------------------------------------------------

You can use :meth:`.PropComparator.of_type` with aliases and polymorphic
constructs; also works with most relationship functions like
:func:`.joinedload`, :func:`.subqueryload`, :func:`.contains_eager`,
:meth:`.PropComparator.any`, and :meth:`.PropComparator.has`:

::


    # use eager loading in conjunction with with_polymorphic targets
    Job_P = with_polymorphic(Job, SubJob, aliased=True)
    q = s.query(DataContainer).\
                join(DataContainer.jobs.of_type(Job_P)).\
                    options(contains_eager(DataContainer.jobs.of_type(Job_P)))

    # pass subclasses to eager loads (implicitly applies with_polymorphic)
    q = s.query(ParentThing).\
                    options(
                        joinedload_all(
                            ParentThing.container,
                            DataContainer.jobs.of_type(SubJob)
                    ))

    # control self-referential aliasing with any()/has()
    Job_A = aliased(Job)
    q = s.query(Job).join(DataContainer.jobs).\
                    filter(
                        DataContainer.jobs.of_type(Job_A).\
                            any(and_(Job_A.id < Job.id, Job_A.type=='fred'))


:ticket:`2438` :ticket:`1106`

New DeferredReflection Feature in Declarative
---------------------------------------------

The "deferred reflection" example has been moved to a
supported feature within Declarative.  This feature allows
the construction of declarative mapped classes with only
placeholder ``Table`` metadata, until a ``prepare()`` step
is called, given an ``Engine`` with which to reflect fully
all tables and establish actual mappings.   The system
supports overriding of columns, single and joined
inheritance, as well as distinct bases-per-engine. A full
declarative configuration can now be created against an
existing table that is assembled upon engine creation time
in one step:

::

    class ReflectedOne(DeferredReflection, Base):
        __abstract__ = True

    class ReflectedTwo(DeferredReflection, Base):
        __abstract__ = True

    class MyClass(ReflectedOne):
        __tablename__ = 'mytable'

    class MyOtherClass(ReflectedOne):
        __tablename__ = 'myothertable'

    class YetAnotherClass(ReflectedTwo):
        __tablename__ = 'yetanothertable'

    ReflectedOne.prepare(engine_one)
    ReflectedTwo.prepare(engine_two)

:ticket:`2485`

New, configurable DATE, TIME types for SQLite
---------------------------------------------

SQLite has no built-in DATE, TIME, or DATETIME types, and
instead provides some support for storage of date and time
values either as strings or integers.   The date and time
types for SQLite are enhanced in 0.8 to be much more
configurable as to the specific format, including that the
"microseconds" portion is optional, as well as pretty much
everything else.

::

    Column('sometimestamp', sqlite.DATETIME(truncate_microseconds=True))
    Column('sometimestamp', sqlite.DATETIME(
                        storage_format=(
                                    "%(year)04d%(month)02d%(day)02d"
                                    "%(hour)02d%(minute)02d%(second)02d%(microsecond)06d"
                        ),
                        regexp="(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{6})"
                        )
                )
    Column('somedate', sqlite.DATE(
                        storage_format="%(month)02d/%(day)02d/%(year)04d",
                        regexp="(?P<month>\d+)/(?P<day>\d+)/(?P<year>\d+)",
                    )
                )


Huge thanks to Nate Dub for the sprinting on this at Pycon
'12.

:ticket:`2363`

Query.update() will support UPDATE..FROM
----------------------------------------

Not 100% sure if this will make it in, the new UPDATE..FROM
mechanics should work in query.update():

::

    query(SomeEntity).\
        filter(SomeEntity.id==SomeOtherEntity.id).\
        filter(SomeOtherEntity.foo=='bar').\
        update({"data":"x"})

Should also work when used against a joined-inheritance
entity, provided the target of the UPDATE is local to the
table being filtered on, or if the parent and child tables
are mixed, they are joined explicitly in the query.  Below,
given ``Engineer`` as a joined subclass of ``Person``:

::

    query(Engineer).\
            filter(Person.id==Engineer.id).\
            filter(Person.name=='dilbert').\
            update({"engineer_data":"java"})

would produce:

::

    UPDATE engineer SET engineer_data='java' FROM person
    WHERE person.id=engineer.id AND person.name='dilbert'

:ticket:`2365`

Enhanced Postgresql ARRAY type
------------------------------

The ``postgresql.ARRAY`` type will accept an optional
"dimension" argument, pinning it to a fixed number of
dimensions and greatly improving efficiency when retrieving
results:

::

    # old way, still works since PG supports N-dimensions per row:
    Column("my_array", postgresql.ARRAY(Integer))

    # new way, will render ARRAY with correct number of [] in DDL,
    # will process binds and results more efficiently as we don't need
    # to guess how many levels deep to go
    Column("my_array", postgresql.ARRAY(Integer, dimensions=2))

:ticket:`2441`

rollback() will only roll back "dirty" objects from a begin_nested()
--------------------------------------------------------------------

A behavioral change that should improve efficiency for those
users using SAVEPOINT via ``Session.begin_nested()`` - upon
``rollback()``, only those objects that were made dirty
since the last flush will be expired, the rest of the
``Session`` remains intact.  This because a ROLLBACK to a
SAVEPOINT does not terminate the containing transaction's
isolation, so no expiry is needed except for those changes
that were not flushed in the current transaction.

:ticket:`2452`

Behavioral Changes
==================

The after_attach event fires after the item is associated with the Session instead of before; before_attach added
-----------------------------------------------------------------------------------------------------------------

Event handlers which use after_attach can now assume the
given instance is associated with the given session:

::

    @event.listens_for(Session, "after_attach")
    def after_attach(session, instance):
        assert instance in session

Some use cases require that it work this way.  However,
other use cases require that the item is *not* yet part of
the session, such as when a query, intended to load some
state required for an instance, emits autoflush first and
would otherwise prematurely flush the target object.  Those
use cases should use the new "before_attach" event:

::

    @event.listens_for(Session, "before_attach")
    def before_attach(session, instance):
        instance.some_necessary_attribute = session.query(Widget).\
                                                filter_by(instance.widget_name).\
                                                first()

:ticket:`2464`

Query now auto-correlates like a select() does
----------------------------------------------

Previously it was necessary to call ``Query.correlate`` in
order to have a column- or WHERE-subquery correlate to the
parent:

::

    subq = session.query(Entity.value).\
                    filter(Entity.id==Parent.entity_id).\
                    correlate(Parent).\
                    as_scalar()
    session.query(Parent).filter(subq=="some value")

This was the opposite behavior of a plain ``select()``
construct which would assume auto-correlation by default.
The above statement in 0.8 will correlate automatically:

::

    subq = session.query(Entity.value).\
                    filter(Entity.id==Parent.entity_id).\
                    as_scalar()
    session.query(Parent).filter(subq=="some value")

like in ``select()``, correlation can be disabled by calling
``query.correlate(None)`` or manually set by passing an
entity, ``query.correlate(someentity)``.

:ticket:`2179`

No more magic coercion of "=" to IN when comparing to subquery in MS-SQL
------------------------------------------------------------------------

We found a very old behavior in the MSSQL dialect which
would attempt to rescue the user from his or herself when
doing something like this:

::

    scalar_subq = select([someothertable.c.id]).where(someothertable.c.data=='foo')
    select([sometable]).where(sometable.c.id==scalar_subq)

SQL Server doesn't allow an equality comparison to a scalar
SELECT, that is, "x = (SELECT something)". The MSSQL dialect
would convert this to an IN.   The same thing would happen
however upon a comparison like "(SELECT something) = x", and
overall this level of guessing is outside of SQLAlchemy's
usual scope so the behavior is removed.

:ticket:`2277`

Fixed the behavior of Session.is_modified()
-------------------------------------------

The ``Session.is_modified()`` method accepts an argument
``passive`` which basically should not be necessary, the
argument in all cases should be the value ``True`` - when
left at its default of ``False`` it would have the effect of
hitting the database, and often triggering autoflush which
would itself change the results.   In 0.8 the ``passive``
argument will have no effect, and unloaded attributes will
never be checked for history since by definition there can
be no pending state change on an unloaded attribute.

:ticket:`2320`

``column.key`` is honored in the ``.c.`` attribute of ``select()`` with ``apply_labels()``
------------------------------------------------------------------------------------------

Users of the expression system know that ``apply_labels()``
prepends the table name to each column name, affecting the
names that are available from ``.c.``:

::

    s = select([table1]).apply_labels()
    s.c.table1_col1
    s.c.table1_col2

Before 0.8, if the ``Column`` had a different ``key``, this
key would be ignored, inconsistently versus when
``apply_labels()`` were not used:

::

    # before 0.8
    table1 = Table('t1', metadata,
        Column('col1', Integer, key='column_one')
    )
    s = select([table1])
    s.c.column_one # would be accessible like this
    s.c.col1 # would raise AttributeError

    s = select([table1]).apply_labels()
    s.c.table1_column_one # would raise AttributeError
    s.c.table1_col1 # would be accessible like this

In 0.8, ``key`` is honored in both cases:

::

    # with 0.8
    table1 = Table('t1', metadata,
        Column('col1', Integer, key='column_one')
    )
    s = select([table1])
    s.c.column_one # works
    s.c.col1 # AttributeError

    s = select([table1]).apply_labels()
    s.c.table1_column_one # works
    s.c.table1_col1 # AttributeError

All other behavior regarding "name" and "key" are the same,
including that the rendered SQL will still use the form
``<tablename>_<colname>`` - the emphasis here was on
preventing the ``key`` contents from being rendered into the
``SELECT`` statement so that there are no issues with
special/ non-ascii characters used in the ``key``.

:ticket:`2397`

single_parent warning is now an error
-------------------------------------

A :func:`.relationship` that is many-to-one or many-to-many and
specifies "cascade='all, delete-orphan'", which is an
awkward but nonetheless supported use case (with
restrictions) will now raise an error if the relationship
does not specify the ``single_parent=True`` option.
Previously it would only emit a warning, but a failure would
follow almost immediately within the attribute system in any
case.

:ticket:`2405`

Adding the ``inspector`` argument to the ``column_reflect`` event
-----------------------------------------------------------------

0.7 added a new event called ``column_reflect``, provided so
that the reflection of columns could be augmented as each
one were reflected.   We got this event slightly wrong in
that the event gave no way to get at the current
``Inspector`` and ``Connection`` being used for the
reflection, in the case that additional information from the
database is needed.   As this is a new event not widely used
yet, we'll be adding the ``inspector`` argument into it
directly:

::

    @event.listens_for(Table, "column_reflect")
    def listen_for_col(inspector, table, column_info):
        # ...

:ticket:`2418`

Disabling auto-detect of collations, casing for MySQL
-----------------------------------------------------

The MySQL dialect does two calls, one very expensive, to
load all possible collations from the database as well as
information on casing, the first time an ``Engine``
connects.   Neither of these collections are used for any
SQLAlchemy functions, so these calls will be changed to no
longer be emitted automatically. Applications that might
have relied on these collections being present on
``engine.dialect`` will need to call upon
``_detect_collations()`` and ``_detect_casing()`` directly.

:ticket:`2404`

"Unconsumed column names" warning becomes an exception
------------------------------------------------------

Referring to a non-existent column in an ``insert()`` or
``update()`` construct will raise an error instead of a
warning:

::

    t1 = table('t1', column('x'))
    t1.insert().values(x=5, z=5) # raises "Unconsumed column names: z"

:ticket:`2415`

Inspector.get_primary_keys() is deprecated, use Inspector.get_pk_constraint
---------------------------------------------------------------------------

These two methods on ``Inspector`` were redundant, where
``get_primary_keys()`` would return the same information as
``get_pk_constraint()`` minus the name of the constraint:

::

    >>> insp.get_primary_keys()
    ["a", "b"]

    >>> insp.get_pk_constraint()
    {"name":"pk_constraint", "constrained_columns":["a", "b"]}

:ticket:`2422`

Case-insensitive result row names will be disabled in most cases
----------------------------------------------------------------

A very old behavior, the column names in ``RowProxy`` were
always compared case-insensitively:

::

    >>> row = result.fetchone()
    >>> row['foo'] == row['FOO'] == row['Foo']
    True

This was for the benefit of a few dialects which in the
early days needed this, like Oracle and Firebird, but in
modern usage we have more accurate ways of dealing with the
case-insensitive behavior of these two platforms.

Going forward, this behavior will be available only
optionally, by passing the flag ```case_sensitive=False```
to ```create_engine()```, but otherwise column names
requested from the row must match as far as casing.

:ticket:`2423`

``InstrumentationManager`` and alternate class instrumentation is now an extension
----------------------------------------------------------------------------------

The ``sqlalchemy.orm.interfaces.InstrumentationManager``
class is moved to
``sqlalchemy.ext.instrumentation.InstrumentationManager``.
The "alternate instrumentation" system was built for the
benefit of a very small number of installations that needed
to work with existing or unusual class instrumentation
systems, and generally is very seldom used.   The complexity
of this system has been exported to an ``ext.`` module.  It
remains unused until once imported, typically when a third
party library imports ``InstrumentationManager``, at which
point it is injected back into ``sqlalchemy.orm`` by
replacing the default ``InstrumentationFactory`` with
``ExtendedInstrumentationRegistry``.

Removed
=======

SQLSoup
-------

SQLSoup is a handy package that presents an alternative
interface on top of the SQLAlchemy ORM.   SQLSoup is now
moved into its own project and documented/released
separately; see https://bitbucket.org/zzzeek/sqlsoup.

SQLSoup is a very simple tool that could also benefit from
contributors who are interested in its style of usage.

:ticket:`2262`

MutableType
-----------

The older "mutable" system within the SQLAlchemy ORM has
been removed.   This refers to the ``MutableType`` interface
which was applied to types such as ``PickleType`` and
conditionally to ``TypeDecorator``, and since very early
SQLAlchemy versions has provided a way for the ORM to detect
changes in so-called "mutable" data structures such as JSON
structures and pickled objects.   However, the
implementation was never reasonable and forced a very
inefficient mode of usage on the unit-of-work which caused
an expensive scan of all objects to take place during flush.
In 0.7, the `sqlalchemy.ext.mutable <http://docs.sqlalchemy.
org/en/latest/orm/extensions/mutable.html>`_ extension was
introduced so that user-defined datatypes can appropriately
send events to the unit of work as changes occur.

Today, usage of ``MutableType`` is expected to be low, as
warnings have been in place for some years now regarding its
inefficiency.

:ticket:`2442`

sqlalchemy.exceptions (has been sqlalchemy.exc for years)
---------------------------------------------------------

We had left in an alias ``sqlalchemy.exceptions`` to attempt
to make it slightly easier for some very old libraries that
hadn't yet been upgraded to use ``sqlalchemy.exc``.  Some
users are still being confused by it however so in 0.8 we're
taking it out entirely to eliminate any of that confusion.

:ticket:`2433`

