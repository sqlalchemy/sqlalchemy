==============================
What's New in SQLAlchemy 0.8?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.7,
    undergoing maintenance releases as of October, 2012,
    and SQLAlchemy version 0.8, which is expected for release
    in early 2013.

    Document date: October 25, 2012
    Updated: March 9, 2013

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

New ORM Features
================

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

* Previously difficult custom join conditions, like those involving
  functions and/or CASTing of types, will now function as
  expected in most cases::

    class HostEntry(Base):
        __tablename__ = 'host_entry'

        id = Column(Integer, primary_key=True)
        ip_address = Column(INET)
        content = Column(String(50))

        # relationship() using explicit foreign_keys, remote_side
        parent_host = relationship("HostEntry",
                            primaryjoin=ip_address == cast(content, INET),
                            foreign_keys=content,
                            remote_side=ip_address
                        )

  The new :func:`.relationship` mechanics make use of a
  SQLAlchemy concept known as :term:`annotations`.  These annotations
  are also available to application code explicitly via
  the :func:`.foreign` and :func:`.remote` functions, either
  as a means to improve readability for advanced configurations
  or to directly inject an exact configuration, bypassing
  the usual join-inspection heuristics::

    from sqlalchemy.orm import foreign, remote

    class HostEntry(Base):
        __tablename__ = 'host_entry'

        id = Column(Integer, primary_key=True)
        ip_address = Column(INET)
        content = Column(String(50))

        # relationship() using explicit foreign() and remote() annotations
        # in lieu of separate arguments
        parent_host = relationship("HostEntry",
                            primaryjoin=remote(ip_address) == \
                                    cast(foreign(content), INET),
                        )


.. seealso::

    :ref:`relationship_configure_joins` - a newly revised section on :func:`.relationship`
    detailing the latest techniques for customizing related attributes and collection
    access.

:ticket:`1401` :ticket:`610`

.. _feature_orminspection_08:

New Class/Object Inspection System
----------------------------------

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

0.8 now provides a consistent, stable and fully
documented API for this purpose, including an inspection
system which works on mapped classes, instances, attributes,
and other Core and ORM constructs.  The entrypoint to this
system is the core-level :func:`.inspect` function.
In most cases, the object being inspected
is one already part of SQLAlchemy's system,
such as :class:`.Mapper`, :class:`.InstanceState`,
:class:`.Inspector`.  In some cases, new objects have been
added with the job of providing the inspection API in
certain contexts, such as :class:`.AliasedInsp` and
:class:`.AttributeState`.

A walkthrough of some key capabilities follows::

    >>> class User(Base):
    ...     __tablename__ = 'user'
    ...     id = Column(Integer, primary_key=True)
    ...     name = Column(String)
    ...     name_syn = synonym(name)
    ...     addresses = relationship("Address")
    ...

    >>> # universal entry point is inspect()
    >>> b = inspect(User)

    >>> # b in this case is the Mapper
    >>> b
    <Mapper at 0x101521950; User>

    >>> # Column namespace
    >>> b.columns.id
    Column('id', Integer(), table=<user>, primary_key=True, nullable=False)

    >>> # mapper's perspective of the primary key
    >>> b.primary_key
    (Column('id', Integer(), table=<user>, primary_key=True, nullable=False),)

    >>> # MapperProperties available from .attrs
    >>> b.attrs.keys()
    ['name_syn', 'addresses', 'id', 'name']

    >>> # .column_attrs, .relationships, etc. filter this collection
    >>> b.column_attrs.keys()
    ['id', 'name']

    >>> list(b.relationships)
    [<sqlalchemy.orm.properties.RelationshipProperty object at 0x1015212d0>]

    >>> # they are also namespaces
    >>> b.column_attrs.id
    <sqlalchemy.orm.properties.ColumnProperty object at 0x101525090>

    >>> b.relationships.addresses
    <sqlalchemy.orm.properties.RelationshipProperty object at 0x1015212d0>

    >>> # point inspect() at a mapped, class level attribute,
    >>> # returns the attribute itself
    >>> b = inspect(User.addresses)
    >>> b
    <sqlalchemy.orm.attributes.InstrumentedAttribute object at 0x101521fd0>

    >>> # From here we can get the mapper:
    >>> b.mapper
    <Mapper at 0x101525810; Address>

    >>> # the parent inspector, in this case a mapper
    >>> b.parent
    <Mapper at 0x101521950; User>

    >>> # an expression
    >>> print(b.expression)
    "user".id = address.user_id

    >>> # inspect works on instances
    >>> u1 = User(id=3, name='x')
    >>> b = inspect(u1)

    >>> # it returns the InstanceState
    >>> b
    <sqlalchemy.orm.state.InstanceState object at 0x10152bed0>

    >>> # similar attrs accessor refers to the
    >>> b.attrs.keys()
    ['id', 'name_syn', 'addresses', 'name']

    >>> # attribute interface - from attrs, you get a state object
    >>> b.attrs.id
    <sqlalchemy.orm.state.AttributeState object at 0x10152bf90>

    >>> # this object can give you, current value...
    >>> b.attrs.id.value
    3

    >>> # ... current history
    >>> b.attrs.id.history
    History(added=[3], unchanged=(), deleted=())

    >>> # InstanceState can also provide session state information
    >>> # lets assume the object is persistent
    >>> s = Session()
    >>> s.add(u1)
    >>> s.commit()

    >>> # now we can get primary key identity, always
    >>> # works in query.get()
    >>> b.identity
    (3,)

    >>> # the mapper level key
    >>> b.identity_key
    (<class '__main__.User'>, (3,))

    >>> # state within the session
    >>> b.persistent, b.transient, b.deleted, b.detached
    (True, False, False, False)

    >>> # owning session
    >>> b.session
    <sqlalchemy.orm.session.Session object at 0x101701150>

.. seealso::

    :ref:`core_inspection_toplevel`

:ticket:`2208`

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

The :meth:`.PropComparator.of_type` method is used to specify
a specific subtype to use when constructing SQL expressions along
a :func:`.relationship` that has a :term:`polymorphic` mapping as its target.
This method can now be used to target *any number* of target subtypes,
by combining it with the new :func:`.with_polymorphic` function::

    # use eager loading in conjunction with with_polymorphic targets
    Job_P = with_polymorphic(Job, [SubJob, ExtraJob], aliased=True)
    q = s.query(DataContainer).\
                join(DataContainer.jobs.of_type(Job_P)).\
                    options(contains_eager(DataContainer.jobs.of_type(Job_P)))

The method now works equally well in most places a regular relationship
attribute is accepted, including with loader functions like
:func:`.joinedload`, :func:`.subqueryload`, :func:`.contains_eager`,
and comparison methods like :meth:`.PropComparator.any`
and :meth:`.PropComparator.has`::

    # use eager loading in conjunction with with_polymorphic targets
    Job_P = with_polymorphic(Job, [SubJob, ExtraJob], aliased=True)
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
                            any(and_(Job_A.id < Job.id, Job_A.type=='fred')
                        )
                    )

.. seealso::

    :ref:`of_type`

:ticket:`2438` :ticket:`1106`

Events Can Be Applied to Unmapped Superclasses
----------------------------------------------

Mapper and instance events can now be associated with an unmapped
superclass, where those events will be propagated to subclasses
as those subclasses are mapped.   The ``propagate=True`` flag
should be used.  This feature allows events to be associated
with a declarative base class::

    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    @event.listens_for("load", Base, propagate=True)
    def on_load(target, context):
        print("New instance loaded:", target)

    # on_load() will be applied to SomeClass
    class SomeClass(Base):
        __tablename__ = 'sometable'

        # ...

:ticket:`2585`

Declarative Distinguishes Between Modules/Packages
--------------------------------------------------

A key feature of Declarative is the ability to refer
to other mapped classes using their string name.   The
registry of class names is now sensitive to the owning
module and package of a given class.   The classes
can be referred to via dotted name in expressions::

    class Snack(Base):
        # ...

        peanuts = relationship("nuts.Peanut",
                primaryjoin="nuts.Peanut.snack_id == Snack.id")

The resolution allows that any full or partial
disambiguating package name can be used.   If the
path to a particular class is still ambiguous,
an error is raised.

:ticket:`2338`


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

.. seealso::

    :class:`.DeferredReflection`

:ticket:`2485`

ORM Classes Now Accepted by Core Constructs
-------------------------------------------

While the SQL expressions used with :meth:`.Query.filter`,
such as ``User.id == 5``, have always been compatible for
use with core constructs such as :func:`.select`, the mapped
class itself would not be recognized when passed to :func:`.select`,
:meth:`.Select.select_from`, or :meth:`.Select.correlate`.
A new SQL registration system allows a mapped class to be
accepted as a FROM clause within the core::

    from sqlalchemy import select

    stmt = select([User]).where(User.id == 5)

Above, the mapped ``User`` class will expand into
the :class:`.Table` to which ``User`` is mapped.

:ticket:`2245`

Query.update() supports UPDATE..FROM
-------------------------------------

The new UPDATE..FROM mechanics work in query.update().
Below, we emit an UPDATE against ``SomeEntity``, adding
a FROM clause (or equivalent, depending on backend)
against ``SomeOtherEntity``::

    query(SomeEntity).\
        filter(SomeEntity.id==SomeOtherEntity.id).\
        filter(SomeOtherEntity.foo=='bar').\
        update({"data":"x"})

In particular, updates to joined-inheritance
entities are supported, provided the target of the UPDATE is local to the
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

Caching Example now uses dogpile.cache
---------------------------------------

The caching example now uses `dogpile.cache <http://dogpilecache.readthedocs.org/>`_.
Dogpile.cache is a rewrite of the caching portion
of Beaker, featuring vastly simpler and faster operation,
as well as support for distributed locking.

Note that the SQLAlchemy APIs used by the Dogpile example as well
as the previous Beaker example have changed slightly, in particular
this change is needed as illustrated in the Beaker example::

    --- examples/beaker_caching/caching_query.py
    +++ examples/beaker_caching/caching_query.py
    @@ -222,7 +222,8 @@

             """
             if query._current_path:
    -            mapper, key = query._current_path[-2:]
    +            mapper, prop = query._current_path[-2:]
    +            key = prop.key

                 for cls in mapper.class_.__mro__:
                     if (cls, key) in self._relationship_options:

.. seealso::

    :mod:`dogpile_caching`

:ticket:`2589`

New Core Features
==================

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
:class:`.ColumnElement` type so that it consults its
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
    print(conn.execute(stmt).fetchall())


New features which have come from this immediately include
support for Postgresql's HSTORE type, as well as new
operations associated with Postgresql's ARRAY
type.    It also paves the way for existing types to acquire
lots more operators that are specific to those types, such
as more string, integer and date operators.

.. seealso::

    :ref:`types_operators`

    :class:`.HSTORE`

:ticket:`2547`

.. _feature_2623:

Multiple-VALUES support for Insert
-----------------------------------

The :meth:`.Insert.values` method now supports a list of dictionaries,
which will render a multi-VALUES statement such as
``VALUES (<row1>), (<row2>), ...``.  This is only relevant to backends which
support this syntax, including Postgresql, SQLite, and MySQL.  It is
not the same thing as the usual ``executemany()`` style of INSERT which
remains unchanged::

    users.insert().values([
                        {"name": "some name"},
                        {"name": "some other name"},
                        {"name": "yet another name"},
                    ])

.. seealso::

    :meth:`.Insert.values`

:ticket:`2623`

Type Expressions
-----------------

SQL expressions can now be associated with types.  Historically,
:class:`.TypeEngine` has always allowed Python-side functions which
receive both bound parameters as well as result row values, passing
them through a Python side conversion function on the way to/back from
the database.   The new feature allows similar
functionality, except on the database side::

    from sqlalchemy.types import String
    from sqlalchemy import func, Table, Column, MetaData

    class LowerString(String):
        def bind_expression(self, bindvalue):
            return func.lower(bindvalue)

        def column_expression(self, col):
            return func.lower(col)

    metadata = MetaData()
    test_table = Table(
            'test_table',
            metadata,
            Column('data', LowerString)
    )

Above, the ``LowerString`` type defines a SQL expression that will be emitted
whenever the ``test_table.c.data`` column is rendered in the columns
clause of a SELECT statement::

    >>> print(select([test_table]).where(test_table.c.data == 'HI'))
    SELECT lower(test_table.data) AS data
    FROM test_table
    WHERE test_table.data = lower(:data_1)

This feature is also used heavily by the new release of GeoAlchemy,
to embed PostGIS expressions inline in SQL based on type rules.

.. seealso::

    :ref:`types_sql_value_processing`

:ticket:`1534`

Core Inspection System
-----------------------

The :func:`.inspect` function introduced in :ref:`feature_orminspection_08`
also applies to the core.  Applied to an :class:`.Engine` it produces
an :class:`.Inspector` object::

    from sqlalchemy import inspect
    from sqlalchemy import create_engine

    engine = create_engine("postgresql://scott:tiger@localhost/test")
    insp = inspect(engine)
    print(insp.get_table_names())

It can also be applied to any :class:`.ClauseElement`, which returns
the :class:`.ClauseElement` itself, such as :class:`.Table`, :class:`.Column`,
:class:`.Select`, etc.   This allows it to work fluently between Core
and ORM constructs.


New Method :meth:`.Select.correlate_except`
-------------------------------------------

:func:`.select` now has a method :meth:`.Select.correlate_except`
which specifies "correlate on all FROM clauses except those
specified".  It can be used for mapping scenarios where
a related subquery should correlate normally, except
against a particular target selectable::

    class SnortEvent(Base):
        __tablename__ = "event"

        id = Column(Integer, primary_key=True)
        signature = Column(Integer, ForeignKey("signature.id"))

        signatures = relationship("Signature", lazy=False)

    class Signature(Base):
        __tablename__ = "signature"

        id = Column(Integer, primary_key=True)

        sig_count = column_property(
                        select([func.count('*')]).\
                            where(SnortEvent.signature == id).
                            correlate_except(SnortEvent)
                    )

.. seealso::

    :meth:`.Select.correlate_except`

Postgresql HSTORE type
----------------------

Support for Postgresql's ``HSTORE`` type is now available as
:class:`.postgresql.HSTORE`.   This type makes great usage
of the new operator system to provide a full range of operators
for HSTORE types, including index access, concatenation,
and containment methods such as
:meth:`~.HSTORE.comparator_factory.has_key`,
:meth:`~.HSTORE.comparator_factory.has_any`, and
:meth:`~.HSTORE.comparator_factory.matrix`::

    from sqlalchemy.dialects.postgresql import HSTORE

    data = Table('data_table', metadata,
            Column('id', Integer, primary_key=True),
            Column('hstore_data', HSTORE)
        )

    engine.execute(
        select([data.c.hstore_data['some_key']])
    ).scalar()

    engine.execute(
        select([data.c.hstore_data.matrix()])
    ).scalar()


.. seealso::

    :class:`.postgresql.HSTORE`

    :class:`.postgresql.hstore`

:ticket:`2606`

Enhanced Postgresql ARRAY type
------------------------------

The :class:`.postgresql.ARRAY` type will accept an optional
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

The type also introduces new operators, using the new type-specific
operator framework.  New operations include indexed access::

    result = conn.execute(
        select([mytable.c.arraycol[2]])
    )

slice access in SELECT::

    result = conn.execute(
        select([mytable.c.arraycol[2:4]])
    )

slice updates in UPDATE::

    conn.execute(
        mytable.update().values({mytable.c.arraycol[2:3]: [7, 8]})
    )

freestanding array literals::

    >>> from sqlalchemy.dialects import postgresql
    >>> conn.scalar(
    ...    select([
    ...        postgresql.array([1, 2]) + postgresql.array([3, 4, 5])
    ...    ])
    ...  )
    [1, 2, 3, 4, 5]

array concatenation, where below, the right side ``[4, 5, 6]`` is coerced into an array literal::

    select([mytable.c.arraycol + [4, 5, 6]])

.. seealso::

    :class:`.postgresql.ARRAY`

    :class:`.postgresql.array`

:ticket:`2441`

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

Huge thanks to Nate Dub for the sprinting on this at Pycon 2012.

.. seealso::

    :class:`.sqlite.DATETIME`

    :class:`.sqlite.DATE`

    :class:`.sqlite.TIME`

:ticket:`2363`

"COLLATE" supported across all dialects; in particular MySQL, Postgresql, SQLite
--------------------------------------------------------------------------------

The "collate" keyword, long accepted by the MySQL dialect, is now established
on all :class:`.String` types and will render on any backend, including
when features such as :meth:`.MetaData.create_all` and :func:`.cast` is used::

    >>> stmt = select([cast(sometable.c.somechar, String(20, collation='utf8'))])
    >>> print(stmt)
    SELECT CAST(sometable.somechar AS VARCHAR(20) COLLATE "utf8") AS anon_1
    FROM sometable

.. seealso::

    :class:`.String`

:ticket:`2276`

"Prefixes" now supported for :func:`.update`, :func:`.delete`
-------------------------------------------------------------

Geared towards MySQL, a "prefix" can be rendered within any of
these constructs.   E.g.::

    stmt = table.delete().prefix_with("LOW_PRIORITY", dialect="mysql")


    stmt = table.update().prefix_with("LOW_PRIORITY", dialect="mysql")

The method is new in addition to those which already existed
on :func:`.insert`, :func:`.select` and :class:`.Query`.

.. seealso::

    :meth:`.Update.prefix_with`

    :meth:`.Delete.prefix_with`

    :meth:`.Insert.prefix_with`

    :meth:`.Select.prefix_with`

    :meth:`.Query.prefix_with`

:ticket:`2431`


Behavioral Changes
==================

.. _legacy_is_orphan_addition:

The consideration of a "pending" object as an "orphan" has been made more aggressive
------------------------------------------------------------------------------------

This is a late add to the 0.8 series, however it is hoped that the new behavior
is generally more consistent and intuitive in a wider variety of
situations.   The ORM has since at least version 0.4 included behavior
such that an object that's "pending", meaning that it's
associated with a :class:`.Session` but hasn't been inserted into the database
yet, is automatically expunged from the :class:`.Session` when it becomes an "orphan",
which means it has been de-associated with a parent object that refers to it
with ``delete-orphan`` cascade on the configured :func:`.relationship`.   This
behavior is intended to approximately mirror the behavior of a persistent
(that is, already inserted) object, where the ORM will emit a DELETE for such
objects that become orphans based on the interception of detachment events.

The behavioral change comes into play for objects that
are referred to by multiple kinds of parents that each specify ``delete-orphan``; the
typical example is an :ref:`association object <association_pattern>` that bridges two other kinds of objects
in a many-to-many pattern.   Previously, the behavior was such that the
pending object would be expunged only when de-associated with *all* of its parents.
With the behavioral change, the pending object
is expunged as soon as it is de-associated from *any* of the parents that it was
previously associated with.  This behavior is intended to more closely
match that of persistent objects, which are deleted as soon
as they are de-associated from any parent.

The rationale for the older behavior dates back
at least to version 0.4, and was basically a defensive decision to try to alleviate
confusion when an object was still being constructed for INSERT.   But the reality
is that the object is re-associated with the :class:`.Session` as soon as it is
attached to any new parent in any case.

It's still possible to flush an object
that is not associated with all of its required parents, if the object was either
not associated with those parents in the first place, or if it was expunged, but then
re-associated with a :class:`.Session` via a subsequent attachment event but still
not fully associated.   In this situation, it is expected that the database
would emit an integrity error, as there are likely NOT NULL foreign key columns
that are unpopulated.   The ORM makes the decision to let these INSERT attempts
occur, based on the judgment that an object that is only partially associated with
its required parents but has been actively associated with some of them,
is more often than not a user error, rather than an intentional
omission which should be silently skipped - silently skipping the INSERT here would
make user errors of this nature very hard to debug.

The old behavior, for applications that might have been relying upon it, can be re-enabled for
any :class:`.Mapper` by specifying the flag ``legacy_is_orphan`` as a mapper
option.

The new behavior allows the following test case to work::

    from sqlalchemy import Column, Integer, String, ForeignKey
    from sqlalchemy.orm import relationship, backref
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'
        id = Column(Integer, primary_key=True)
        name = Column(String(64))

    class UserKeyword(Base):
        __tablename__ = 'user_keyword'
        user_id = Column(Integer, ForeignKey('user.id'), primary_key=True)
        keyword_id = Column(Integer, ForeignKey('keyword.id'), primary_key=True)

        user = relationship(User,
                    backref=backref("user_keywords",
                                    cascade="all, delete-orphan")
                )

        keyword = relationship("Keyword",
                    backref=backref("user_keywords",
                                    cascade="all, delete-orphan")
                )

        # uncomment this to enable the old behavior
        # __mapper_args__ = {"legacy_is_orphan": True}

    class Keyword(Base):
        __tablename__ = 'keyword'
        id = Column(Integer, primary_key=True)
        keyword = Column('keyword', String(64))

    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session

    # note we're using Postgresql to ensure that referential integrity
    # is enforced, for demonstration purposes.
    e = create_engine("postgresql://scott:tiger@localhost/test", echo=True)

    Base.metadata.drop_all(e)
    Base.metadata.create_all(e)

    session = Session(e)

    u1 = User(name="u1")
    k1 = Keyword(keyword="k1")

    session.add_all([u1, k1])

    uk1 = UserKeyword(keyword=k1, user=u1)

    # previously, if session.flush() were called here,
    # this operation would succeed, but if session.flush()
    # were not called here, the operation fails with an
    # integrity error.
    # session.flush()
    del u1.user_keywords[0]

    session.commit()


:ticket:`2655`

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

Previously it was necessary to call :meth:`.Query.correlate` in
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

.. _correlation_context_specific:

Correlation is now always context-specific
------------------------------------------

To allow a wider variety of correlation scenarios, the behavior of
:meth:`.Select.correlate` and :meth:`.Query.correlate` has changed slightly
such that the SELECT statement will omit the "correlated" target from the
FROM clause only if the statement is actually used in that context.  Additionally,
it's no longer possible for a SELECT statement that's placed as a FROM
in an enclosing SELECT statement to "correlate" (i.e. omit) a FROM clause.

This change only makes things better as far as rendering SQL, in that it's no
longer possible to render illegal SQL where there are insufficient FROM
objects relative to what's being selected::

    from sqlalchemy.sql import table, column, select

    t1 = table('t1', column('x'))
    t2 = table('t2', column('y'))
    s = select([t1, t2]).correlate(t1)

    print(s)

Prior to this change, the above would return::

    SELECT t1.x, t2.y FROM t2

which is invalid SQL as "t1" is not referred to in any FROM clause.

Now, in the absence of an enclosing SELECT, it returns::

    SELECT t1.x, t2.y FROM t1, t2

Within a SELECT, the correlation takes effect as expected::

    s2 = select([t1, t2]).where(t1.c.x == t2.c.y).where(t1.c.x == s)

    print(s2)

    SELECT t1.x, t2.y FROM t1, t2
    WHERE t1.x = t2.y AND t1.x =
        (SELECT t1.x, t2.y FROM t2)

This change is not expected to impact any existing applications, as
the correlation behavior remains identical for properly constructed
expressions.  Only an application that relies, most likely within a
testing scenario, on the invalid string output of a correlated
SELECT used in a non-correlating context would see any change.

:ticket:`2668`


.. _metadata_create_drop_tables:

create_all() and drop_all() will now honor an empty list as such
----------------------------------------------------------------

The methods :meth:`.MetaData.create_all` and :meth:`.MetaData.drop_all`
will now accept a list of :class:`.Table` objects that is empty,
and will not emit any CREATE or DROP statements.  Previously,
an empty list was interepreted the same as passing ``None``
for a collection, and CREATE/DROP would be emitted for all
items unconditionally.

This is a bug fix but some applications may have been relying upon
the previous behavior.

:ticket:`2664`

Repaired the Event Targeting of :class:`.InstrumentationEvents`
----------------------------------------------------------------

The :class:`.InstrumentationEvents` series of event targets have
documented that the events will only be fired off according to
the actual class passed as a target.  Through 0.7, this wasn't the
case, and any event listener applied to :class:`.InstrumentationEvents`
would be invoked for all classes mapped.  In 0.8, additional
logic has been added so that the events will only invoke for those
classes sent in.  The ``propagate`` flag here is set to ``True``
by default as class instrumentation events are typically used to
intercept classes that aren't yet created.

:ticket:`2590`

No more magic coercion of "=" to IN when comparing to subquery in MS-SQL
------------------------------------------------------------------------

We found a very old behavior in the MSSQL dialect which
would attempt to rescue users from themselves when
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

Fixed the behavior of :meth:`.Session.is_modified`
--------------------------------------------------

The :meth:`.Session.is_modified` method accepts an argument
``passive`` which basically should not be necessary, the
argument in all cases should be the value ``True`` - when
left at its default of ``False`` it would have the effect of
hitting the database, and often triggering autoflush which
would itself change the results.   In 0.8 the ``passive``
argument will have no effect, and unloaded attributes will
never be checked for history since by definition there can
be no pending state change on an unloaded attribute.

.. seealso::

    :meth:`.Session.is_modified`

:ticket:`2320`

:attr:`.Column.key` is honored in the :attr:`.Select.c` attribute of :func:`.select` with :meth:`.Select.apply_labels`
-----------------------------------------------------------------------------------------------------------------------

Users of the expression system know that :meth:`.Select.apply_labels`
prepends the table name to each column name, affecting the
names that are available from :attr:`.Select.c`:

::

    s = select([table1]).apply_labels()
    s.c.table1_col1
    s.c.table1_col2

Before 0.8, if the :class:`.Column` had a different :attr:`.Column.key`, this
key would be ignored, inconsistently versus when
:meth:`.Select.apply_labels` were not used:

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

In 0.8, :attr:`.Column.key` is honored in both cases:

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
preventing the :attr:`.Column.key` contents from being rendered into the
``SELECT`` statement so that there are no issues with
special/ non-ascii characters used in the :attr:`.Column.key`.

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

