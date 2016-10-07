==============================
What's New in SQLAlchemy 1.0?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.9,
    undergoing maintenance releases as of May, 2014,
    and SQLAlchemy version 1.0, released in April, 2015.

    Document last updated: June 9, 2015

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.0,
and also documents changes which affect users migrating
their applications from the 0.9 series of SQLAlchemy to 1.0.

Please carefully review the sections on behavioral changes for
potentially backwards-incompatible changes in behavior.


New Features and Improvements - ORM
===================================

New Session Bulk INSERT/UPDATE API
----------------------------------

A new series of :class:`.Session` methods which provide hooks directly
into the unit of work's facility for emitting INSERT and UPDATE
statements has been created.  When used correctly, this expert-oriented system
can allow ORM-mappings to be used to generate bulk insert and update
statements batched into executemany groups, allowing the statements
to proceed at speeds that rival direct use of the Core.

.. seealso::

    :ref:`bulk_operations` - introduction and full documentation

:ticket:`3100`

New Performance Example Suite
------------------------------

Inspired by the benchmarking done for the :ref:`bulk_operations` feature
as well as for the :ref:`faq_how_to_profile` section of the FAQ, a new
example section has been added which features several scripts designed
to illustrate the relative performance profile of various Core and ORM
techniques.  The scripts are organized into use cases, and are packaged
under a single console interface such that any combination of demonstrations
can be run, dumping out timings, Python profile results and/or RunSnake profile
displays.

.. seealso::

    :ref:`examples_performance`

"Baked" Queries
---------------

The "baked" query feature is an unusual new approach which allows for
straightforward construction an invocation of :class:`.Query` objects
using caching, which upon successive calls features vastly reduced
Python function call overhead (over 75%).    By  specifying a
:class:`.Query` object as a series of lambdas which are only invoked
once, a query as a pre-compiled unit begins to be feasible::

    from sqlalchemy.ext import baked
    from sqlalchemy import bindparam

    bakery = baked.bakery()

    def search_for_user(session, username, email=None):

        baked_query = bakery(lambda session: session.query(User))
        baked_query += lambda q: q.filter(User.name == bindparam('username'))

        baked_query += lambda q: q.order_by(User.id)

        if email:
            baked_query += lambda q: q.filter(User.email == bindparam('email'))

        result = baked_query(session).params(username=username, email=email).all()

        return result

.. seealso::

    :ref:`baked_toplevel`

:ticket:`3054`

.. _feature_3150:

Improvements to declarative mixins, ``@declared_attr`` and related features
----------------------------------------------------------------------------

The declarative system in conjunction with :class:`.declared_attr` has been
overhauled to support new capabilities.

A function decorated with :class:`.declared_attr` is now called only **after**
any mixin-based column copies are generated.  This means the function can
call upon mixin-established columns and will receive a reference to the correct
:class:`.Column` object::

    class HasFooBar(object):
        foobar = Column(Integer)

        @declared_attr
        def foobar_prop(cls):
            return column_property('foobar: ' + cls.foobar)

    class SomeClass(HasFooBar, Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)

Above, ``SomeClass.foobar_prop`` will be invoked against ``SomeClass``,
and ``SomeClass.foobar`` will be the final :class:`.Column` object that is
to be mapped to ``SomeClass``, as opposed to the non-copied object present
directly on ``HasFooBar``, even though the columns aren't mapped yet.

The :class:`.declared_attr` function now **memoizes** the value
that's returned on a per-class basis, so that repeated calls to the same
attribute will return the same value.  We can alter the example to illustrate
this::

    class HasFooBar(object):
        @declared_attr
        def foobar(cls):
            return Column(Integer)

        @declared_attr
        def foobar_prop(cls):
            return column_property('foobar: ' + cls.foobar)

    class SomeClass(HasFooBar, Base):
        __tablename__ = 'some_table'
        id = Column(Integer, primary_key=True)

Previously, ``SomeClass`` would be mapped with one particular copy of
the ``foobar`` column, but the ``foobar_prop`` by calling upon ``foobar``
a second time would produce a different column.   The value of
``SomeClass.foobar`` is now memoized during declarative setup time, so that
even before the attribute is mapped by the mapper, the interim column
value will remain consistent no matter how many times the
:class:`.declared_attr` is called upon.

The two behaviors above should help considerably with declarative definition
of many types of mapper properties that derive from other attributes, where
the :class:`.declared_attr` function is called upon from other
:class:`.declared_attr` functions locally present before the class is
actually mapped.

For a pretty slim edge case where one wishes to build a declarative mixin
that establishes distinct columns per subclass, a new modifier
:attr:`.declared_attr.cascading` is added.  With this modifier, the
decorated function will be invoked individually for each class in the
mapped inheritance hierarchy.  While this is already the behavior for
special attributes such as ``__table_args__`` and ``__mapper_args__``,
for columns and other properties the behavior by default assumes that attribute
is affixed to the base class only, and just inherited from subclasses.
With :attr:`.declared_attr.cascading`, individual behaviors can be
applied::

    class HasIdMixin(object):
        @declared_attr.cascading
        def id(cls):
            if has_inherited_table(cls):
                return Column(ForeignKey('myclass.id'), primary_key=True)
            else:
                return Column(Integer, primary_key=True)

    class MyClass(HasIdMixin, Base):
        __tablename__ = 'myclass'
        # ...

    class MySubClass(MyClass):
        ""
        # ...

.. seealso::

    :ref:`mixin_inheritance_columns`

Finally, the :class:`.AbstractConcreteBase` class has been reworked
so that a relationship or other mapper property can be set up inline
on the abstract base::

    from sqlalchemy import Column, Integer, ForeignKey
    from sqlalchemy.orm import relationship
    from sqlalchemy.ext.declarative import (declarative_base, declared_attr,
        AbstractConcreteBase)

    Base = declarative_base()

    class Something(Base):
        __tablename__ = u'something'
        id = Column(Integer, primary_key=True)


    class Abstract(AbstractConcreteBase, Base):
        id = Column(Integer, primary_key=True)

        @declared_attr
        def something_id(cls):
            return Column(ForeignKey(Something.id))

        @declared_attr
        def something(cls):
            return relationship(Something)


    class Concrete(Abstract):
        __tablename__ = u'cca'
        __mapper_args__ = {'polymorphic_identity': 'cca', 'concrete': True}


The above mapping will set up a table ``cca`` with both an ``id`` and
a ``something_id`` column, and ``Concrete`` will also have a relationship
``something``.  The new feature is that ``Abstract`` will also have an
independently configured relationship ``something`` that builds against
the polymorphic union of the base.

:ticket:`3150` :ticket:`2670` :ticket:`3149` :ticket:`2952` :ticket:`3050`

ORM full object fetches 25% faster
----------------------------------

The mechanics of the ``loading.py`` module as well as the identity map
have undergone several passes of inlining, refactoring, and pruning, so
that a raw load of rows now populates ORM-based objects around 25% faster.
Assuming a 1M row table, a script like the following illustrates the type
of load that's improved the most::

    import time
    from sqlalchemy import Integer, Column, create_engine, Table
    from sqlalchemy.orm import Session
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Foo(Base):
        __table__ = Table(
            'foo', Base.metadata,
            Column('id', Integer, primary_key=True),
            Column('a', Integer(), nullable=False),
            Column('b', Integer(), nullable=False),
            Column('c', Integer(), nullable=False),
        )

    engine = create_engine(
        'mysql+mysqldb://scott:tiger@localhost/test', echo=True)

    sess = Session(engine)

    now = time.time()

    # avoid using all() so that we don't have the overhead of building
    # a large list of full objects in memory
    for obj in sess.query(Foo).yield_per(100).limit(1000000):
        pass

    print("Total time: %d" % (time.time() - now))

Local MacBookPro results bench from 19 seconds for 0.9 down to 14 seconds for
1.0.  The :meth:`.Query.yield_per` call is always a good idea when batching
huge numbers of rows, as it prevents the Python interpreter from having
to allocate a huge amount of memory for all objects and their instrumentation
at once.  Without the :meth:`.Query.yield_per`, the above script on the
MacBookPro is 31 seconds on 0.9 and 26 seconds on 1.0, the extra time spent
setting up very large memory buffers.

.. _feature_3176:

New KeyedTuple implementation dramatically faster
-------------------------------------------------

We took a look into the :class:`.KeyedTuple` implementation in the hopes
of improving queries like this::

    rows = sess.query(Foo.a, Foo.b, Foo.c).all()

The :class:`.KeyedTuple` class is used rather than Python's
``collections.namedtuple()``, because the latter has a very complex
type-creation routine that benchmarks much slower than :class:`.KeyedTuple`.
However, when fetching hundreds of thousands of rows,
``collections.namedtuple()`` quickly overtakes :class:`.KeyedTuple` which
becomes dramatically slower as instance invocation goes up.   What to do?
A new type that hedges between the approaches of both.   Benching
all three types for "size" (number of rows returned) and "num"
(number of distinct queries), the new "lightweight keyed tuple" either
outperforms both, or lags very slightly behind the faster object, based on
which scenario.  In the "sweet spot", where we are both creating a good number
of new types as well as fetching a good number of rows, the lightweight
object totally smokes both namedtuple and KeyedTuple::

    -----------------
    size=10 num=10000                 # few rows, lots of queries
    namedtuple: 3.60302400589         # namedtuple falls over
    keyedtuple: 0.255059957504        # KeyedTuple very fast
    lw keyed tuple: 0.582715034485    # lw keyed trails right on KeyedTuple
    -----------------
    size=100 num=1000                 # <--- sweet spot
    namedtuple: 0.365247011185
    keyedtuple: 0.24896979332
    lw keyed tuple: 0.0889317989349   # lw keyed blows both away!
    -----------------
    size=10000 num=100
    namedtuple: 0.572599887848
    keyedtuple: 2.54251694679
    lw keyed tuple: 0.613876104355
    -----------------
    size=1000000 num=10               # few queries, lots of rows
    namedtuple: 5.79669594765         # namedtuple very fast
    keyedtuple: 28.856498003          # KeyedTuple falls over
    lw keyed tuple: 6.74346804619     # lw keyed trails right on namedtuple


:ticket:`3176`

.. _feature_slots:

Significant Improvements in Structural Memory Use
--------------------------------------------------

Structural memory use has been improved via much more significant use
of ``__slots__`` for many internal objects.  This optimization is
particularly geared towards the base memory size of large applications
that have lots of tables and columns, and reduces memory
size for a variety of high-volume objects including event listening
internals, comparator objects and parts of the ORM attribute and
loader strategy system.

A bench that makes use of heapy measure the startup size of Nova
illustrates a difference of about 3.7 fewer megs, or 46%,
taken up by SQLAlchemy's objects, associated dictionaries, as
well as weakrefs, within a basic import of "nova.db.sqlalchemy.models"::

    # reported by heapy, summation of SQLAlchemy objects +
    # associated dicts + weakref-related objects with core of Nova imported:

        Before: total count 26477 total bytes 7975712
        After: total count 18181 total bytes 4236456

    # reported for the Python module space overall with the
    # core of Nova imported:

        Before: Partition of a set of 355558 objects. Total size = 61661760 bytes.
        After: Partition of a set of 346034 objects. Total size = 57808016 bytes.


.. _feature_updatemany:

UPDATE statements are now batched with executemany() in a flush
----------------------------------------------------------------

UPDATE statements can now be batched within an ORM flush
into more performant executemany() call, similarly to how INSERT
statements can be batched; this will be invoked within flush
based on the following criteria:

* two or more UPDATE statements in sequence involve the identical set of
  columns to be modified.

* The statement has no embedded SQL expressions in the SET clause.

* The mapping does not use a :paramref:`~.orm.mapper.version_id_col`, or
  the backend dialect supports a "sane" rowcount for an executemany()
  operation; most DBAPIs support this correctly now.

.. _feature_3178:


.. _bug_3035:

Session.get_bind() handles a wider variety of inheritance scenarios
-------------------------------------------------------------------

The :meth:`.Session.get_bind` method is invoked whenever a query or unit
of work flush process seeks to locate the database engine that corresponds
to a particular class.   The method has been improved to handle a variety
of inheritance-oriented scenarios, including:

* Binding to a Mixin or Abstract Class::

        class MyClass(SomeMixin, Base):
            __tablename__ = 'my_table'
            # ...

        session = Session(binds={SomeMixin: some_engine})


* Binding to inherited concrete subclasses individually based on table::

        class BaseClass(Base):
            __tablename__ = 'base'

            # ...

        class ConcreteSubClass(BaseClass):
            __tablename__ = 'concrete'

            # ...

            __mapper_args__ = {'concrete': True}


        session = Session(binds={
            base_table: some_engine,
            concrete_table: some_other_engine
        })


:ticket:`3035`


.. _bug_3227:

Session.get_bind() will receive the Mapper in all relevant Query cases
-----------------------------------------------------------------------

A series of issues were repaired where the :meth:`.Session.get_bind`
would not receive the primary :class:`.Mapper` of the :class:`.Query`,
even though this mapper was readily available (the primary mapper is the
single mapper, or alternatively the first mapper, that is associated with
a :class:`.Query` object).

The :class:`.Mapper` object, when passed to :meth:`.Session.get_bind`,
is typically used by sessions that make use of the
:paramref:`.Session.binds` parameter to associate mappers with a
series of engines (although in this use case, things frequently
"worked" in most cases anyway as the bind would be located via the
mapped table object), or more specifically implement a user-defined
:meth:`.Session.get_bind` method that provies some pattern of
selecting engines based on mappers, such as horizontal sharding or a
so-called "routing" session that routes queries to different backends.

These scenarios include:

* :meth:`.Query.count`::

        session.query(User).count()

* :meth:`.Query.update` and :meth:`.Query.delete`, both for the UPDATE/DELETE
  statement as well as for the SELECT used by the "fetch" strategy::

        session.query(User).filter(User.id == 15).update(
                {"name": "foob"}, synchronize_session='fetch')

        session.query(User).filter(User.id == 15).delete(
                synchronize_session='fetch')

* Queries against individual columns::

        session.query(User.id, User.name).all()

* SQL functions and other expressions against indirect mappings such as
  :obj:`.column_property`::

        class User(Base):
            # ...

            score = column_property(func.coalesce(self.tables.users.c.name, None)))

        session.query(func.max(User.score)).scalar()

:ticket:`3227` :ticket:`3242` :ticket:`1326`

.. _feature_2963:

.info dictionary improvements
-----------------------------

The :attr:`.InspectionAttr.info` collection is now available on every kind
of object that one would retrieve from the :attr:`.Mapper.all_orm_descriptors`
collection.  This includes :class:`.hybrid_property` and :func:`.association_proxy`.
However, as these objects are class-bound descriptors, they must be accessed
**separately** from the class to which they are attached in order to get
at the attribute.  Below this is illustared using the
:attr:`.Mapper.all_orm_descriptors` namespace::

    class SomeObject(Base):
        # ...

        @hybrid_property
        def some_prop(self):
            return self.value + 5


    inspect(SomeObject).all_orm_descriptors.some_prop.info['foo'] = 'bar'

It is also available as a constructor argument for all :class:`.SchemaItem`
objects (e.g. :class:`.ForeignKey`, :class:`.UniqueConstraint` etc.) as well
as remaining ORM constructs such as :func:`.orm.synonym`.

:ticket:`2971`

:ticket:`2963`

.. _bug_3188:

ColumnProperty constructs work a lot better with aliases, order_by
-------------------------------------------------------------------

A variety of issues regarding :func:`.column_property` have been fixed,
most specifically with regards to the :func:`.aliased` construct as well
as the "order by label" logic introduced in 0.9 (see :ref:`migration_1068`).

Given a mapping like the following::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)

    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))


    A.b = column_property(
            select([func.max(B.id)]).where(B.a_id == A.id).correlate(A)
        )

A simple scenario that included "A.b" twice would fail to render
correctly::

    print(sess.query(A, a1).order_by(a1.b))

This would order by the wrong column::

    SELECT a.id AS a_id, (SELECT max(b.id) AS max_1 FROM b
    WHERE b.a_id = a.id) AS anon_1, a_1.id AS a_1_id,
    (SELECT max(b.id) AS max_2
    FROM b WHERE b.a_id = a_1.id) AS anon_2
    FROM a, a AS a_1 ORDER BY anon_1

New output::

    SELECT a.id AS a_id, (SELECT max(b.id) AS max_1
    FROM b WHERE b.a_id = a.id) AS anon_1, a_1.id AS a_1_id,
    (SELECT max(b.id) AS max_2
    FROM b WHERE b.a_id = a_1.id) AS anon_2
    FROM a, a AS a_1 ORDER BY anon_2

There were also many scenarios where the "order by" logic would fail
to order by label, for example if the mapping were "polymorphic"::

    class A(Base):
        __tablename__ = 'a'

        id = Column(Integer, primary_key=True)
        type = Column(String)

        __mapper_args__ = {'polymorphic_on': type, 'with_polymorphic': '*'}

The order_by would fail to use the label, as it would be anonymized due
to the polymorphic loading::

    SELECT a.id AS a_id, a.type AS a_type, (SELECT max(b.id) AS max_1
    FROM b WHERE b.a_id = a.id) AS anon_1
    FROM a ORDER BY (SELECT max(b.id) AS max_2
    FROM b WHERE b.a_id = a.id)

Now that the order by label tracks the anonymized label, this now works::

    SELECT a.id AS a_id, a.type AS a_type, (SELECT max(b.id) AS max_1
    FROM b WHERE b.a_id = a.id) AS anon_1
    FROM a ORDER BY anon_1

Included in these fixes are a variety of heisenbugs that could corrupt
the state of an ``aliased()`` construct such that the labeling logic
would again fail; these have also been fixed.

:ticket:`3148` :ticket:`3188`

New Features and Improvements - Core
====================================

.. _feature_3034:

Select/Query LIMIT / OFFSET may be specified as an arbitrary SQL expression
----------------------------------------------------------------------------

The :meth:`.Select.limit` and :meth:`.Select.offset` methods now accept
any SQL expression, in addition to integer values, as arguments.  The ORM
:class:`.Query` object also passes through any expression to the underlying
:class:`.Select` object.   Typically
this is used to allow a bound parameter to be passed, which can be substituted
with a value later::

    sel = select([table]).limit(bindparam('mylimit')).offset(bindparam('myoffset'))

Dialects which don't support non-integer LIMIT or OFFSET expressions may continue
to not support this behavior; third party dialects may also need modification
in order to take advantage of the new behavior.  A dialect which currently
uses the ``._limit`` or ``._offset`` attributes will continue to function
for those cases where the limit/offset was specified as a simple integer value.
However, when a SQL expression is specified, these two attributes will
instead raise a :class:`.CompileError` on access.  A third-party dialect which
wishes to support the new feature should now call upon the ``._limit_clause``
and ``._offset_clause`` attributes to receive the full SQL expression, rather
than the integer value.

.. _feature_3282:

The ``use_alter`` flag on ``ForeignKeyConstraint`` is (usually) no longer needed
--------------------------------------------------------------------------------

The :meth:`.MetaData.create_all` and :meth:`.MetaData.drop_all` methods will
now make use of a system that automatically renders an ALTER statement
for foreign key constraints that are involved in mutually-dependent cycles
between tables, without the
need to specify :paramref:`.ForeignKeyConstraint.use_alter`.   Additionally,
the foreign key constraints no longer need to have a name in order to be
created via ALTER; only the DROP operation requires a name.   In the case
of a DROP, the feature will ensure that only constraints which have
explicit names are actually included as ALTER statements.  In the
case of an unresolvable cycle within a DROP, the system emits
a succinct and clear error message now if the DROP cannot proceed.

The :paramref:`.ForeignKeyConstraint.use_alter` and
:paramref:`.ForeignKey.use_alter` flags remain in place, and continue to have
the same effect of establishing those constraints for which ALTER is
required during a CREATE/DROP scenario.

As of version 1.0.1, special logic takes over in the case of SQLite, which
does not support ALTER, in the case that during a DROP, the given tables have
an unresolvable cycle; in this case a warning is emitted, and the tables
are dropped with **no** ordering, which is usually fine on SQLite unless
constraints are enabled. To resolve the warning and proceed with at least
a partial ordering on a SQLite database, particularly one where constraints
are enabled, re-apply "use_alter" flags to those
:class:`.ForeignKey` and :class:`.ForeignKeyConstraint` objects which should
be explicitly omitted from the sort.

.. seealso::

    :ref:`use_alter` - full description of the new behavior.

:ticket:`3282`

.. _change_3330:

ResultProxy "auto close" is now a "soft" close
----------------------------------------------

For many releases, the :class:`.ResultProxy` object has always been
automatically closed out at the point at which all result rows have been
fetched.  This was to allow usage of the object without the need to call
upon :meth:`.ResultProxy.close` explicitly; as all DBAPI resources had been
freed, the object was safe to discard.   However, the object maintained
a strict "closed" behavior, which meant that any subsequent calls to
:meth:`.ResultProxy.fetchone`, :meth:`.ResultProxy.fetchmany` or
:meth:`.ResultProxy.fetchall` would now raise a :class:`.ResourceClosedError`::

    >>> result = connection.execute(stmt)
    >>> result.fetchone()
    (1, 'x')
    >>> result.fetchone()
    None  # indicates no more rows
    >>> result.fetchone()
    exception: ResourceClosedError

This behavior is inconsistent vs. what pep-249 states, which is
that you can call upon the fetch methods repeatedly even after results
are exhausted.  It also interferes with behavior for some implementations of
result proxy, such as the :class:`.BufferedColumnResultProxy` used by the
cx_oracle dialect for certain datatypes.

To solve this, the "closed" state of the :class:`.ResultProxy` has been
broken into two states; a "soft close" which does the majority of what
"close" does, in that it releases the DBAPI cursor and in the case of a
"close with result" object will also release the connection, and a
"closed" state which is everything included by "soft close" as well as
establishing the fetch methods as "closed".   The :meth:`.ResultProxy.close`
method is now never called implicitly, only the :meth:`.ResultProxy._soft_close`
method which is non-public::

    >>> result = connection.execute(stmt)
    >>> result.fetchone()
    (1, 'x')
    >>> result.fetchone()
    None  # indicates no more rows
    >>> result.fetchone()
    None  # still None
    >>> result.fetchall()
    []
    >>> result.close()
    >>> result.fetchone()
    exception: ResourceClosedError  # *now* it raises

:ticket:`3330`
:ticket:`3329`

CHECK Constraints now support the ``%(column_0_name)s`` token in naming conventions
-----------------------------------------------------------------------------------

The ``%(column_0_name)s`` will derive from the first column found in the
expression of a :class:`.CheckConstraint`::

    metadata = MetaData(
        naming_convention={"ck": "ck_%(table_name)s_%(column_0_name)s"}
    )

    foo = Table('foo', metadata,
        Column('value', Integer),
    )

    CheckConstraint(foo.c.value > 5)

Will render::

    CREATE TABLE foo (
        value INTEGER,
        CONSTRAINT ck_foo_value CHECK (value > 5)
    )

The combination of naming conventions with the constraint produced by a
:class:`.SchemaType` such as :class:`.Boolean` or :class:`.Enum` will also
now make use of all CHECK constraint conventions.

.. seealso::

    :ref:`naming_check_constraints`

    :ref:`naming_schematypes`

:ticket:`3299`

.. _change_3341:

Constraints referring to unattached Columns can auto-attach to the Table when their referred columns are attached
-----------------------------------------------------------------------------------------------------------------

Since at least version 0.8, a :class:`.Constraint` has had the ability to
"auto-attach" itself to a :class:`.Table` based on being passed table-attached columns::

    from sqlalchemy import Table, Column, MetaData, Integer, UniqueConstraint

    m = MetaData()

    t = Table('t', m,
        Column('a', Integer),
        Column('b', Integer)
    )

    uq = UniqueConstraint(t.c.a, t.c.b)  # will auto-attach to Table

    assert uq in t.constraints

In order to assist with some cases that tend to come up with declarative,
this same auto-attachment logic can now function even if the :class:`.Column`
objects are not yet associated with the :class:`.Table`; additional events
are established such that when those :class:`.Column` objects are associated,
the :class:`.Constraint` is also added::

    from sqlalchemy import Table, Column, MetaData, Integer, UniqueConstraint

    m = MetaData()

    a = Column('a', Integer)
    b = Column('b', Integer)

    uq = UniqueConstraint(a, b)

    t = Table('t', m, a, b)

    assert uq in t.constraints  # constraint auto-attached

The above feature was a late add as of version 1.0.0b3.  A fix as of
version 1.0.4 for :ticket:`3411` ensures that this logic
does not occur if the :class:`.Constraint` refers to a mixture of
:class:`.Column` objects and string column names; as we do not yet have
tracking for the addition of names to a :class:`.Table`::

    from sqlalchemy import Table, Column, MetaData, Integer, UniqueConstraint

    m = MetaData()

    a = Column('a', Integer)
    b = Column('b', Integer)

    uq = UniqueConstraint(a, 'b')

    t = Table('t', m, a, b)

    # constraint *not* auto-attached, as we do not have tracking
    # to locate when a name 'b' becomes available on the table
    assert uq not in t.constraints

Above, the attachment event for column "a" to table "t" will fire off before
column "b" is attached (as "a" is stated in the :class:`.Table` constructor
before "b"), and the constraint will fail to locate "b" if it were to attempt
an attachment.  For consistency, if the constraint refers to any string names,
the autoattach-on-column-attach logic is skipped.

The original auto-attach logic of course remains in place, if the :class:`.Table`
already contains all the target :class:`.Column` objects at the time
the :class:`.Constraint` is constructed::

    from sqlalchemy import Table, Column, MetaData, Integer, UniqueConstraint

    m = MetaData()

    a = Column('a', Integer)
    b = Column('b', Integer)


    t = Table('t', m, a, b)

    uq = UniqueConstraint(a, 'b')

    # constraint auto-attached normally as in older versions
    assert uq in t.constraints


:ticket:`3341`
:ticket:`3411`

.. _change_2051:

.. _feature_insert_from_select_defaults:

INSERT FROM SELECT now includes Python and SQL-expression defaults
-------------------------------------------------------------------

:meth:`.Insert.from_select` now includes Python and SQL-expression defaults if
otherwise unspecified; the limitation where non-server column defaults
aren't included in an INSERT FROM SELECT is now lifted and these
expressions are rendered as constants into the SELECT statement::

    from sqlalchemy import Table, Column, MetaData, Integer, select, func

    m = MetaData()

    t = Table(
        't', m,
        Column('x', Integer),
        Column('y', Integer, default=func.somefunction()))

    stmt = select([t.c.x])
    print(t.insert().from_select(['x'], stmt))

Will render::

    INSERT INTO t (x, y) SELECT t.x, somefunction() AS somefunction_1
    FROM t

The feature can be disabled using
:paramref:`.Insert.from_select.include_defaults`.

.. _change_3087:

Column server defaults now render literal values
------------------------------------------------

The "literal binds" compiler flag is switched on when a
:class:`.DefaultClause`, set up by :paramref:`.Column.server_default`
is present as a SQL expression to be compiled.  This allows literals
embedded in SQL to render correctly, such as::

    from sqlalchemy import Table, Column, MetaData, Text
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.dialects.postgresql import ARRAY, array
    from sqlalchemy.dialects import postgresql

    metadata = MetaData()

    tbl = Table("derp", metadata,
        Column("arr", ARRAY(Text),
                    server_default=array(["foo", "bar", "baz"])),
    )

    print(CreateTable(tbl).compile(dialect=postgresql.dialect()))

Now renders::

    CREATE TABLE derp (
        arr TEXT[] DEFAULT ARRAY['foo', 'bar', 'baz']
    )

Previously, the literal values ``"foo", "bar", "baz"`` would render as
bound parameters, which are useless in DDL.

:ticket:`3087`

.. _feature_3184:

UniqueConstraint is now part of the Table reflection process
------------------------------------------------------------

A :class:`.Table` object populated using ``autoload=True`` will now
include :class:`.UniqueConstraint` constructs as well as
:class:`.Index` constructs.  This logic has a few caveats for
PostgreSQL and MySQL:

PostgreSQL
^^^^^^^^^^

PostgreSQL has the behavior such that when a UNIQUE constraint is
created, it implicitly creates a UNIQUE INDEX corresponding to that
constraint as well. The :meth:`.Inspector.get_indexes` and the
:meth:`.Inspector.get_unique_constraints` methods will continue to
**both** return these entries distinctly, where
:meth:`.Inspector.get_indexes` now features a token
``duplicates_constraint`` within the index entry  indicating the
corresponding constraint when detected.   However, when performing
full table reflection using  ``Table(..., autoload=True)``, the
:class:`.Index` construct is detected as being linked to the
:class:`.UniqueConstraint`, and is **not** present within the
:attr:`.Table.indexes` collection; only the :class:`.UniqueConstraint`
will be present in the :attr:`.Table.constraints` collection.   This
deduplication logic works by joining to the ``pg_constraint`` table
when querying ``pg_index`` to see if the two constructs are linked.

MySQL
^^^^^

MySQL does not have separate concepts for a UNIQUE INDEX and a UNIQUE
constraint.  While it supports both syntaxes when creating tables and indexes,
it does not store them any differently. The
:meth:`.Inspector.get_indexes`
and the :meth:`.Inspector.get_unique_constraints` methods will continue to
**both** return an entry for a UNIQUE index in MySQL,
where :meth:`.Inspector.get_unique_constraints` features a new token
``duplicates_index`` within the constraint entry indicating that this is a
dupe entry corresponding to that index.  However, when performing
full table reflection using ``Table(..., autoload=True)``,
the :class:`.UniqueConstraint` construct is
**not** part of the fully reflected :class:`.Table` construct under any
circumstances; this construct is always represented by a :class:`.Index`
with the ``unique=True`` setting present in the :attr:`.Table.indexes`
collection.

.. seealso::

    :ref:`postgresql_index_reflection`

    :ref:`mysql_unique_constraints`

:ticket:`3184`


New systems to safely emit parameterized warnings
-------------------------------------------------

For a long time, there has been a restriction that warning messages could not
refer to data elements, such that a particular function might emit an
infinite number of unique warnings.  The key place this occurs is in the
``Unicode type received non-unicode bind param value`` warning.  Placing
the data value in this message would mean that the Python ``__warningregistry__``
for that module, or in some cases the Python-global ``warnings.onceregistry``,
would grow unbounded, as in most warning scenarios, one of these two collections
is populated with every distinct warning message.

The change here is that by using a special ``string`` type that purposely
changes how the string is hashed, we can control that a large number of
parameterized messages are hashed only on a small set of possible hash
values, such that a warning such as ``Unicode type received non-unicode
bind param value`` can be tailored to be emitted only a specific number
of times; beyond that, the Python warnings registry will begin recording
them as duplicates.

To illustrate, the following test script will show only ten warnings being
emitted for ten of the parameter sets, out of a total of 1000::

    from sqlalchemy import create_engine, Unicode, select, cast
    import random
    import warnings

    e = create_engine("sqlite://")

    # Use the "once" filter (which is also the default for Python
    # warnings).  Exactly ten of these warnings will
    # be emitted; beyond that, the Python warnings registry will accumulate
    # new values as dupes of one of the ten existing.
    warnings.filterwarnings("once")

    for i in range(1000):
        e.execute(select([cast(
            ('foo_%d' % random.randint(0, 1000000)).encode('ascii'), Unicode)]))

The format of the warning here is::

    /path/lib/sqlalchemy/sql/sqltypes.py:186: SAWarning: Unicode type received
      non-unicode bind param value 'foo_4852'. (this warning may be
      suppressed after 10 occurrences)


:ticket:`3178`

Key Behavioral Changes - ORM
============================

.. _bug_3228:

query.update() now resolves string names into mapped attribute names
--------------------------------------------------------------------

The documentation for :meth:`.Query.update` states that the given
``values`` dictionary is "a dictionary with attributes names as keys",
implying that these are mapped attribute names.  Unfortunately, the function
was designed more in mind to receive attributes and SQL expressions and
not as much strings; when strings
were passed, these strings would be passed through straight to the core
update statement without any resolution as far as how these names are
represented on the mapped class, meaning the name would have to match that
of a table column exactly, not how an attribute of that name was mapped
onto the class.

The string names are now resolved as attribute names in earnest::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column('user_name', String(50))

Above, the column ``user_name`` is mapped as ``name``.  Previously,
a call to :meth:`.Query.update` that was passed strings would have to
have been called as follows::

    session.query(User).update({'user_name': 'moonbeam'})

The given string is now resolved against the entity::

    session.query(User).update({'name': 'moonbeam'})

It is typically preferable to use the attribute directly, to avoid any
ambiguity::

    session.query(User).update({User.name: 'moonbeam'})

The change also indicates that synonyms and hybrid attributes can be referred
to by string name as well::

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column('user_name', String(50))

        @hybrid_property
        def fullname(self):
            return self.name

    session.query(User).update({'fullname': 'moonbeam'})

:ticket:`3228`

.. _bug_3371:

Warnings emitted when comparing objects with None values to relationships
-------------------------------------------------------------------------

This change is new as of 1.0.1.  Some users are performing
queries that are essentially of this form::

    session.query(Address).filter(Address.user == User(id=None))

This pattern is not currently supported in SQLAlchemy.  For all versions,
it emits SQL resembling::

    SELECT address.id AS address_id, address.user_id AS address_user_id,
    address.email_address AS address_email_address
    FROM address WHERE ? = address.user_id
    (None,)

Note above, there is a comparison ``WHERE ? = address.user_id`` where the
bound value ``?`` is receiving ``None``, or ``NULL`` in SQL.  **This will
always return False in SQL**.  The comparison here would in theory
generate SQL as follows::

    SELECT address.id AS address_id, address.user_id AS address_user_id,
    address.email_address AS address_email_address
    FROM address WHERE address.user_id IS NULL

But right now, **it does not**.   Applications which are relying upon the
fact that "NULL = NULL" produces False in all cases run the risk that
someday, SQLAlchemy might fix this issue to generate "IS NULL", and the queries
will then produce different results.  Therefore with this kind of operation,
you will see a warning::

    SAWarning: Got None for value of column user.id; this is unsupported
    for a relationship comparison and will not currently produce an
    IS comparison (but may in a future release)

Note that this pattern was broken in most cases for release 1.0.0 including
all of the betas; a value like ``SYMBOL('NEVER_SET')`` would be generated.
This issue has been fixed, but as a result of identifying this pattern,
the warning is now there so that we can more safely repair this broken
behavior (now captured in :ticket:`3373`) in a future release.

:ticket:`3371`

.. _bug_3374:

A "negated contains or equals" relationship comparison will use the current value of attributes, not the database value
-------------------------------------------------------------------------------------------------------------------------

This change is new as of 1.0.1; while we would have preferred for this to be in 1.0.0,
it only became apparent as a result of :ticket:`3371`.

Given a mapping::

    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)

    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))
        a = relationship("A")

Given ``A``, with primary key of 7, but which we changed to be 10
without flushing::

    s = Session(autoflush=False)
    a1 = A(id=7)
    s.add(a1)
    s.commit()

    a1.id = 10

A query against a many-to-one relationship with this object as the target
will use the value 10 in the bound parameters::

    s.query(B).filter(B.a == a1)

Produces::

    SELECT b.id AS b_id, b.a_id AS b_a_id
    FROM b
    WHERE ? = b.a_id
    (10,)

However, before this change, the negation of this criteria would **not** use
10, it would use 7, unless the object were flushed first::

    s.query(B).filter(B.a != a1)

Produces (in 0.9 and all versions prior to 1.0.1)::

    SELECT b.id AS b_id, b.a_id AS b_a_id
    FROM b
    WHERE b.a_id != ? OR b.a_id IS NULL
    (7,)

For a transient object, it would produce a broken query::

    SELECT b.id, b.a_id
    FROM b
    WHERE b.a_id != :a_id_1 OR b.a_id IS NULL
    {u'a_id_1': symbol('NEVER_SET')}

This inconsistency has been repaired, and in all queries the current attribute
value, in this example ``10``, will now be used.

:ticket:`3374`

.. _migration_3061:

Changes to attribute events and other operations regarding attributes that have no pre-existing value
------------------------------------------------------------------------------------------------------

In this change, the default return value of ``None`` when accessing an object
is now returned dynamically on each access, rather than implicitly setting the
attribute's state with a special "set" operation when it is first accessed.
The visible result of this change is that ``obj.__dict__`` is not implicitly
modified on get, and there are also some minor behavioral changes
for :func:`.attributes.get_history` and related functions.

Given an object with no state::

    >>> obj = Foo()

It has always been SQLAlchemy's behavior such that if we access a scalar
or many-to-one attribute that was never set, it is returned as ``None``::

    >>> obj.someattr
    None

This value of ``None`` is in fact now part of the state of ``obj``, and is
not unlike as though we had set the attribute explicitly, e.g.
``obj.someattr = None``.  However, the "set on get" here would behave
differently as far as history and events.   It would not emit any attribute
event, and additionally if we view history, we see this::

    >>> inspect(obj).attrs.someattr.history
    History(added=(), unchanged=[None], deleted=())   # 0.9 and below

That is, it's as though the attribute were always ``None`` and were
never changed.  This is explicitly different from if we had set the
attribute first instead::

    >>> obj = Foo()
    >>> obj.someattr = None
    >>> inspect(obj).attrs.someattr.history
    History(added=[None], unchanged=(), deleted=())  # all versions

The above means that the behavior of our "set" operation can be corrupted
by the fact that the value was accessed via "get" earlier.  In 1.0, this
inconsistency has been resolved, by no longer actually setting anything
when the default "getter" is used.

    >>> obj = Foo()
    >>> obj.someattr
    None
    >>> inspect(obj).attrs.someattr.history
    History(added=(), unchanged=(), deleted=())  # 1.0
    >>> obj.someattr = None
    >>> inspect(obj).attrs.someattr.history
    History(added=[None], unchanged=(), deleted=())

The reason the above behavior hasn't had much impact is because the
INSERT statement in relational databases considers a missing value to be
the same as NULL in most cases.   Whether SQLAlchemy received a history
event for a particular attribute set to None or not would usually not matter;
as the difference between sending None/NULL or not wouldn't have an impact.
However, as :ticket:`3060` (described here in :ref:`migration_3060`)
illustrates, there are some seldom edge cases
where we do in fact want to positively have ``None`` set.  Also, allowing
the attribute event here means it's now possible to create "default value"
functions for ORM mapped attributes.

As part of this change, the generation of the implicit "None" is now disabled
for other situations where this used to occur; this includes when an
attribute set operation on a many-to-one is received; previously, the "old" value
would be "None" if it had been not set otherwise; it now will send the
value :data:`.orm.attributes.NEVER_SET`, which is a value that may be sent
to an attribute listener now.   This symbol may also be received when
calling on mapper utility functions such as :meth:`.Mapper.primary_key_from_instance`;
if the primary key attributes have no setting at all, whereas the value
would be ``None`` before, it will now be the :data:`.orm.attributes.NEVER_SET`
symbol, and no change to the object's state occurs.

:ticket:`3061`

.. _migration_3060:

Priority of attribute changes on relationship-bound attributes vs. FK-bound may appear to change
------------------------------------------------------------------------------------------------

As a side effect of :ticket:`3060`, setting a relationship-bound attribute to ``None``
is now a tracked history event which refers to the intention of persisting
``None`` to that attribute.   As it has always been the case that setting a
relationship-bound attribute will trump direct assignment to the foreign key
attributes, a change in behavior can be seen here when assigning None.
Given a mapping::

    class A(Base):
        __tablename__ = 'table_a'

        id = Column(Integer, primary_key=True)

    class B(Base):
        __tablename__ = 'table_b'

        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('table_a.id'))
        a = relationship(A)

In 1.0, the relationship-bound attribute takes precedence over the FK-bound
attribute in all cases, whether or not
the value we assign is a reference to an ``A`` object or is ``None``.
In 0.9, the behavior is inconsistent and
only takes effect if a value is assigned; the None is not considered::

    a1 = A(id=1)
    a2 = A(id=2)
    session.add_all([a1, a2])
    session.flush()

    b1 = B()
    b1.a = a1   # we expect a_id to be '1'; takes precedence in 0.9 and 1.0

    b2 = B()
    b2.a = None  # we expect a_id to be None; takes precedence only in 1.0

    b1.a_id = 2
    b2.a_id = 2

    session.add_all([b1, b2])
    session.commit()

    assert b1.a is a1  # passes in both 0.9 and 1.0
    assert b2.a is None  # passes in 1.0, in 0.9 it's a2

:ticket:`3060`

.. _bug_3139:

session.expunge() will fully detach an object that's been deleted
-----------------------------------------------------------------

The behavior of :meth:`.Session.expunge` had a bug that caused an
inconsistency in behavior regarding deleted objects.  The
:func:`.object_session` function as well as the :attr:`.InstanceState.session`
attribute would still report object as belonging to the :class:`.Session`
subsequent to the expunge::

    u1 = sess.query(User).first()
    sess.delete(u1)

    sess.flush()

    assert u1 not in sess
    assert inspect(u1).session is sess  # this is normal before commit

    sess.expunge(u1)

    assert u1 not in sess
    assert inspect(u1).session is None  # would fail

Note that it is normal for ``u1 not in sess`` to be True while
``inspect(u1).session`` still refers to the session, while the transaction
is ongoing subsequent to the delete operation and :meth:`.Session.expunge`
has not been called; the full detachment normally completes once the
transaction is committed.  This issue would also impact functions
that rely on :meth:`.Session.expunge` such as :func:`.make_transient`.

:ticket:`3139`

.. _migration_yield_per_eager_loading:

Joined/Subquery eager loading explicitly disallowed with yield_per
------------------------------------------------------------------

In order to make the :meth:`.Query.yield_per` method easier to use,
an exception is raised if any subquery eager loaders, or joined
eager loaders that would use collections, are
to take effect when yield_per is used, as these are currently not compatible
with yield-per (subquery loading could be in theory, however).
When this error is raised, the :func:`.lazyload` option can be sent with
an asterisk::

    q = sess.query(Object).options(lazyload('*')).yield_per(100)

or use :meth:`.Query.enable_eagerloads`::

    q = sess.query(Object).enable_eagerloads(False).yield_per(100)

The :func:`.lazyload` option has the advantage that additional many-to-one
joined loader options can still be used::

    q = sess.query(Object).options(
        lazyload('*'), joinedload("some_manytoone")).yield_per(100)

.. _bug_3233:

Changes and fixes in handling of duplicate join targets
--------------------------------------------------------

Changes here encompass bugs where an unexpected and inconsistent
behavior would occur in some scenarios when joining to an entity
twice, or to multple single-table entities against the same table,
without using a relationship-based ON clause, as well as when joining
multiple times to the same target relationship.

Starting with a mapping as::

    from sqlalchemy import Integer, Column, String, ForeignKey
    from sqlalchemy.orm import Session, relationship
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)
        bs = relationship("B")

    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))

A query that joins to ``A.bs`` twice::

    print(s.query(A).join(A.bs).join(A.bs))

Will render::

    SELECT a.id AS a_id
    FROM a JOIN b ON a.id = b.a_id

The query deduplicates the redundant ``A.bs`` because it is attempting
to support a case like the following::

    s.query(A).join(A.bs).\
        filter(B.foo == 'bar').\
        reset_joinpoint().join(A.bs, B.cs).filter(C.bar == 'bat')

That is, the ``A.bs`` is part of a "path".  As part of :ticket:`3367`,
arriving at the same endpoint twice without it being part of a
larger path will now emit a warning::

    SAWarning: Pathed join target A.bs has already been joined to; skipping

The bigger change involves when joining to an entity without using a
relationship-bound path.  If we join to ``B`` twice::

    print(s.query(A).join(B, B.a_id == A.id).join(B, B.a_id == A.id))

In 0.9, this would render as follows::

    SELECT a.id AS a_id
    FROM a JOIN b ON b.a_id = a.id JOIN b AS b_1 ON b_1.a_id = a.id

This is problematic since the aliasing is implicit and in the case of different
ON clauses can lead to unpredictable results.

In 1.0, no automatic aliasing is applied and we get::

    SELECT a.id AS a_id
    FROM a JOIN b ON b.a_id = a.id JOIN b ON b.a_id = a.id

This will raise an error from the database.  While it might be nice if
the "duplicate join target" acted identically if we joined both from
redundant relationships vs. redundant non-relationship based targets,
for now we are only changing the behavior in the more serious case where
implicit aliasing would have occurred previously, and only emitting a warning
in the relationship case.  Ultimately, joining to the same thing twice without
any aliasing to disambiguate should raise an error in all cases.

The change also has an impact on single-table inheritance targets.  Using
a mapping as follows::

    from sqlalchemy import Integer, Column, String, ForeignKey
    from sqlalchemy.orm import Session, relationship
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class A(Base):
        __tablename__ = "a"

        id = Column(Integer, primary_key=True)
        type = Column(String)

        __mapper_args__ = {'polymorphic_on': type, 'polymorphic_identity': 'a'}


    class ASub1(A):
        __mapper_args__ = {'polymorphic_identity': 'asub1'}


    class ASub2(A):
        __mapper_args__ = {'polymorphic_identity': 'asub2'}


    class B(Base):
        __tablename__ = 'b'

        id = Column(Integer, primary_key=True)

        a_id = Column(Integer, ForeignKey("a.id"))

        a = relationship("A", primaryjoin="B.a_id == A.id", backref='b')

    s = Session()

    print(s.query(ASub1).join(B, ASub1.b).join(ASub2, B.a))

    print(s.query(ASub1).join(B, ASub1.b).join(ASub2, ASub2.id == B.a_id))

The two queries at the bottom are equivalent, and should both render
the identical SQL::

    SELECT a.id AS a_id, a.type AS a_type
    FROM a JOIN b ON b.a_id = a.id JOIN a ON b.a_id = a.id AND a.type IN (:type_1)
    WHERE a.type IN (:type_2)

The above SQL is invalid, as it renders "a" within the FROM list twice.
However, the implicit aliasing bug would occur with the second query only
and render this instead::

    SELECT a.id AS a_id, a.type AS a_type
    FROM a JOIN b ON b.a_id = a.id JOIN a AS a_1
    ON a_1.id = b.a_id AND a_1.type IN (:type_1)
    WHERE a_1.type IN (:type_2)

Where above, the second join to "a" is aliased.  While this seems convenient,
it's not how single-inheritance queries work in general and is misleading
and inconsistent.

The net effect is that applications which were relying on this bug will now
have an error raised by the database.   The solution is to use the expected
form.  When referring to multiple subclasses of a single-inheritance
entity in a query, you must manually use aliases to disambiguate the table,
as all the subclasses normally refer to the same table::

    asub2_alias = aliased(ASub2)

    print(s.query(ASub1).join(B, ASub1.b).join(asub2_alias, B.a.of_type(asub2_alias)))

:ticket:`3233`
:ticket:`3367`


Deferred Columns No Longer Implicitly Undefer
---------------------------------------------

Mapped attributes marked as deferred without explicit undeferral
will now remain "deferred" even if their column is otherwise
present in the result set in some way.   This is a performance
enhancement in that an ORM load no longer spends time searching
for each deferred column when the result set is obtained.  However,
for an application that has been relying upon this, an explicit
:func:`.undefer` or similar option should now be used, in order
to prevent a SELECT from being emitted when the attribute is accessed.


.. _migration_deprecated_orm_events:

Deprecated ORM Event Hooks Removed
----------------------------------

The following ORM event hooks, some of which have been deprecated since
0.5, have been removed:   ``translate_row``, ``populate_instance``,
``append_result``, ``create_instance``.  The use cases for these hooks
originated in the very early 0.1 / 0.2 series of SQLAlchemy and have long
since been unnecessary.  In particular, the hooks were largely unusable
as the behavioral contracts within these events was strongly linked to
the surrounding internals, such as how an instance needs to be created
and initialized as well as how columns are located within an ORM-generated
row.   The removal of these hooks greatly simplifies the mechanics of ORM
object loading.

.. _bundle_api_change:

API Change for new Bundle feature when custom row loaders are used
------------------------------------------------------------------

The new :class:`.Bundle` object of 0.9 has a small change in API,
when the ``create_row_processor()`` method is overridden on a custom class.
Previously, the sample code looked like::

    from sqlalchemy.orm import Bundle

    class DictBundle(Bundle):
        def create_row_processor(self, query, procs, labels):
            """Override create_row_processor to return values as dictionaries"""
            def proc(row, result):
                return dict(
                            zip(labels, (proc(row, result) for proc in procs))
                        )
            return proc

The unused ``result`` member is now removed::

    from sqlalchemy.orm import Bundle

    class DictBundle(Bundle):
        def create_row_processor(self, query, procs, labels):
            """Override create_row_processor to return values as dictionaries"""
            def proc(row):
                return dict(
                            zip(labels, (proc(row) for proc in procs))
                        )
            return proc

.. seealso::

    :ref:`bundles`

.. _migration_3008:

Right inner join nesting now the default for joinedload with innerjoin=True
---------------------------------------------------------------------------

The behavior of :paramref:`.joinedload.innerjoin` as well as
:paramref:`.relationship.innerjoin` is now to use "nested"
inner joins, that is, right-nested, as the default behavior when an
inner join joined eager load is chained to an outer join eager load.  In
order to get the old behavior of chaining all joined eager loads as
outer join when an outer join is present, use ``innerjoin="unnested"``.

As introduced in :ref:`feature_2976` from version 0.9, the behavior of
``innerjoin="nested"`` is that an inner join eager load chained to an outer
join eager load will use a right-nested join.  ``"nested"`` is now implied
when using ``innerjoin=True``::

    query(User).options(
        joinedload("orders", innerjoin=False).joinedload("items", innerjoin=True))

With the new default, this will render the FROM clause in the form::

    FROM users LEFT OUTER JOIN (orders JOIN items ON <onclause>) ON <onclause>

That is, using a right-nested join for the INNER join so that the full
result of ``users`` can be returned.   The use of an INNER join is more efficient
than using an OUTER join, and allows the :paramref:`.joinedload.innerjoin`
optimization parameter to take effect in all cases.

To get the older behavior, use ``innerjoin="unnested"``::

    query(User).options(
        joinedload("orders", innerjoin=False).joinedload("items", innerjoin="unnested"))

This will avoid right-nested joins and chain the joins together using all
OUTER joins despite the innerjoin directive::

    FROM users LEFT OUTER JOIN orders ON <onclause> LEFT OUTER JOIN items ON <onclause>

As noted in the 0.9 notes, the only database backend that has difficulty
with right-nested joins is SQLite; SQLAlchemy as of 0.9 converts a right-nested
join into a subquery as a join target on SQLite.

.. seealso::

    :ref:`feature_2976` - description of the feature as introduced in 0.9.4.

:ticket:`3008`

.. _change_3249:

Subqueries no longer applied to uselist=False joined eager loads
----------------------------------------------------------------

Given a joined eager load like the following::

    class A(Base):
        __tablename__ = 'a'
        id = Column(Integer, primary_key=True)
        b = relationship("B", uselist=False)


    class B(Base):
        __tablename__ = 'b'
        id = Column(Integer, primary_key=True)
        a_id = Column(ForeignKey('a.id'))

    s = Session()
    print(s.query(A).options(joinedload(A.b)).limit(5))

SQLAlchemy considers the relationship ``A.b`` to be a "one to many,
loaded as a single value", which is essentially a "one to one"
relationship.  However, joined eager loading has always treated the
above as a situation where the main query needs to be inside a
subquery, as would normally be needed for a collection of B objects
where the main query has a LIMIT applied::

    SELECT anon_1.a_id AS anon_1_a_id, b_1.id AS b_1_id, b_1.a_id AS b_1_a_id
    FROM (SELECT a.id AS a_id
    FROM a LIMIT :param_1) AS anon_1
    LEFT OUTER JOIN b AS b_1 ON anon_1.a_id = b_1.a_id

However, since the relationship of the inner query to the outer one is
that at most only one row is shared in the case of ``uselist=False``
(in the same way as a many-to-one), the "subquery" used with LIMIT +
joined eager loading is now dropped in this case::

    SELECT a.id AS a_id, b_1.id AS b_1_id, b_1.a_id AS b_1_a_id
    FROM a LEFT OUTER JOIN b AS b_1 ON a.id = b_1.a_id
    LIMIT :param_1

In the case that the LEFT OUTER JOIN returns more than one row, the ORM
has always emitted a warning here and ignored additional results for
``uselist=False``, so the results in that error situation should not change.

:ticket:`3249`


query.update() / query.delete() raises if used with join(), select_from(), from_self()
--------------------------------------------------------------------------------------

A warning is emitted in SQLAlchemy 0.9.10 (not yet released as of
June 9, 2015) when the :meth:`.Query.update` or :meth:`.Query.delete` methods
are invoked against a query which has also called upon :meth:`.Query.join`,
:meth:`.Query.outerjoin`,
:meth:`.Query.select_from` or :meth:`.Query.from_self`.  These are unsupported
use cases which silently fail in the 0.9 series up until 0.9.10 where it emits
a warning.  In 1.0, these cases raise an exception.

:ticket:`3349`


query.update() with ``synchronize_session='evaluate'`` raises on multi-table update
-----------------------------------------------------------------------------------

The "evaluator" for :meth:`.Query.update` won't work with multi-table
updates, and needs to be set to ``synchronize_session=False`` or
``synchronize_session='fetch'`` when multiple tables are present.
The new behavior is that an explicit exception is now raised, with a message
to change the synchronize setting.
This is upgraded from a warning emitted as of 0.9.7.

:ticket:`3117`

Resurrect Event has been Removed
--------------------------------

The "resurrect" ORM event has been removed entirely.  This event ceased to
have any function since version 0.8 removed the older "mutable" system
from the unit of work.


.. _migration_3177:

Change to single-table-inheritance criteria when using from_self(), count()
---------------------------------------------------------------------------

Given a single-table inheritance mapping, such as::

    class Widget(Base):
        __table__ = 'widget_table'

    class FooWidget(Widget):
        pass

Using :meth:`.Query.from_self` or :meth:`.Query.count` against a subclass
would produce a subquery, but then add the "WHERE" criteria for subtypes
to the outside::

    sess.query(FooWidget).from_self().all()

rendering::

    SELECT
        anon_1.widgets_id AS anon_1_widgets_id,
        anon_1.widgets_type AS anon_1_widgets_type
    FROM (SELECT widgets.id AS widgets_id, widgets.type AS widgets_type,
    FROM widgets) AS anon_1
    WHERE anon_1.widgets_type IN (?)

The issue with this is that if the inner query does not specify all
columns, then we can't add the WHERE clause on the outside (it actually tries,
and produces a bad query).  This decision
apparently goes way back to 0.6.5 with the note "may need to make more
adjustments to this".   Well, those adjustments have arrived!  So now the
above query will render::

    SELECT
        anon_1.widgets_id AS anon_1_widgets_id,
        anon_1.widgets_type AS anon_1_widgets_type
    FROM (SELECT widgets.id AS widgets_id, widgets.type AS widgets_type,
    FROM widgets
    WHERE widgets.type IN (?)) AS anon_1

So that queries that don't include "type" will still work!::

    sess.query(FooWidget.id).count()

Renders::

    SELECT count(*) AS count_1
    FROM (SELECT widgets.id AS widgets_id
    FROM widgets
    WHERE widgets.type IN (?)) AS anon_1


:ticket:`3177`


.. _migration_3222:


single-table-inheritance criteria added to all ON clauses unconditionally
-------------------------------------------------------------------------

When joining to a single-table inheritance subclass target, the ORM always adds
the "single table criteria" when joining on a relationship.  Given a
mapping as::

    class Widget(Base):
        __tablename__ = 'widget'
        id = Column(Integer, primary_key=True)
        type = Column(String)
        related_id = Column(ForeignKey('related.id'))
        related = relationship("Related", backref="widget")
        __mapper_args__ = {'polymorphic_on': type}


    class FooWidget(Widget):
        __mapper_args__ = {'polymorphic_identity': 'foo'}


    class Related(Base):
        __tablename__ = 'related'
        id = Column(Integer, primary_key=True)

It's been the behavior for quite some time that a JOIN on the relationship
will render a "single inheritance" clause for the type::

    s.query(Related).join(FooWidget, Related.widget).all()

SQL output::

    SELECT related.id AS related_id
    FROM related JOIN widget ON related.id = widget.related_id AND widget.type IN (:type_1)

Above, because we joined to a subclass ``FooWidget``, :meth:`.Query.join`
knew to add the ``AND widget.type IN ('foo')`` criteria to the ON clause.

The change here is that the ``AND widget.type IN()`` criteria is now appended
to *any* ON clause, not just those generated from a relationship,
including one that is explicitly stated::

    # ON clause will now render as
    # related.id = widget.related_id AND widget.type IN (:type_1)
    s.query(Related).join(FooWidget, FooWidget.related_id == Related.id).all()

As well as the "implicit" join when no ON clause of any kind is stated::

    # ON clause will now render as
    # related.id = widget.related_id AND widget.type IN (:type_1)
    s.query(Related).join(FooWidget).all()

Previously, the ON clause for these would not include the single-inheritance
criteria.  Applications that are already adding this criteria to work around
this will want to remove its explicit use, though it should continue to work
fine if the criteria happens to be rendered twice in the meantime.

.. seealso::

    :ref:`bug_3233`

:ticket:`3222`

Key Behavioral Changes - Core
=============================

.. _migration_2992:

Warnings emitted when coercing full SQL fragments into text()
-------------------------------------------------------------

Since SQLAlchemy's inception, there has always been an emphasis on not getting
in the way of the usage of plain text.   The Core and ORM expression systems
were intended to allow any number of points at which the user can just
use plain text SQL expressions, not just in the sense that you can send a
full SQL string to :meth:`.Connection.execute`, but that you can send strings
with SQL expressions into many functions, such as :meth:`.Select.where`,
:meth:`.Query.filter`, and :meth:`.Select.order_by`.

Note that by "SQL expressions" we mean a **full fragment of a SQL string**,
such as::

    # the argument sent to where() is a full SQL expression
    stmt = select([sometable]).where("somecolumn = 'value'")

and we are **not talking about string arguments**, that is, the normal
behavior of passing string values that become parameterized::

    # This is a normal Core expression with a string argument -
    # we aren't talking about this!!
    stmt = select([sometable]).where(sometable.c.somecolumn == 'value')

The Core tutorial has long featured an example of the use of this technique,
using a :func:`.select` construct where virtually all components of it
are specified as straight strings.  However, despite this long-standing
behavior and example, users are apparently surprised that this behavior
exists, and when asking around the community, I was unable to find any user
that was in fact *not* surprised that you can send a full string into a method
like :meth:`.Query.filter`.

So the change here is to encourage the user to qualify textual strings when
composing SQL that is partially or fully composed from textual fragments.
When composing a select as below::

    stmt = select(["a", "b"]).where("a = b").select_from("sometable")

The statement is built up normally, with all the same coercions as before.
However, one will see the following warnings emitted::

    SAWarning: Textual column expression 'a' should be explicitly declared
    with text('a'), or use column('a') for more specificity
    (this warning may be suppressed after 10 occurrences)

    SAWarning: Textual column expression 'b' should be explicitly declared
    with text('b'), or use column('b') for more specificity
    (this warning may be suppressed after 10 occurrences)

    SAWarning: Textual SQL expression 'a = b' should be explicitly declared
    as text('a = b') (this warning may be suppressed after 10 occurrences)

    SAWarning: Textual SQL FROM expression 'sometable' should be explicitly
    declared as text('sometable'), or use table('sometable') for more
    specificity (this warning may be suppressed after 10 occurrences)

These warnings attempt to show exactly where the issue is by displaying
the parameters as well as where the string was received.
The warnings make use of the :ref:`feature_3178` so that parameterized warnings
can be emitted safely without running out of memory, and as always, if
one wishes the warnings to be exceptions, the
`Python Warnings Filter <https://docs.python.org/2/library/warnings.html>`_
should be used::

    import warnings
    warnings.simplefilter("error")   # all warnings raise an exception

Given the above warnings, our statement works just fine, but
to get rid of the warnings we would rewrite our statement as follows::

    from sqlalchemy import select, text
    stmt = select([
            text("a"),
            text("b")
        ]).where(text("a = b")).select_from(text("sometable"))

and as the warnings suggest, we can give our statement more specificity
about the text if we use :func:`.column` and :func:`.table`::

    from sqlalchemy import select, text, column, table

    stmt = select([column("a"), column("b")]).\
        where(text("a = b")).select_from(table("sometable"))

Where note also that :func:`.table` and :func:`.column` can now
be imported from "sqlalchemy" without the "sql" part.

The behavior here applies to :func:`.select` as well as to key methods
on :class:`.Query`, including :meth:`.Query.filter`,
:meth:`.Query.from_statement` and :meth:`.Query.having`.

ORDER BY and GROUP BY are special cases
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is one case where usage of a string has special meaning, and as part
of this change we have enhanced its functionality.  When we have a
:func:`.select` or :class:`.Query` that refers to some column name or named
label, we might want to GROUP BY and/or ORDER BY known columns or labels::

    stmt = select([
        user.c.name,
        func.count(user.c.id).label("id_count")
    ]).group_by("name").order_by("id_count")

In the above statement we expect to see "ORDER BY id_count", as opposed to a
re-statement of the function.   The string argument given is actively
matched to an entry in the columns clause during compilation, so the above
statement would produce as we expect, without warnings (though note that
the ``"name"`` expression has been resolved to ``users.name``!)::

    SELECT users.name, count(users.id) AS id_count
    FROM users GROUP BY users.name ORDER BY id_count

However, if we refer to a name that cannot be located, then we get
the warning again, as below::

    stmt = select([
            user.c.name,
            func.count(user.c.id).label("id_count")
        ]).order_by("some_label")

The output does what we say, but again it warns us::

    SAWarning: Can't resolve label reference 'some_label'; converting to
    text() (this warning may be suppressed after 10 occurrences)

    SELECT users.name, count(users.id) AS id_count
    FROM users ORDER BY some_label

The above behavior applies to all those places where we might want to refer
to a so-called "label reference"; ORDER BY and GROUP BY, but also within an
OVER clause as well as a DISTINCT ON clause that refers to columns (e.g. the
PostgreSQL syntax).

We can still specify any arbitrary expression for ORDER BY or others using
:func:`.text`::

    stmt = select([users]).order_by(text("some special expression"))

The upshot of the whole change is that SQLAlchemy now would like us
to tell it when a string is sent that this string is explicitly
a :func:`.text` construct, or a column, table, etc., and if we use it as a
label name in an order by, group by, or other expression, SQLAlchemy expects
that the string resolves to something known, else it should again
be qualified with :func:`.text` or similar.

:ticket:`2992`

.. _bug_3288:

Python-side defaults invoked for each row invidually when using a multivalued insert
------------------------------------------------------------------------------------

Support for Python-side column defaults when using the multi-valued
version of :meth:`.Insert.values` were essentially not implemented, and
would only work "by accident" in specific situations, when the dialect in
use was using a non-positional (e.g. named) style of bound parameter, and
when it was not necessary that a Python-side callable be invoked for each
row.

The feature has been overhauled so that it works more similarly to
that of an "executemany" style of invocation::

    import itertools

    counter = itertools.count(1)
    t = Table(
        'my_table', metadata,
        Column('id', Integer, default=lambda: next(counter)),
        Column('data', String)
    )

    conn.execute(t.insert().values([
        {"data": "d1"},
        {"data": "d2"},
        {"data": "d3"},
    ]))

The above example will invoke ``next(counter)`` for each row individually
as would be expected::

    INSERT INTO my_table (id, data) VALUES (?, ?), (?, ?), (?, ?)
    (1, 'd1', 2, 'd2', 3, 'd3')

Previously, a positional dialect would fail as a bind would not be generated
for additional positions::

    Incorrect number of bindings supplied. The current statement uses 6,
    and there are 4 supplied.
    [SQL: u'INSERT INTO my_table (id, data) VALUES (?, ?), (?, ?), (?, ?)']
    [parameters: (1, 'd1', 'd2', 'd3')]

And with a "named" dialect, the same value for "id" would be re-used in
each row (hence this change is backwards-incompatible with a system that
relied on this)::

    INSERT INTO my_table (id, data) VALUES (:id, :data_0), (:id, :data_1), (:id, :data_2)
    {u'data_2': 'd3', u'data_1': 'd2', u'data_0': 'd1', 'id': 1}

The system will also refuse to invoke a "server side" default as inline-rendered
SQL, since it cannot be guaranteed that a server side default is compatible
with this.  If the VALUES clause renders for a specific column, then a Python-side
value is required; if an omitted value only refers to a server-side default,
an exception is raised::

    t = Table(
        'my_table', metadata,
        Column('id', Integer, primary_key=True),
        Column('data', String, server_default='some default')
    )

    conn.execute(t.insert().values([
        {"data": "d1"},
        {"data": "d2"},
        {},
    ]))

will raise::

    sqlalchemy.exc.CompileError: INSERT value for column my_table.data is
    explicitly rendered as a boundparameter in the VALUES clause; a
    Python-side value or SQL expression is required

Previously, the value "d1" would be copied into that of the third
row (but again, only with named format!)::

    INSERT INTO my_table (data) VALUES (:data_0), (:data_1), (:data_0)
    {u'data_1': 'd2', u'data_0': 'd1'}

:ticket:`3288`

.. _change_3163:

Event listeners can not be added or removed from within that event's runner
---------------------------------------------------------------------------

Removal of an event listener from inside that same event itself would
modify  the elements of a list during iteration, which would cause
still-attached event listeners to silently fail to fire.    To prevent
this while still maintaining performance, the lists have been replaced
with ``collections.deque()``, which does not allow any additions or
removals during iteration, and instead raises ``RuntimeError``.

:ticket:`3163`

.. _change_3169:

The INSERT...FROM SELECT construct now implies ``inline=True``
--------------------------------------------------------------

Using :meth:`.Insert.from_select` now implies ``inline=True``
on :func:`.insert`.  This helps to fix a bug where an
INSERT...FROM SELECT construct would inadvertently be compiled
as "implicit returning" on supporting backends, which would
cause breakage in the case of an INSERT that inserts zero rows
(as implicit returning expects a row), as well as arbitrary
return data in the case of an INSERT that inserts multiple
rows (e.g. only the first row of many).
A similar change is also applied to an INSERT..VALUES
with multiple parameter sets; implicit RETURNING will no longer emit
for this statement either.  As both of these constructs deal
with varible numbers of rows, the
:attr:`.ResultProxy.inserted_primary_key` accessor does not
apply.   Previously, there was a documentation note that one
may prefer ``inline=True`` with INSERT..FROM SELECT as some databases
don't support returning and therefore can't do "implicit" returning,
but there's no reason an INSERT...FROM SELECT needs implicit returning
in any case.   Regular explicit :meth:`.Insert.returning` should
be used to return variable numbers of result rows if inserted
data is needed.

:ticket:`3169`

.. _change_3027:

``autoload_with`` now implies ``autoload=True``
-----------------------------------------------

A :class:`.Table` can be set up for reflection by passing
:paramref:`.Table.autoload_with` alone::

    my_table = Table('my_table', metadata, autoload_with=some_engine)

:ticket:`3027`

.. _change_3266:

DBAPI exception wrapping and handle_error() event improvements
--------------------------------------------------------------

SQLAlchemy's wrapping of DBAPI exceptions was not taking place in the
case where a :class:`.Connection` object was invalidated, and then tried
to reconnect and encountered an error; this has been resolved.

Additionally, the recently added :meth:`.ConnectionEvents.handle_error`
event is now invoked for errors that occur upon initial connect, upon
reconnect, and when :func:`.create_engine` is used given a custom connection
function via :paramref:`.create_engine.creator`.

The :class:`.ExceptionContext` object has a new datamember
:attr:`.ExceptionContext.engine` that will always refer to the :class:`.Engine`
in use, in those cases when the :class:`.Connection` object is not available
(e.g. on initial connect).


:ticket:`3266`

.. _change_3243:

ForeignKeyConstraint.columns is now a ColumnCollection
------------------------------------------------------

:attr:`.ForeignKeyConstraint.columns` was previously a plain list
containing either strings or :class:`.Column` objects, depending on
how the :class:`.ForeignKeyConstraint` was constructed and whether it was
associated with a table.  The collection is now a :class:`.ColumnCollection`,
and is only initialized after the :class:`.ForeignKeyConstraint` is
associated with a :class:`.Table`.  A new accessor
:attr:`.ForeignKeyConstraint.column_keys`
is added to unconditionally return string keys for the local set of
columns regardless of how the object was constructed or its current
state.


.. _feature_3084:

MetaData.sorted_tables accessor is "deterministic"
-----------------------------------------------------

The sorting of tables resulting from the :attr:`.MetaData.sorted_tables`
accessor is "deterministic"; the ordering should be the same in all cases
regardless of Python hashing.   This is done by first sorting the tables
by name before passing them to the topological algorithm, which maintains
that ordering as it iterates.

Note that this change does **not** yet apply to the ordering applied
when emitting :meth:`.MetaData.create_all` or :meth:`.MetaData.drop_all`.

:ticket:`3084`

.. _bug_3170:

null(), false() and true() constants are no longer singletons
-------------------------------------------------------------

These three constants were changed to return a "singleton" value
in 0.9; unfortunately, that would lead to a query like the following
to not render as expected::

    select([null(), null()])

rendering only ``SELECT NULL AS anon_1``, because the two :func:`.null`
constructs would come out as the same  ``NULL`` object, and
SQLAlchemy's Core model is based on object identity in order to
determine lexical significance.    The change in 0.9 had no
importance other than the desire to save on object overhead; in general,
an unnamed construct needs to stay lexically unique so that it gets
labeled uniquely.

:ticket:`3170`

.. _change_3204:

SQLite/Oracle have distinct methods for temporary table/view name reporting
---------------------------------------------------------------------------

The :meth:`.Inspector.get_table_names` and :meth:`.Inspector.get_view_names`
methods in the case of SQLite/Oracle would also return the names of temporary
tables and views, which is not provided by any other dialect (in the case
of MySQL at least it is not even possible).  This logic has been moved
out to two new methods :meth:`.Inspector.get_temp_table_names` and
:meth:`.Inspector.get_temp_view_names`.

Note that reflection of a specific named temporary table or temporary view,
either by ``Table('name', autoload=True)`` or via methods like
:meth:`.Inspector.get_columns` continues to function for most if not all
dialects.   For SQLite specifically, there is a bug fix for UNIQUE constraint
reflection from temp tables as well, which is :ticket:`3203`.

:ticket:`3204`

Dialect Improvements and Changes - PostgreSQL
=============================================

.. _change_3319:

Overhaul of ENUM type create/drop rules
---------------------------------------

The rules for PostgreSQL :class:`.postgresql.ENUM` have been made more strict
with regards to creating and dropping of the TYPE.

An :class:`.postgresql.ENUM` that is created **without** being explicitly
associated with a :class:`.MetaData` object will be created *and* dropped
corresponding to :meth:`.Table.create` and :meth:`.Table.drop`::

    table = Table('sometable', metadata,
        Column('some_enum', ENUM('a', 'b', 'c', name='myenum'))
    )

    table.create(engine)  # will emit CREATE TYPE and CREATE TABLE
    table.drop(engine)  # will emit DROP TABLE and DROP TYPE - new for 1.0

This means that if a second table also has an enum named 'myenum', the
above DROP operation will now fail.    In order to accommodate the use case
of a common shared enumerated type, the behavior of a metadata-associated
enumeration has been enhanced.

An :class:`.postgresql.ENUM` that is created **with** being explicitly
associated with a :class:`.MetaData` object will *not* be created *or* dropped
corresponding to :meth:`.Table.create` and :meth:`.Table.drop`, with
the exception of :meth:`.Table.create` called with the ``checkfirst=True``
flag::

    my_enum = ENUM('a', 'b', 'c', name='myenum', metadata=metadata)

    table = Table('sometable', metadata,
        Column('some_enum', my_enum)
    )

    # will fail: ENUM 'my_enum' does not exist
    table.create(engine)

    # will check for enum and emit CREATE TYPE
    table.create(engine, checkfirst=True)

    table.drop(engine)  # will emit DROP TABLE, *not* DROP TYPE

    metadata.drop_all(engine) # will emit DROP TYPE

    metadata.create_all(engine) # will emit CREATE TYPE


:ticket:`3319`

New PostgreSQL Table options
-----------------------------

Added support for PG table options TABLESPACE, ON COMMIT,
WITH(OUT) OIDS, and INHERITS, when rendering DDL via
the :class:`.Table` construct.

.. seealso::

    :ref:`postgresql_table_options`

:ticket:`2051`

.. _feature_get_enums:

New get_enums() method with PostgreSQL Dialect
----------------------------------------------

The :func:`.inspect` method returns a :class:`.PGInspector` object in the
case of PostgreSQL, which includes a new :meth:`.PGInspector.get_enums`
method that returns information on all available ``ENUM`` types::

    from sqlalchemy import inspect, create_engine

    engine = create_engine("postgresql+psycopg2://host/dbname")
    insp = inspect(engine)
    print(insp.get_enums())

.. seealso::

    :meth:`.PGInspector.get_enums`

.. _feature_2891:

PostgreSQL Dialect reflects Materialized Views, Foreign Tables
--------------------------------------------------------------

Changes are as follows:

* the :class:`Table` construct with ``autoload=True`` will now match a name
  that exists in the database as a materialized view or foreign table.

* :meth:`.Inspector.get_view_names` will return plain and materialized view
  names.

* :meth:`.Inspector.get_table_names` does **not** change for PostgreSQL, it
  continues to return only the names of plain tables.

* A new method :meth:`.PGInspector.get_foreign_table_names` is added which
  will return the names of tables that are specifically marked as "foreign"
  in the PostgreSQL schema tables.

The change to reflection involves adding ``'m'`` and ``'f'`` to the list
of qualifiers we use when querying ``pg_class.relkind``, but this change
is new in 1.0.0 to avoid any backwards-incompatible surprises for those
running 0.9 in production.

:ticket:`2891`

.. _change_3264:

PostgreSQL ``has_table()`` now works for temporary tables
---------------------------------------------------------

This is a simple fix such that "has table" for temporary tables now works,
so that code like the following may proceed::

    from sqlalchemy import *

    metadata = MetaData()
    user_tmp = Table(
        "user_tmp", metadata,
        Column("id", INT, primary_key=True),
        Column('name', VARCHAR(50)),
        prefixes=['TEMPORARY']
    )

    e = create_engine("postgresql://scott:tiger@localhost/test", echo='debug')
    with e.begin() as conn:
        user_tmp.create(conn, checkfirst=True)

        # checkfirst will succeed
        user_tmp.create(conn, checkfirst=True)

The very unlikely case that this behavior will cause a non-failing application
to behave differently, is because PostgreSQL allows a non-temporary table
to silently overwrite a temporary table.  So code like the following will
now act completely differently, no longer creating the real table following
the temporary table::

    from sqlalchemy import *

    metadata = MetaData()
    user_tmp = Table(
        "user_tmp", metadata,
        Column("id", INT, primary_key=True),
        Column('name', VARCHAR(50)),
        prefixes=['TEMPORARY']
    )

    e = create_engine("postgresql://scott:tiger@localhost/test", echo='debug')
    with e.begin() as conn:
        user_tmp.create(conn, checkfirst=True)

        m2 = MetaData()
        user = Table(
            "user_tmp", m2,
            Column("id", INT, primary_key=True),
            Column('name', VARCHAR(50)),
        )

        # in 0.9, *will create* the new table, overwriting the old one.
        # in 1.0, *will not create* the new table
        user.create(conn, checkfirst=True)

:ticket:`3264`

.. _feature_gh134:

PostgreSQL FILTER keyword
-------------------------

The SQL standard FILTER keyword for aggregate functions is now supported
by PostgreSQL as of 9.4.  SQLAlchemy allows this using
:meth:`.FunctionElement.filter`::

    func.count(1).filter(True)

.. seealso::

    :meth:`.FunctionElement.filter`

    :class:`.FunctionFilter`

PG8000 dialect supports client side encoding
---------------------------------------------

The :paramref:`.create_engine.encoding` parameter is now honored
by the pg8000 dialect, using on connect handler which
emits ``SET CLIENT_ENCODING`` matching the selected encoding.

PG8000 native JSONB support
--------------------------------------

Support for PG8000 versions greater than 1.10.1 has been added, where
JSONB is supported natively.


Support for psycopg2cffi Dialect on Pypy
----------------------------------------

Support for the pypy psycopg2cffi dialect is added.

.. seealso::

    :mod:`sqlalchemy.dialects.postgresql.psycopg2cffi`

Dialect Improvements and Changes - MySQL
=============================================

.. _change_3155:

MySQL TIMESTAMP Type now renders NULL / NOT NULL in all cases
--------------------------------------------------------------

The MySQL dialect has always worked around MySQL's implicit NOT NULL
default associated with TIMESTAMP columns by emitting NULL for
such a type, if the column is set up with ``nullable=True``.   However,
MySQL 5.6.6 and above features a new flag
`explicit_defaults_for_timestamp <http://dev.mysql.com/doc/refman/
5.6/en/server-system-variables.html
#sysvar_explicit_defaults_for_timestamp>`_ which repairs MySQL's non-standard
behavior to make it behave like any other type; to accommodate this,
SQLAlchemy now emits NULL/NOT NULL unconditionally for all TIMESTAMP
columns.

.. seealso::

    :ref:`mysql_timestamp_null`

:ticket:`3155`


.. _change_3283:

MySQL SET Type Overhauled to support empty sets, unicode, blank value handling
-------------------------------------------------------------------------------

The :class:`.mysql.SET` type historically not included a system of handling
blank sets and empty values separately; as different drivers had different
behaviors for treatment of empty strings and empty-string-set representations,
the SET type tried only to hedge between these behaviors, opting to treat the
empty set as ``set([''])`` as is still the current behavior for the
MySQL-Connector-Python DBAPI.
Part of the rationale here was that it was otherwise impossible to actually
store a blank string within a MySQL SET, as the driver gives us back strings
with no way to discern between ``set([''])`` and ``set()``.  It was left
to the user to determine if ``set([''])`` actually meant "empty set" or not.

The new behavior moves the use case for the blank string, which is an unusual
case that isn't even documented in MySQL's documentation, into a special
case, and the default behavior of :class:`.mysql.SET` is now:

* to treat the empty string ``''`` as returned by MySQL-python into the empty
  set ``set()``;

* to convert the single-blank value set ``set([''])`` returned by
  MySQL-Connector-Python into the empty set ``set()``;

* To handle the case of a set type that actually wishes includes the blank
  value ``''`` in its list of possible values,
  a new feature (required in this use case) is implemented whereby the set
  value is persisted and loaded as a bitwise integer value; the
  flag :paramref:`.mysql.SET.retrieve_as_bitwise` is added in order to
  enable this.

Using the :paramref:`.mysql.SET.retrieve_as_bitwise` flag allows the set
to be persisted and retrieved with no ambiguity of values.   Theoretically
this flag can be turned on in all cases, as long as the given list of
values to the type matches the ordering exactly as declared in the
database; it only makes the SQL echo output a bit more unusual.

The default behavior of :class:`.mysql.SET` otherwise remains the same,
roundtripping values using strings.   The string-based behavior now
supports unicode fully including MySQL-python with use_unicode=0.

:ticket:`3283`


MySQL internal "no such table" exceptions not passed to event handlers
----------------------------------------------------------------------

The MySQL dialect will now disable :meth:`.ConnectionEvents.handle_error`
events from firing for those statements which it uses internally
to detect if a table exists or not.   This is achieved using an
execution option ``skip_user_error_events`` that disables the handle
error event for the scope of that execution.   In this way, user code
that rewrites exceptions doesn't need to worry about the MySQL
dialect or other dialects that occasionally need to catch
SQLAlchemy specific exceptions.


Changed the default value of ``raise_on_warnings`` for MySQL-Connector
----------------------------------------------------------------------

Changed the default value of "raise_on_warnings" to False for
MySQL-Connector.  This was set at True for some reason.  The "buffered"
flag unfortunately must stay at True as MySQLconnector does not allow
a cursor to be closed unless all results are fully fetched.

:ticket:`2515`

.. _bug_3186:

MySQL boolean symbols "true", "false" work again
------------------------------------------------

0.9's overhaul of the IS/IS NOT operators as well as boolean types in
:ticket:`2682` disallowed the MySQL dialect from making use of the
"true" and "false" symbols in the context of "IS" / "IS NOT".  Apparently,
even though MySQL has no "boolean" type, it supports IS / IS NOT when the
special "true" and "false" symbols are used, even though these are otherwise
synonymous with "1" and "0" (and IS/IS NOT don't work with the numerics).

So the change here is that the MySQL dialect remains "non native boolean",
but the :func:`.true` and :func:`.false` symbols again produce the
keywords "true" and "false", so that an expression like ``column.is_(true())``
again works on MySQL.

:ticket:`3186`

.. _change_3263:

The match() operator now returns an agnostic MatchType compatible with MySQL's floating point return value
----------------------------------------------------------------------------------------------------------

The return type of a :meth:`.ColumnOperators.match` expression is now a new type
called :class:`.MatchType`.  This is a subclass of :class:`.Boolean`,
that can be intercepted by the dialect in order to produce a different
result type at SQL execution time.

Code like the following will now function correctly and return floating points
on MySQL::

    >>> connection.execute(
    ...    select([
    ...        matchtable.c.title.match('Agile Ruby Programming').label('ruby'),
    ...        matchtable.c.title.match('Dive Python').label('python'),
    ...        matchtable.c.title
    ...    ]).order_by(matchtable.c.id)
    ... )
    [
        (2.0, 0.0, 'Agile Web Development with Ruby On Rails'),
        (0.0, 2.0, 'Dive Into Python'),
        (2.0, 0.0, "Programming Matz's Ruby"),
        (0.0, 0.0, 'The Definitive Guide to Django'),
        (0.0, 1.0, 'Python in a Nutshell')
    ]


:ticket:`3263`

.. _change_2984:

Drizzle Dialect is now an External Dialect
------------------------------------------

The dialect for `Drizzle <http://www.drizzle.org/>`_ is now an external
dialect, available at https://bitbucket.org/zzzeek/sqlalchemy-drizzle.
This dialect was added to SQLAlchemy right before SQLAlchemy was able to
accommodate third party dialects well; going forward, all databases that aren't
within the "ubiquitous use" category are third party dialects.
The dialect's implementation hasn't changed and is still based on the
MySQL + MySQLdb dialects within SQLAlchemy.  The dialect is as of yet
unreleased and in "attic" status; however it passes the majority of tests
and is generally in decent working order, if someone wants to pick up
on polishing it.

Dialect Improvements and Changes - SQLite
=============================================

SQLite named and unnamed UNIQUE and FOREIGN KEY constraints will inspect and reflect
-------------------------------------------------------------------------------------

UNIQUE and FOREIGN KEY constraints are now fully reflected on
SQLite both with and without names.  Previously, foreign key
names were ignored and unnamed unique constraints were skipped.   In particular
this will help with Alembic's new SQLite migration features.

To achieve this, for both foreign keys and unique constraints, the result
of PRAGMA foreign_keys, index_list, and index_info is combined with regular
expression parsing of the CREATE TABLE statement overall to form a complete
picture of the names of constraints, as well as differentiating UNIQUE
constraints that were created as UNIQUE vs. unnamed INDEXes.

:ticket:`3244`

:ticket:`3261`

Dialect Improvements and Changes - SQL Server
=============================================

.. _change_3182:

PyODBC driver name is required with hostname-based SQL Server connections
-------------------------------------------------------------------------

Connecting to SQL Server with PyODBC using a DSN-less connection, e.g.
with an explicit hostname, now requires a driver name - SQLAlchemy will no
longer attempt to guess a default::

    engine = create_engine("mssql+pyodbc://scott:tiger@myhost:port/databasename?driver=SQL+Server+Native+Client+10.0")

SQLAlchemy's previously hardcoded default of "SQL Server" is obsolete on
Windows, and SQLAlchemy cannot be tasked with guessing the best driver
based on operation system/driver detection.   Using a DSN is always preferred
when using ODBC to avoid this issue entirely.

:ticket:`3182`

SQL Server 2012 large text / binary types render as VARCHAR, NVARCHAR, VARBINARY
--------------------------------------------------------------------------------

The rendering of the :class:`.Text`, :class:`.UnicodeText`, and :class:`.LargeBinary`
types has been changed for SQL Server 2012 and greater, with options
to control the behavior completely, based on deprecation guidelines from
Microsoft.  See :ref:`mssql_large_type_deprecation` for details.

Dialect Improvements and Changes - Oracle
=============================================

.. _change_3220:

Improved support for CTEs in Oracle
-----------------------------------

CTE support has been fixed up for Oracle, and there is also a new feature
:meth:`.CTE.with_suffixes` that can assist with Oracle's special directives::

    included_parts = select([
        part.c.sub_part, part.c.part, part.c.quantity
    ]).where(part.c.part == "p1").\
        cte(name="included_parts", recursive=True).\
        suffix_with(
            "search depth first by part set ord1",
            "cycle part set y_cycle to 1 default 0", dialect='oracle')

:ticket:`3220`

New Oracle Keywords for DDL
-----------------------------

Keywords such as COMPRESS, ON COMMIT, BITMAP:

:ref:`oracle_table_options`

:ref:`oracle_index_options`
