==============================
What's New in SQLAlchemy 1.0?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.9,
    undergoing maintenance releases as of May, 2014,
    and SQLAlchemy version 1.0, as of yet unreleased.

    Document last updated: September 25, 2014

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.0,
and also documents changes which affect users migrating
their applications from the 0.9 series of SQLAlchemy to 1.0.

Please carefully review
:ref:`behavioral_changes_orm_10` and :ref:`behavioral_changes_core_10` for
potentially backwards-incompatible changes.


New Features
============

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


Behavioral Improvements
=======================

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

.. _feature_3178:

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

    class HasSomeAttribute(object):
        @declared_attr.cascading
        def some_id(cls):
            if has_inherited_table(cls):
                return Column(ForeignKey('myclass.id'), primary_key=True)
            else:
                return Column(Integer, primary_key=True)

            return Column('id', Integer, primary_key=True)

    class MyClass(HasSomeAttribute, Base):
        ""
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

    print sess.query(A, a1).order_by(a1.b)

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

.. _behavioral_changes_orm_10:

Behavioral Changes - ORM
========================

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
However, as :ticket:`3060` illustrates, there are some seldom edge cases
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


.. _migration_migration_deprecated_orm_events:

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

query.update() with ``synchronize_session='evaluate'`` raises on multi-table update
-----------------------------------------------------------------------------------

The "evaulator" for :meth:`.Query.update` won't work with multi-table
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


.. _behavioral_changes_core_10:

Behavioral Changes - Core
=========================

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
Postgresql syntax).

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



Dialect Changes
===============

.. _change_2051:

New Postgresql Table options
-----------------------------

Added support for PG table options TABLESPACE, ON COMMIT,
WITH(OUT) OIDS, and INHERITS, when rendering DDL via
the :class:`.Table` construct.

.. seealso::

    :ref:`postgresql_table_options`

:ticket:`2051`

.. _feature_get_enums:

New get_enums() method with Postgresql Dialect
----------------------------------------------

The :func:`.inspect` method returns a :class:`.PGInspector` object in the
case of Postgresql, which includes a new :meth:`.PGInspector.get_enums`
method that returns information on all available ``ENUM`` types::

    from sqlalchemy import inspect, create_engine

    engine = create_engine("postgresql+psycopg2://host/dbname")
    insp = inspect(engine)
    print(insp.get_enums())

.. seealso::

    :meth:`.PGInspector.get_enums`

.. _feature_2891:

Postgresql Dialect reflects Materialized Views, Foreign Tables
--------------------------------------------------------------

Changes are as follows:

* the :class:`Table` construct with ``autoload=True`` will now match a name
  that exists in the database as a materialized view or foriegn table.

* :meth:`.Inspector.get_view_names` will return plain and materialized view
  names.

* :meth:`.Inspector.get_table_names` does **not** change for Postgresql, it
  continues to return only the names of plain tables.

* A new method :meth:`.PGInspector.get_foreign_table_names` is added which
  will return the names of tables that are specifically marked as "foreign"
  in the Postgresql schema tables.

The change to reflection involves adding ``'m'`` and ``'f'`` to the list
of qualifiers we use when querying ``pg_class.relkind``, but this change
is new in 1.0.0 to avoid any backwards-incompatible surprises for those
running 0.9 in production.

:ticket:`2891`


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
