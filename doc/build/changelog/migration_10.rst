==============================
What's New in SQLAlchemy 1.0?
==============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.9,
    undergoing maintenance releases as of May, 2014,
    and SQLAlchemy version 1.0, as of yet unreleased.

    Document last updated: August 26, 2014

Introduction
============

This guide introduces what's new in SQLAlchemy version 1.0,
and also documents changes which affect users migrating
their applications from the 0.9 series of SQLAlchemy to 1.0.

Please carefully review
:ref:`behavioral_changes_orm_10` and :ref:`behavioral_changes_core_10` for
potentially backwards-incompatible changes.


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
	History(added=(), unchanged=[None], deleted=())	  # 0.9 and below

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
