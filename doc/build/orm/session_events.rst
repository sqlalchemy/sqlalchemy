.. _session_events_toplevel:

Tracking queries, object and Session Changes with Events
=========================================================

SQLAlchemy features an extensive :ref:`Event Listening <event_toplevel>`
system used throughout the Core and ORM.   Within the ORM, there are a
wide variety of event listener hooks, which are documented at an API
level at :ref:`orm_event_toplevel`.   This collection of events has
grown over the years to include lots of very useful new events as well
as some older events that aren't as relevant as they once were.  This
section will attempt to introduce the major event hooks and when they
might be used.

.. _session_execute_events:

Execute Events
---------------

.. versionadded:: 1.4  The :class:`_orm.Session` now features a single
   comprehensive hook designed to intercept all SELECT statements made
   on behalf of the ORM as well as bulk UPDATE and DELETE statements.
   This hook supersedes the previous :meth:`_orm.QueryEvents.before_compile`
   event as well :meth:`_orm.QueryEvents.before_compile_update` and
   :meth:`_orm.QueryEvents.before_compile_delete`.

:class:`_orm.Session` features a comprehensive system by which all queries
invoked via the :meth:`_orm.Session.execute` method, which includes all
SELECT statements emitted by :class:`_orm.Query` as well as all SELECT
statements emitted on behalf of column and relationship loaders, may
be intercepted and modified.   The system makes use of the
:meth:`_orm.SessionEvents.do_orm_execute` event hook as well as the
:class:`_orm.ORMExecuteState` object to represent the event state.


Basic Query Interception
^^^^^^^^^^^^^^^^^^^^^^^^^

:meth:`_orm.SessionEvents.do_orm_execute` is firstly useful for any kind of
interception of a query, which includes those emitted by
:class:`_orm.Query` with :term:`1.x style` as well as when an ORM-enabled
:term:`2.0 style` :func:`_sql.select`,
:func:`_sql.update` or :func:`_sql.delete` construct is delivered to
:meth:`_orm.Session.execute`.   The :class:`_orm.ORMExecuteState` construct
provides accessors to allow modifications to statements, parameters, and
options::

    Session = sessionmaker(engine, future=True)

    @event.listens_for(Session, "do_orm_execute")
    def _do_orm_execute(orm_execute_state):
        if orm_execute_state.is_select:
            # add populate_existing for all SELECT statements

            orm_execute_state.update_execution_options(populate_existing=True)

            # check if the SELECT is against a certain entity and add an
            # ORDER BY if so
            col_descriptions = orm_execute_state.statement.column_descriptions

            if col_descriptions[0]['entity'] is MyEntity:
                orm_execute_state.statement = statement.order_by(MyEntity.name)

The above example illustrates some simple modifications to SELECT statements.
At this level, the :meth:`_orm.SessionEvents.do_orm_execute` event hook intends
to replace the previous use of the :meth:`_orm.QueryEvents.before_compile` event,
which was not fired off consistently for various kinds of loaders; additionally,
the :meth:`_orm.QueryEvents.before_compile` only applies to :term:`1.x style`
use with :class:`_orm.Query` and not with :term:`2.0 style` use of
:meth:`_orm.Session.execute`.


.. _do_orm_execute_global_criteria:

Adding global WHERE / ON criteria
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One of the most requested query-extension features is the ability to add WHERE
criteria to all occurrences of an entity in all queries.   This is achievable
by making use of the :func:`_orm.with_loader_criteria` query option, which
may be used on its own, or is ideally suited to be used within the
:meth:`_orm.SessionEvents.do_orm_execute` event::

    from sqlalchemy.orm import with_loader_criteria

    Session = sessionmaker(engine, future=True)

    @event.listens_for(Session, "do_orm_execute")
    def _do_orm_execute(orm_execute_state):

        if (
            orm_execute_state.is_select and
            not orm_execute_state.is_column_load and
            not orm_execute_state.is_relationship_load
        ):
            orm_execute_state.statement = orm_execute_state.statement.options(
                with_loader_criteria(MyEntity.public == True)
            )

Above, an option is added to all SELECT statements that will limit all queries
against ``MyEntity`` to filter on ``public == True``.   The criteria
will be applied to **all** loads of that class within the scope of the
immediate query.    The :func:`_orm.with_loader_criteria` option by default
will automatically propagate to relationship loaders as well, which will
apply to subsequent relationship loads, which includes
lazy loads, selectinloads, etc.

For a series of classes that all feature some common column structure,
if the classes are composed using a :ref:`declarative mixin <declarative_mixins>`,
the mixin class itself may be used in conjunction with the :func:`_orm.with_loader_criteria`
option by making use of a Python lambda.  The Python lambda will be invoked at
query compilation time against the specific entities which match the criteria.
Given a series of classes based on a mixin called ``HasTimestamp``::

    import datetime

    class HasTimestamp(object):
        timestamp = Column(DateTime, default=datetime.datetime.now)


    class SomeEntity(HasTimestamp, Base):
        __tablename__ = "some_entity"
        id = Column(Integer, primary_key=True)

    class SomeOtherEntity(HasTimestamp, Base):
        __tablename__ = "some_entity"
        id = Column(Integer, primary_key=True)


The above classes ``SomeEntity`` and ``SomeOtherEntity`` will each have a column
``timestamp`` that defaults to the current date and time.   An event may be used
to intercept all objects that extend from ``HasTimestamp`` and filter their
``timestamp`` column on a date that is no older than one month ago::

    @event.listens_for(Session, "do_orm_execute")
    def _do_orm_execute(orm_execute_state):
        if (
                orm_execute_state.is_select
                and not orm_execute_state.is_column_load
                and not orm_execute_state.is_relationship_load
        ):
            one_month_ago = datetime.datetime.today() - datetime.timedelta(months=1)

            orm_execute_state.statement = orm_execute_state.statement.options(
                with_loader_criteria(
                    HasTimestamp,
                    lambda cls: cls.timestamp >= one_month_ago,
                    include_aliases=True
                )
            )

.. warning:: The use of a lambda inside of the call to
   :func:`_orm.with_loader_criteria` is only invoked **once per unique class**.
   Custom functions should not be invoked within this lambda.   See
   :ref:`engine_lambda_caching` for an overview of the "lambda SQL" feature,
   which is for advanced use only.

.. seealso::

    :ref:`examples_session_orm_events` - includes working examples of the
    above :func:`_orm.with_loader_criteria` recipes.

.. _do_orm_execute_re_executing:

Re-Executing Statements
^^^^^^^^^^^^^^^^^^^^^^^

.. deepalchemy:: the statement re-execution feature involves a slightly
   intricate recursive sequence, and is intended to solve the fairly hard
   problem of being able to re-route the execution of a SQL statement into
   various non-SQL contexts.    The twin examples of "dogpile caching" and
   "horizontal sharding", linked below, should be used as a guide for when this
   rather advanced feature is appropriate to be used.

The :class:`_orm.ORMExecuteState` is capable of controlling the execution of
the given statement; this includes the ability to either not invoke the
statement at all, allowing a pre-constructed result set retrieved from a cache to
be returned instead, as well as the ability to invoke the same statement
repeatedly with different state, such as invoking it against multiple database
connections and then merging the results together in memory.   Both of these
advanced patterns are demonstrated in SQLAlchemy's example suite as detailed
below.

When inside the :meth:`_orm.SessionEvents.do_orm_execute` event hook, the
:meth:`_orm.ORMExecuteState.invoke_statement` method may be used to invoke
the statement using a new nested invocation of :meth:`_orm.Session.execute`,
which will then preempt the subsequent handling of the current execution
in progress and instead return the :class:`_engine.Result` returned by the
inner execution.   The event handlers thus far invoked for the
:meth:`_orm.SessionEvents.do_orm_execute` hook within this process will
be skipped within this nested call as well.

The :meth:`_orm.ORMExecuteState.invoke_statement` method returns a
:class:`_engine.Result` object; this object then features the ability for it to
be "frozen" into a cacheable format and "unfrozen" into a new
:class:`_engine.Result` object, as well as for its data to be merged with
that of other :class:`_engine.Result` objects.

E.g., using :meth:`_orm.SessionEvents.do_orm_execute` to implement a cache::

    from sqlalchemy.orm import loading

    cache = {}

    @event.listens_for(Session, "do_orm_execute")
    def _do_orm_execute(orm_execute_state):
        if "my_cache_key" in orm_execute_state.execution_options:
            cache_key = orm_execute_state.execution_options["my_cache_key"]

            if cache_key in cache:
                frozen_result = cache[cache_key]
            else:
                frozen_result = orm_execute_state.invoke_statement().freeze()
                cache[cache_key] = frozen_result

            return loading.merge_frozen_result(
                orm_execute_state.session,
                orm_execute_state.statement,
                frozen_result,
                load=False,
            )

With the above hook in place, an example of using the cache would look like::

    stmt = select(User).where(User.name == 'sandy').execution_options(my_cache_key="key_sandy")

    result = session.execute(stmt)

Above, a custom execution option is passed to
:meth:`_sql.Select.execution_options` in order to establish a "cache key" that
will then be intercepted by the :meth:`_orm.SessionEvents.do_orm_execute` hook.  This
cache key is then matched to a :class:`_engine.FrozenResult` object that may be
present in the cache, and if present, the object is re-used.  The recipe makes
use of the :meth:`_engine.Result.freeze` method to "freeze" a
:class:`_engine.Result` object, which above will contain ORM results, such that
it can be stored in a cache and used multiple times. In order to return a live
result from the "frozen" result, the :func:`_orm.loading.merge_frozen_result`
function is used to merge the "frozen" data from the result object into the
current session.

The above example is implemented as a complete example in :ref:`examples_caching`.

The :meth:`_orm.ORMExecuteState.invoke_statement` method may also be called
multiple times, passing along different information to the
:paramref:`_orm.ORMExecuteState.invoke_statement.bind_arguments` parameter such
that the :class:`_orm.Session` will make use of different
:class:`_engine.Engine` objects each time.  This will return a different
:class:`_engine.Result` object each time; these results can be merged together
using the :meth:`_engine.Result.merge` method.  This is the technique employed
by the :ref:`horizontal_sharding_toplevel` extension; see the source code to
familiarize.

.. seealso::

    :ref:`examples_caching`

    :ref:`examples_sharding`




.. _session_persistence_events:

Persistence Events
------------------

Probably the most widely used series of events are the "persistence" events,
which correspond to the :ref:`flush process<session_flushing>`.
The flush is where all the decisions are made about pending changes to
objects and are then emitted out to the database in the form of INSERT,
UPDATE, and DELETE statements.

``before_flush()``
^^^^^^^^^^^^^^^^^^

The :meth:`.SessionEvents.before_flush` hook is by far the most generally
useful event to use when an application wants to ensure that
additional persistence changes to the database are made when a flush proceeds.
Use :meth:`.SessionEvents.before_flush` in order to operate
upon objects to validate their state as well as to compose additional objects
and references before they are persisted.   Within this event,
it is **safe to manipulate the Session's state**, that is, new objects
can be attached to it, objects can be deleted, and individual attributes
on objects can be changed freely, and these changes will be pulled into
the flush process when the event hook completes.

The typical :meth:`.SessionEvents.before_flush` hook will be tasked with
scanning the collections :attr:`.Session.new`, :attr:`.Session.dirty` and
:attr:`.Session.deleted` in order to look for objects
where something will be happening.

For illustrations of :meth:`.SessionEvents.before_flush`, see
examples such as :ref:`examples_versioned_history` and
:ref:`examples_versioned_rows`.

``after_flush()``
^^^^^^^^^^^^^^^^^

The :meth:`.SessionEvents.after_flush` hook is called after the SQL has been
emitted for a flush process, but **before** the state of the objects that
were flushed has been altered.  That is, you can still inspect
the :attr:`.Session.new`, :attr:`.Session.dirty` and
:attr:`.Session.deleted` collections to see what was just flushed, and
you can also use history tracking features like the ones provided
by :class:`.AttributeState` to see what changes were just persisted.
In the :meth:`.SessionEvents.after_flush` event, additional SQL can be emitted
to the database based on what's observed to have changed.

``after_flush_postexec()``
^^^^^^^^^^^^^^^^^^^^^^^^^^

:meth:`.SessionEvents.after_flush_postexec` is called soon after
:meth:`.SessionEvents.after_flush`, but is invoked **after** the state of
the objects has been modified to account for the flush that just took place.
The :attr:`.Session.new`, :attr:`.Session.dirty` and
:attr:`.Session.deleted` collections are normally completely empty here.
Use :meth:`.SessionEvents.after_flush_postexec` to inspect the identity map
for finalized objects and possibly emit additional SQL.   In this hook,
there is the ability to make new changes on objects, which means the
:class:`.Session` will again go into a "dirty" state; the mechanics of the
:class:`.Session` here will cause it to flush **again** if new changes
are detected in this hook if the flush were invoked in the context of
:meth:`.Session.commit`; otherwise, the pending changes will be bundled
as part of the next normal flush.  When the hook detects new changes within
a :meth:`.Session.commit`, a counter ensures that an endless loop in this
regard is stopped after 100 iterations, in the case that an
:meth:`.SessionEvents.after_flush_postexec`
hook continually adds new state to be flushed each time it is called.

.. _session_persistence_mapper:

Mapper-level Events
^^^^^^^^^^^^^^^^^^^

In addition to the flush-level hooks, there is also a suite of hooks
that are more fine-grained, in that they are called on a per-object
basis and are broken out based on INSERT, UPDATE or DELETE.   These
are the mapper persistence hooks, and they too are very popular,
however these events need to be approached more cautiously, as they
proceed within the context of the flush process that is already
ongoing; many operations are not safe to proceed here.

The events are:

* :meth:`.MapperEvents.before_insert`
* :meth:`.MapperEvents.after_insert`
* :meth:`.MapperEvents.before_update`
* :meth:`.MapperEvents.after_update`
* :meth:`.MapperEvents.before_delete`
* :meth:`.MapperEvents.after_delete`

Each event is passed the :class:`_orm.Mapper`,
the mapped object itself, and the :class:`_engine.Connection` which is being
used to emit an INSERT, UPDATE or DELETE statement.     The appeal of these
events is clear, in that if an application wants to tie some activity to
when a specific type of object is persisted with an INSERT, the hook is
very specific; unlike the :meth:`.SessionEvents.before_flush` event,
there's no need to search through collections like :attr:`.Session.new`
in order to find targets.  However, the flush plan which
represents the full list of every single INSERT, UPDATE, DELETE statement
to be emitted has *already been decided* when these events are called,
and no changes may be made at this stage.  Therefore the only changes that are
even possible to the given objects are upon attributes **local** to the
object's row.   Any other change to the object or other objects will
impact the state of the :class:`.Session`, which will fail to function
properly.

Operations that are not supported within these mapper-level persistence
events include:

* :meth:`.Session.add`
* :meth:`.Session.delete`
* Mapped collection append, add, remove, delete, discard, etc.
* Mapped relationship attribute set/del events,
  i.e. ``someobject.related = someotherobject``

The reason the :class:`_engine.Connection` is passed is that it is encouraged that
**simple SQL operations take place here**, directly on the :class:`_engine.Connection`,
such as incrementing counters or inserting extra rows within log tables.
When dealing with the :class:`_engine.Connection`, it is expected that Core-level
SQL operations will be used; e.g. those described in :ref:`sqlexpression_toplevel`.

There are also many per-object operations that don't need to be handled
within a flush event at all.   The most common alternative is to simply
establish additional state along with an object inside its ``__init__()``
method, such as creating additional objects that are to be associated with
the new object.  Using validators as described in :ref:`simple_validators` is
another approach; these functions can intercept changes to attributes and
establish additional state changes on the target object in response to the
attribute change.   With both of these approaches, the object is in
the correct state before it ever gets to the flush step.

.. _session_lifecycle_events:

Object Lifecycle Events
-----------------------

Another use case for events is to track the lifecycle of objects.  This
refers to the states first introduced at :ref:`session_object_states`.

.. versionadded:: 1.1 added a system of events that intercept all possible
   state transitions of an object within the :class:`.Session`.

All the states above can be tracked fully with events.   Each event
represents a distinct state transition, meaning, the starting state
and the destination state are both part of what are tracked.   With the
exception of the initial transient event, all the events are in terms of
the :class:`.Session` object or class, meaning they can be associated either
with a specific :class:`.Session` object::

    from sqlalchemy import event
    from sqlalchemy.orm import Session

    session = Session()

    @event.listens_for(session, 'transient_to_pending')
    def object_is_pending(session, obj):
        print("new pending: %s" % obj)

Or with the :class:`.Session` class itself, as well as with a specific
:class:`.sessionmaker`, which is likely the most useful form::

    from sqlalchemy import event
    from sqlalchemy.orm import sessionmaker

    maker = sessionmaker()

    @event.listens_for(maker, 'transient_to_pending')
    def object_is_pending(session, obj):
        print("new pending: %s" % obj)

The listeners can of course be stacked on top of one function, as is
likely to be common.   For example, to track all objects that are
entering the persistent state::

        @event.listens_for(maker, "pending_to_persistent")
        @event.listens_for(maker, "deleted_to_persistent")
        @event.listens_for(maker, "detached_to_persistent")
        @event.listens_for(maker, "loaded_as_persistent")
        def detect_all_persistent(session, instance):
            print("object is now persistent: %s" % instance)

Transient
^^^^^^^^^

All mapped objects when first constructed start out as :term:`transient`.
In this state, the object exists alone and doesn't have an association with
any :class:`.Session`.   For this initial state, there's no specific
"transition" event since there is no :class:`.Session`, however if one
wanted to intercept when any transient object is created, the
:meth:`.InstanceEvents.init` method is probably the best event.  This
event is applied to a specific class or superclass.  For example, to
intercept all new objects for a particular declarative base::

    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy import event

    Base = declarative_base()

    @event.listens_for(Base, "init", propagate=True)
    def intercept_init(instance, args, kwargs):
        print("new transient: %s" % instance)


Transient to Pending
^^^^^^^^^^^^^^^^^^^^

The transient object becomes :term:`pending` when it is first associated
with a :class:`.Session` via the :meth:`.Session.add` or :meth:`.Session.add_all`
method.  An object may also become part of a :class:`.Session` as a result
of a :ref:`"cascade" <unitofwork_cascades>` from a referencing object that was
explicitly added.   The transient to pending transition is detectable using
the :meth:`.SessionEvents.transient_to_pending` event::

    @event.listens_for(sessionmaker, "transient_to_pending")
    def intercept_transient_to_pending(session, object_):
        print("transient to pending: %s" % object_)


Pending to Persistent
^^^^^^^^^^^^^^^^^^^^^

The :term:`pending` object becomes :term:`persistent` when a flush
proceeds and an INSERT statement takes place for the instance.  The object
now has an identity key.   Track pending to persistent with the
:meth:`.SessionEvents.pending_to_persistent` event::

    @event.listens_for(sessionmaker, "pending_to_persistent")
    def intercept_pending_to_persistent(session, object_):
        print("pending to persistent: %s" % object_)

Pending to Transient
^^^^^^^^^^^^^^^^^^^^

The :term:`pending` object can revert back to :term:`transient` if the
:meth:`.Session.rollback` method is called before the pending object
has been flushed, or if the :meth:`.Session.expunge` method is called
for the object before it is flushed.  Track pending to transient with the
:meth:`.SessionEvents.pending_to_transient` event::

    @event.listens_for(sessionmaker, "pending_to_transient")
    def intercept_pending_to_transient(session, object_):
        print("transient to pending: %s" % object_)

Loaded as Persistent
^^^^^^^^^^^^^^^^^^^^

Objects can appear in the :class:`.Session` directly in the :term:`persistent`
state when they are loaded from the database.   Tracking this state transition
is synonymous with tracking objects as they are loaded, and is synonymous
with using the :meth:`.InstanceEvents.load` instance-level event.  However, the
:meth:`.SessionEvents.loaded_as_persistent` event is provided as a
session-centric hook for intercepting objects as they enter the persistent
state via this particular avenue::

    @event.listens_for(sessionmaker, "loaded_as_persistent")
    def intercept_loaded_as_persistent(session, object_):
        print("object loaded into persistent state: %s" % object_)


Persistent to Transient
^^^^^^^^^^^^^^^^^^^^^^^

The persistent object can revert to the transient state if the
:meth:`.Session.rollback` method is called for a transaction where the
object was first added as pending.   In the case of the ROLLBACK, the
INSERT statement that made this object persistent is rolled back, and
the object is evicted from the :class:`.Session` to again become transient.
Track objects that were reverted to transient from
persistent using the :meth:`.SessionEvents.persistent_to_transient`
event hook::

    @event.listens_for(sessionmaker, "persistent_to_transient")
    def intercept_persistent_to_transient(session, object_):
        print("persistent to transient: %s" % object_)

Persistent to Deleted
^^^^^^^^^^^^^^^^^^^^^

The persistent object enters the :term:`deleted` state when an object
marked for deletion is deleted from the database within the flush
process.   Note that this is **not the same** as when the :meth:`.Session.delete`
method is called for a target object.   The :meth:`.Session.delete`
method only **marks** the object for deletion; the actual DELETE statement
is not emitted until the flush proceeds.  It is subsequent to the flush
that the "deleted" state is present for the target object.

Within the "deleted" state, the object is only marginally associated
with the :class:`.Session`.  It is not present in the identity map
nor is it present in the :attr:`.Session.deleted` collection that refers
to when it was pending for deletion.

From the "deleted" state, the object can go either to the detached state
when the transaction is committed, or back to the persistent state
if the transaction is instead rolled back.

Track the persistent to deleted transition with
:meth:`.SessionEvents.persistent_to_deleted`::

    @event.listens_for(sessionmaker, "persistent_to_deleted")
    def intercept_persistent_to_deleted(session, object_):
        print("object was DELETEd, is now in deleted state: %s" % object_)


Deleted to Detached
^^^^^^^^^^^^^^^^^^^

The deleted object becomes :term:`detached` when the session's transaction
is committed.  After the :meth:`.Session.commit` method is called, the
database transaction is final and the :class:`.Session` now fully discards
the deleted object and removes all associations to it.   Track
the deleted to detached transition using :meth:`.SessionEvents.deleted_to_detached`::

    @event.listens_for(sessionmaker, "deleted_to_detached")
    def intercept_deleted_to_detached(session, object_):
        print("deleted to detached: %s" % object_)


.. note::

    While the object is in the deleted state, the :attr:`.InstanceState.deleted`
    attribute, accessible using ``inspect(object).deleted``, returns True.  However
    when the object is detached, :attr:`.InstanceState.deleted` will again
    return False.  To detect that an object was deleted, regardless of whether
    or not it is detached, use the :attr:`.InstanceState.was_deleted`
    accessor.


Persistent to Detached
^^^^^^^^^^^^^^^^^^^^^^

The persistent object becomes :term:`detached` when the object is de-associated
with the :class:`.Session`, via the :meth:`.Session.expunge`,
:meth:`.Session.expunge_all`, or :meth:`.Session.close` methods.

.. note::

  An object may also become **implicitly detached** if its owning
  :class:`.Session` is dereferenced by the application and discarded due to
  garbage collection. In this case, **no event is emitted**.

Track objects as they move from persistent to detached using the
:meth:`.SessionEvents.persistent_to_detached` event::

    @event.listens_for(sessionmaker, "persistent_to_detached")
    def intercept_persistent_to_detached(session, object_):
        print("object became detached: %s" % object_)

Detached to Persistent
^^^^^^^^^^^^^^^^^^^^^^

The detached object becomes persistent when it is re-associated with a
session using the :meth:`.Session.add` or equivalent method.  Track
objects moving back to persistent from detached using the
:meth:`.SessionEvents.detached_to_persistent` event::

    @event.listens_for(sessionmaker, "detached_to_persistent")
    def intercept_detached_to_persistent(session, object_):
        print("object became persistent again: %s" % object_)


Deleted to Persistent
^^^^^^^^^^^^^^^^^^^^^

The :term:`deleted` object can be reverted to the :term:`persistent`
state when the transaction in which it was DELETEd was rolled back
using the :meth:`.Session.rollback` method.   Track deleted objects
moving back to the persistent state using the
:meth:`.SessionEvents.deleted_to_persistent` event::

    @event.listens_for(sessionmaker, "deleted_to_persistent")
    def intercept_deleted_to_persistent(session, object_):
        print("deleted to persistent: %s" % object_)

.. _session_transaction_events:

Transaction Events
------------------

Transaction events allow an application to be notified when transaction
boundaries occur at the :class:`.Session` level as well as when the
:class:`.Session` changes the transactional state on :class:`_engine.Connection`
objects.

* :meth:`.SessionEvents.after_transaction_create`,
  :meth:`.SessionEvents.after_transaction_end` - these events track the
  logical transaction scopes of the :class:`.Session` in a way that is
  not specific to individual database connections.  These events are
  intended to help with integration of transaction-tracking systems such as
  ``zope.sqlalchemy``.  Use these
  events when the application needs to align some external scope with the
  transactional scope of the :class:`.Session`.  These hooks mirror
  the "nested" transactional behavior of the :class:`.Session`, in that they
  track logical "subtransactions" as well as "nested" (e.g. SAVEPOINT)
  transactions.

* :meth:`.SessionEvents.before_commit`, :meth:`.SessionEvents.after_commit`,
  :meth:`.SessionEvents.after_begin`,
  :meth:`.SessionEvents.after_rollback`, :meth:`.SessionEvents.after_soft_rollback` -
  These events allow tracking of transaction events from the perspective
  of database connections.   :meth:`.SessionEvents.after_begin` in particular
  is a per-connection event; a :class:`.Session` that maintains more than
  one connection will emit this event for each connection individually
  as those connections become used within the current transaction.
  The rollback and commit events then refer to when the DBAPI connections
  themselves have received rollback or commit instructions directly.

Attribute Change Events
-----------------------

The attribute change events allow interception of when specific attributes
on an object are modified.  These events include :meth:`.AttributeEvents.set`,
:meth:`.AttributeEvents.append`, and :meth:`.AttributeEvents.remove`.  These
events are extremely useful, particularly for per-object validation operations;
however, it is often much more convenient to use a "validator" hook, which
uses these hooks behind the scenes; see :ref:`simple_validators` for
background on this.  The attribute events are also behind the mechanics
of backreferences.   An example illustrating use of attribute events
is in :ref:`examples_instrumentation`.




