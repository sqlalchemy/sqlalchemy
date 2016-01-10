.. _session_events_toplevel:

Tracking Object and Session Changes with Events
===============================================

SQLAlchemy features an extensive :ref:`Event Listening <event_toplevel>`
system used throughout the Core and ORM.   Within the ORM, there are a
wide variety of event listener hooks, which are documented at an API
level at :ref:`orm_event_toplevel`.   This collection of events has
grown over the years to include lots of very useful new events as well
as some older events that aren't as relevant as they once were.  This
section will attempt to introduce the major event hooks and when they
might be used.

.. _session_persistence_events:

Persistence Events
------------------

Probably the most widely used series of events are the "persistence" events,
which correspond to the :ref:`flush process<session_flushing>`.
The flush is where all the decisions are made about pending changes to
objects and are then emitted out to the database in the form of INSERT,
UPDATE, and DELETE staetments.

``before_flush()``
^^^^^^^^^^^^^^^^^^

The :meth:`.SessionEvents.before_flush` hook is by far the most generally
useful event to use when an application wants to ensure that
additional persistence changes to the database are made when a flush proceeds.
Use :meth:`.SessionEvents.before_flush` in order to operate
upon objects to validate their state as well as to compose additional objects
and references before they are persisted.   Within this event,
it is **safe to manipulate the Session's state**, that is, new objects
can be attached to it, objects can be deleted, and indivual attributes
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

Each event is passed the :class:`.Mapper`,
the mapped object itself, and the :class:`.Connection` which is being
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

The reason the :class:`.Connection` is passed is that it is encouraged that
**simple SQL operations take place here**, directly on the :class:`.Connection`,
such as incrementing counters or inserting extra rows within log tables.
When dealing with the :class:`.Connection`, it is expected that Core-level
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

As of SQLAlchemy 1.0, there is no direct event interface for tracking of
these states.  Events that can be used at the moment to track the state of
objects include:

* :meth:`.InstanceEvents.init`

* :meth:`.InstanceEvents.load`

* :meth:`.SessionEvents.before_attach`

* :meth:`.SessionEvents.after_attach`

* :meth:`.SessionEvents.before_flush` - by scanning the session's collections

* :meth:`.SessionEvents.after_flush` - by scanning the session's collections

SQLAlchemy 1.1 will introduce a comprehensive event system to track
the object persistence states fully and unambiguously.

.. _session_transaction_events:

Transaction Events
------------------

Transaction events allow an application to be notifed when transaction
boundaries occur at the :class:`.Session` level as well as when the
:class:`.Session` changes the transactional state on :class:`.Connection`
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




