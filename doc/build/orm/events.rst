.. _orm_event_toplevel:

ORM Events
==========

The ORM includes a wide variety of hooks available for subscription.

For an introduction to the most commonly used ORM events, see the section
:ref:`session_events_toplevel`.   The event system in general is discussed
at :ref:`event_toplevel`.  Non-ORM events such as those regarding connections
and low-level statement execution are described in :ref:`core_event_toplevel`.

Session Events
--------------

The most basic event hooks are available at the level of the ORM
:class:`_orm.Session` object.   The types of things that are intercepted
here include:

* **Persistence Operations** - the ORM flush process that sends changes to the
  database can be extended using events that fire off at different parts of the
  flush, to augment or modify the data being sent to the database or to allow
  other things to happen when persistence occurs.   Read more about persistence
  events at :ref:`session_persistence_events`.

* **Object lifecycle events** - hooks when objects are added, persisted,
  deleted from sessions.   Read more about these at
  :ref:`session_lifecycle_events`.

* **Execution Events** - Part of the :term:`2.0 style` execution model, all
  SELECT statements against ORM entities emitted, as well as bulk UPDATE
  and DELETE statements outside of the flush process, are intercepted
  from the :meth:`_orm.Session.execute` method using the
  :meth:`_orm.SessionEvents.do_orm_execute` method.  Read more about this
  event at :ref:`session_execute_events`.

Be sure to read the :ref:`session_events_toplevel` chapter for context
on these events.

.. autoclass:: sqlalchemy.orm.SessionEvents
   :members:

Mapper Events
-------------

Mapper event hooks encompass things that happen as related to individual
or multiple :class:`_orm.Mapper` objects, which are the central configurational
object that maps a user-defined class to a :class:`_schema.Table` object.
Types of things which occur at the :class:`_orm.Mapper` level include:

* **Per-object persistence operations** - the most popular mapper hooks are the
  unit-of-work hooks such as :meth:`_orm.MapperEvents.before_insert`,
  :meth:`_orm.MapperEvents.after_update`, etc.  These events are contrasted to
  the more coarse grained session-level events such as
  :meth:`_orm.SessionEvents.before_flush` in that they occur within the flush
  process on a per-object basis; while finer grained activity on an object is
  more straightforward, availability of :class:`_orm.Session` features is
  limited.

* **Mapper configuration events** - the other major class of mapper hooks are
  those which occur as a class is mapped, as a mapper is finalized, and when
  sets of mappers are configured to refer to each other.  These events include
  :meth:`_orm.MapperEvents.instrument_class`,
  :meth:`_orm.MapperEvents.before_mapper_configured` and
  :meth:`_orm.MapperEvents.mapper_configured` at the individual
  :class:`_orm.Mapper` level, and  :meth:`_orm.MapperEvents.before_configured`
  and :meth:`_orm.MapperEvents.after_configured` at the level of collections of
  :class:`_orm.Mapper` objects.

.. autoclass:: sqlalchemy.orm.MapperEvents
   :members:

Instance Events
---------------

Instance events are focused on the construction of ORM mapped instances,
including when they are instantiated as :term:`transient` objects,
when they are loaded from the database and become :term:`persistent` objects,
as well as when database refresh or expiration operations occur on the object.

.. autoclass:: sqlalchemy.orm.InstanceEvents
   :members:



.. _orm_attribute_events:

Attribute Events
----------------

Attribute events are triggered as things occur on individual attributes of
ORM mapped objects.  These events form the basis for things like
:ref:`custom validation functions <simple_validators>` as well as
:ref:`backref handlers <relationships_backref>`.

.. seealso::

  :ref:`mapping_attributes_toplevel`

.. autoclass:: sqlalchemy.orm.AttributeEvents
   :members:


Query Events
------------

.. autoclass:: sqlalchemy.orm.QueryEvents
   :members:

Instrumentation Events
----------------------

.. automodule:: sqlalchemy.orm.instrumentation

.. autoclass:: sqlalchemy.orm.InstrumentationEvents
   :members:

