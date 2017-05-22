.. _orm_event_toplevel:

ORM Events
==========

The ORM includes a wide variety of hooks available for subscription.

For an introduction to the most commonly used ORM events, see the section
:ref:`session_events_toplevel`.   The event system in general is discussed
at :ref:`event_toplevel`.  Non-ORM events such as those regarding connections
and low-level statement execution are described in :ref:`core_event_toplevel`.

Attribute Events
----------------

.. autoclass:: sqlalchemy.orm.events.AttributeEvents
   :members:

Mapper Events
-------------

.. autoclass:: sqlalchemy.orm.events.MapperEvents
   :members:

Instance Events
---------------

.. autoclass:: sqlalchemy.orm.events.InstanceEvents
   :members:

Session Events
--------------

.. autoclass:: sqlalchemy.orm.events.SessionEvents
   :members:

Query Events
------------

.. autoclass:: sqlalchemy.orm.events.QueryEvents
   :members:

Instrumentation Events
----------------------

.. automodule:: sqlalchemy.orm.instrumentation

.. autoclass:: sqlalchemy.orm.events.InstrumentationEvents
   :members:

