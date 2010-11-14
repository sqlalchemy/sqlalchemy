.. _orm_event_toplevel:

ORM Events
==========

The ORM includes a wide variety of hooks available for subscription.  The event
system in 0.7 is all new and supercedes the previous system of "extension" classes.
For an introduction to the event API, see :ref:`event_toplevel`.  Non-ORM events
such as those regarding connections and low-level statement execution are described in 
:ref:`core_event_toplevel`.

Attribute Events
----------------

.. autoclass:: sqlalchemy.orm.events.AttributeEvents
   :members:

Mapper Events
---------------

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

Instrumentation Events
-----------------------

.. autoclass:: sqlalchemy.orm.events.InstrumentationEvents
   :members:

Alternate Class Instrumentation
-------------------------------

.. autoclass:: sqlalchemy.orm.interfaces.InstrumentationManager
    :members:
    :undoc-members:


