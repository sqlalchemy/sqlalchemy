.. _core_event_toplevel:

Core Events
============

This section describes the event interfaces provided in SQLAlchemy Core.  For an introduction
to the event listening API, see :ref:`event_toplevel`.   ORM events are described in :ref:`orm_event_toplevel`.

Connection Pool Events
-----------------------

.. autoclass:: sqlalchemy.events.PoolEvents
   :members:

Connection Events
-----------------------

.. autoclass:: sqlalchemy.events.EngineEvents
    :members:

Schema Events
-----------------------

.. autoclass:: sqlalchemy.events.DDLEvents
    :members:

