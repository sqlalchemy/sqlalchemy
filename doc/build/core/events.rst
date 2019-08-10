.. _core_event_toplevel:

Core Events
===========

This section describes the event interfaces provided in
SQLAlchemy Core.
For an introduction to the event listening API, see :ref:`event_toplevel`.
ORM events are described in :ref:`orm_event_toplevel`.

.. autoclass:: sqlalchemy.event.base.Events
   :members:

Connection Pool Events
----------------------

.. autoclass:: sqlalchemy.events.PoolEvents
   :members:

.. _core_sql_events:

SQL Execution and Connection Events
-----------------------------------

.. autoclass:: sqlalchemy.events.ConnectionEvents
    :members:

.. autoclass:: sqlalchemy.events.DialectEvents
    :members:

Schema Events
-------------

.. autoclass:: sqlalchemy.events.DDLEvents
    :members:

.. autoclass:: sqlalchemy.events.SchemaEventTarget
    :members:

