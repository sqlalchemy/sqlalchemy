.. _core_event_toplevel:

Core Events
============

This section describes the event interfaces provided in
SQLAlchemy Core.
For an introduction to the event listening API, see :ref:`event_toplevel`.
ORM events are described in :ref:`orm_event_toplevel`.

.. versionadded:: 0.7
    The event system supercedes the previous system of "extension", "listener",
    and "proxy" classes.

Connection Pool Events
-----------------------

.. autoclass:: sqlalchemy.events.PoolEvents
   :members:

SQL Execution and Connection Events
------------------------------------

.. autoclass:: sqlalchemy.events.ConnectionEvents
    :members:

Schema Events
-----------------------

.. autoclass:: sqlalchemy.events.DDLEvents
    :members:

.. autoclass:: sqlalchemy.events.SchemaEventTarget
    :members:

