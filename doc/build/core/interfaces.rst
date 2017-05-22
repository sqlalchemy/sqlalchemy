.. _dep_interfaces_core_toplevel:

Deprecated Event Interfaces
===========================

.. module:: sqlalchemy.interfaces

This section describes the class-based core event interface introduced in 
SQLAlchemy 0.5.  The ORM analogue is described at :ref:`dep_interfaces_orm_toplevel`.

.. deprecated:: 0.7
    The new event system described in :ref:`event_toplevel` replaces
    the extension/proxy/listener system, providing a consistent interface
    to all events without the need for subclassing.

Execution, Connection and Cursor Events
---------------------------------------

.. autoclass:: ConnectionProxy
   :members:
   :undoc-members:

Connection Pool Events
----------------------

.. autoclass:: PoolListener
   :members:
   :undoc-members:


