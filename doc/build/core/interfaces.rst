.. _interfaces_core_toplevel:

Core Event Interfaces
======================

.. module:: sqlalchemy.interfaces

This section describes the various categories of events which can be intercepted
in SQLAlchemy core, including execution and connection pool events.

For ORM event documentation, see :ref:`interfaces_orm_toplevel`.

A new version of this API with a significantly more flexible and consistent
interface will be available in version 0.7.

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


