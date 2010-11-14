.. _dep_interfaces_orm_toplevel:

Deprecated ORM Event Interfaces
================================

.. module:: sqlalchemy.orm.interfaces

This section describes the class-based ORM event interface which first
existed in SQLAlchemy 0.1, which progressed with more kinds of events up
until SQLAlchemy 0.5.  The non-ORM analogue is described at :ref:`dep_interfaces_core_toplevel`.

As of SQLAlchemy 0.7, the new event system described in
:ref:`event_toplevel` replaces the extension/proxy/listener system, providing
a consistent interface to all events without the need for subclassing.

Mapper Events
-----------------

.. autoclass:: MapperExtension
    :members:

Session Events
-----------------

.. autoclass:: SessionExtension
    :members:

Attribute Events
--------------------

.. autoclass:: AttributeExtension
    :members:

