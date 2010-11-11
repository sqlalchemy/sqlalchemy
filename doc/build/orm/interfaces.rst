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

To use :class:`.MapperExtension`, make your own subclass of it and just send it off to a mapper::
    
    from sqlalchemy.orm.interfaces import MapperExtension
    
    class MyExtension(MapperExtension):
        def before_insert(self, mapper, connection, instance):
            print "instance %s before insert !" % instance
    
    m = mapper(User, users_table, extension=MyExtension())

Multiple extensions will be chained together and processed in order; they are specified as a list::

    m = mapper(User, users_table, extension=[ext1, ext2, ext3])

.. autoclass:: MapperExtension
    :members:

Session Events
-----------------

The :class:`.SessionExtension` applies plugin points for :class:`.Session` objects::

    from sqlalchemy.orm.interfaces import SessionExtension
    
    class MySessionExtension(SessionExtension):
        def before_commit(self, session):
            print "before commit!"

    Session = sessionmaker(extension=MySessionExtension())

The same :class:`~sqlalchemy.orm.interfaces.SessionExtension` instance can be
used with any number of sessions.

.. autoclass:: SessionExtension
    :members:

Attribute Events
--------------------

:class:`.AttributeExtension` is used to listen for set, remove, and append
events on individual mapped attributes. It is established on an individual
mapped attribute using the `extension` argument, available on
:func:`.column_property`, :func:`.relationship`, and others::

    from sqlalchemy.orm.interfaces import AttributeExtension
    from sqlalchemy.orm import mapper, relationship, column_property
    
    class MyAttrExt(AttributeExtension):
        def append(self, state, value, initiator):
            print "append event !"
            return value
        
        def set(self, state, value, oldvalue, initiator):
            print "set event !"
            return value
            
    mapper(SomeClass, sometable, properties={
        'foo':column_property(sometable.c.foo, extension=MyAttrExt()),
        'bar':relationship(Bar, extension=MyAttrExt())
    })

Note that the :class:`AttributeExtension` methods
:meth:`~.AttributeExtension.append` and :meth:`~.AttributeExtension.set` need
to return the ``value`` parameter. The returned value is used as the effective
value, and allows the extension to change what is ultimately persisted.

.. autoclass:: AttributeExtension
    :members:

Instrumentation Events and Re-implementation
---------------------------------------------

:class:`.InstrumentationManager` can be subclassed in order to receive class
instrumentation events as well as to change how class instrumentation
proceeds. This class exists for the purposes of integration with other object
management frameworks which would like to entirely modify the instrumentation
methodology of the ORM, and is not intended for regular usage. One possible
exception is the :meth:`.InstrumentationManager.post_configure_attribute`
method, which can be useful for adding extensions to all mapped attributes,
though a much better way to do this will be available in a future release of
SQLAlchemy.

For an example of :class:`.InstrumentationManager`, see the example
:ref:`examples_instrumentation`.

.. autoclass:: InstrumentationManager
    :members:
    :undoc-members:
