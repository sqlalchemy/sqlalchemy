.. _event_toplevel:

Events
======

SQLAlchemy includes an event API which publishes a wide variety of hooks into 
the internals of both SQLAlchemy Core and ORM.   The system is all new
as of version 0.7 and supercedes the previous system of "extension", "proxy", 
and "listener" classes.

Event Registration
------------------

Subscribing to an event occurs through a single API point, the :func:`.listen` function.   This function
accepts a user-defined listening function, a string identifier which identifies the event to be
intercepted, and a target.  Additional positional and keyword arguments may be supported by
specific types of events, which may specify alternate interfaces for the given event function, or provide
instructions regarding secondary event targets based on the given target.

The name of an event and the argument signature of a corresponding listener function is derived from 
a class bound specification method, which exists bound to a marker class that's described in the documentation.
For example, the documentation for :meth:`.PoolEvents.connect` indicates that the event name is ``"connect"``
and that a user-defined listener function should receive two positional arguments::

    from sqlalchemy.event import listen
    from sqlalchemy.pool import Pool

    def my_on_connect(dbapi_con, connection_record):
        print "New DBAPI connection:", dbapi_con

    listen(Pool, 'connect', my_on_connect)

Targets
-------

The :func:`.listen` function is very flexible regarding targets.  It generally accepts classes, instances of those
classes, and related classes or objects from which the appropriate target can be derived.  For example,
the above mentioned ``"connect"`` event accepts :class:`.Engine` classes and objects as well as :class:`.Pool`
classes and objects::

    from sqlalchemy.event import listen
    from sqlalchemy.pool import Pool, QueuePool
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Engine
    import psycopg2

    def connect():
        return psycopg2.connect(username='ed', host='127.0.0.1', dbname='test')

    my_pool = QueuePool(connect)
    my_engine = create_engine('postgresql://ed@localhost/test')

    # associate listener with all instances of Pool
    listen(Pool, 'connect', my_on_connect)

    # associate listener with all instances of Pool
    # via the Engine class
    listen(Engine, 'connect', my_on_connect)

    # associate listener with my_pool
    listen(my_pool, 'connect', my_on_connect)

    # associate listener with my_engine.pool
    listen(my_engine, 'connect', my_on_connect)

Modifiers
----------

Some listeners allow modifiers to be passed to :func:`.listen`.  These modifiers sometimes provide alternate
calling signatures for listeners.  Such as with ORM events, some event listeners can have a return value
which modifies the subsequent handling.   By default, no listener ever requires a return value, but by passing
``retval=True`` this value can be supported::

    def validate_phone(target, value, oldvalue, initiator):
        """Strip non-numeric characters from a phone number"""

        return re.sub(r'(?![0-9])', '', value)

    # setup listener on UserContact.phone attribute, instructing
    # it to use the return value
    listen(UserContact.phone, 'set', validate_phone, retval=True)

Event Reference
----------------

Both SQLAlchemy Core and SQLAlchemy ORM feature a wide variety of event hooks:

* **Core Events** - these are described in
  :ref:`core_event_toplevel` and include event hooks specific to
  connection pool lifecycle, SQL statement execution,
  transaction lifecycle, and schema creation and teardown.

* **ORM Events** - these are described in
  :ref:`orm_event_toplevel`, and include event hooks specific to
  class and attribute instrumentation, object initialization
  hooks, attribute on-change hooks, session state, flush, and
  commit hooks, mapper initialization, object/result population,
  and per-instance persistence hooks.

API Reference
-------------

.. autofunction:: sqlalchemy.event.listen

.. autofunction:: sqlalchemy.event.listens_for

