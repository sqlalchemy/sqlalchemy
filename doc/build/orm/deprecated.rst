.. module:: sqlalchemy.orm.interfaces

Deprecated ORM Interfaces
=========================

The event system described in :ref:`events` now supercedes the previous
"Extension" classes found in all versions of SQLAlchemy through version 0.7.
The classes here are deprecated.

MapperExtension
-----------------

MapperExtension is now superceded by the events described in :class:`.MapperEvents`.

To use MapperExtension, make your own subclass of it and just send it off to a mapper::

    m = mapper(User, users_table, extension=MyExtension())

Multiple extensions will be chained together and processed in order; they are specified as a list::

    m = mapper(User, users_table, extension=[ext1, ext2, ext3])

.. autoclass:: MapperExtension

SessionExtension
-----------------

SessionExtension is now superceded by the events described in :class:`.SessionEvents`.

The :class:`~sqlalchemy.orm.interfaces.SessionExtension` applies plugin points for :class:`.Session` objects::

    class MySessionExtension(SessionExtension):
        def before_commit(self, session):
            print "before commit!"

    Session = sessionmaker(extension=MySessionExtension())

The same :class:`~sqlalchemy.orm.interfaces.SessionExtension` instance can be
used with any number of sessions.

.. autoclass:: SessionExtension

AttributeExtension
--------------------

AttributeExtension is now superceded by the events described in :class:`.AttributeEvents`.

..autoclass:: AttributeExtension

