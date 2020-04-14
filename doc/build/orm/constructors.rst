.. currentmodule:: sqlalchemy.orm

.. _mapping_constructors:

Constructors and Object Initialization
======================================

Mapping imposes no restrictions or requirements on the constructor
(``__init__``) method for the class. You are free to require any arguments for
the function that you wish, assign attributes to the instance that are unknown
to the ORM, and generally do anything else you would normally do when writing
a constructor for a Python class.

The SQLAlchemy ORM does not call ``__init__`` when recreating objects from
database rows. The ORM's process is somewhat akin to the Python standard
library's ``pickle`` module, invoking the low level ``__new__`` method and
then quietly restoring attributes directly on the instance rather than calling
``__init__``.

If you need to do some setup on database-loaded instances before they're ready
to use, there is an event hook known as :meth:`.InstanceEvents.load` which
can achieve this; it is also available via a class-specific decorator called
:func:`_orm.reconstructor`.   When using :func:`_orm.reconstructor`,
the mapper will invoke the decorated method with no
arguments every time it loads or reconstructs an instance of the
class. This is
useful for recreating transient properties that are normally assigned in
``__init__``::

    from sqlalchemy import orm

    class MyMappedClass(object):
        def __init__(self, data):
            self.data = data
            # we need stuff on all instances, but not in the database.
            self.stuff = []

        @orm.reconstructor
        def init_on_load(self):
            self.stuff = []

Above, when ``obj = MyMappedClass()`` is executed, the ``__init__`` constructor
is invoked normally and the ``data`` argument is required.  When instances are
loaded during a :class:`~sqlalchemy.orm.query.Query` operation as in
``query(MyMappedClass).one()``, ``init_on_load`` is called.

Any method may be tagged as the :func:`_orm.reconstructor`, even
the ``__init__`` method itself.    It is invoked after all immediate
column-level attributes are loaded as well as after eagerly-loaded scalar
relationships.  Eagerly loaded collections may be only partially populated
or not populated at all, depending on the kind of eager loading used.

ORM state changes made to objects at this stage will not be recorded for the
next flush operation, so the activity within a reconstructor should be
conservative.

:func:`_orm.reconstructor` is a shortcut into a larger system
of "instance level" events, which can be subscribed to using the
event API - see :class:`.InstanceEvents` for the full API description
of these events.

.. autofunction:: reconstructor
