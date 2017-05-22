.. module:: sqlalchemy.orm

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
to use, you can use the ``@reconstructor`` decorator to tag a method as the
ORM counterpart to ``__init__``. SQLAlchemy will call this method with no
arguments every time it loads or reconstructs one of your instances. This is
useful for recreating transient properties that are normally assigned in your
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

When ``obj = MyMappedClass()`` is executed, Python calls the ``__init__``
method as normal and the ``data`` argument is required.  When instances are
loaded during a :class:`~sqlalchemy.orm.query.Query` operation as in
``query(MyMappedClass).one()``, ``init_on_load`` is called.

Any method may be tagged as the :func:`~sqlalchemy.orm.reconstructor`, even
the ``__init__`` method. SQLAlchemy will call the reconstructor method with no
arguments. Scalar (non-collection) database-mapped attributes of the instance
will be available for use within the function. Eagerly-loaded collections are
generally not yet available and will usually only contain the first element.
ORM state changes made to objects at this stage will not be recorded for the
next flush() operation, so the activity within a reconstructor should be
conservative.

:func:`~sqlalchemy.orm.reconstructor` is a shortcut into a larger system
of "instance level" events, which can be subscribed to using the
event API - see :class:`.InstanceEvents` for the full API description
of these events.

.. autofunction:: reconstructor
