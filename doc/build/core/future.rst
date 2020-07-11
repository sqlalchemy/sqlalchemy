.. _core_future_toplevel:

SQLAlchemy 2.0 Future (Core)
============================

This package includes a relatively small number of transitional elements
to allow "2.0 mode" to take place within SQLAlchemy 1.4.   The primary
objects provided here are :class:`_future.Engine` and :class:`_future.Connection`,
which are both subclasses of the existing :class:`_engine.Engine` and
:class:`_engine.Connection` objects with essentially a smaller set of
methods and the removal of "autocommit".

Within the 1.4 series, the "2.0" style of engines and connections is enabled
by passing the :paramref:`_sa.create_engine.future` flag to
:func:`_sa.create_engine`::

    from sqlalchemy import create_engine
    engine = create_engine("postgresql://user:pass@host/dbname", future=True)

Similarly, with the ORM, to enable "future" behavior in the ORM :class:`.Session`,
pass the :paramref:`_orm.Session.future` parameter either to the
:class:`.Session` constructor directly, or via the :class:`_orm.sessionmaker`
class::

    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(engine, future=True)

.. seealso::

    :ref:`migration_20_toplevel` - Introduction to the 2.0 series of SQLAlchemy


.. module:: sqlalchemy.future

.. autoclass:: sqlalchemy.future.Connection
    :members:

.. autofunction:: sqlalchemy.future.create_engine

.. autoclass:: sqlalchemy.future.Engine
    :members:

.. autofunction:: sqlalchemy.future.select

