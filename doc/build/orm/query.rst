.. _query_api_toplevel:

Querying
========

This section provides API documentation for the :class:`.Query` object and related constructs.

For an in-depth introduction to querying with the SQLAlchemy ORM, please see the :ref:`ormtutorial_toplevel`.


.. module:: sqlalchemy.orm

The Query Object
----------------

:class:`~.Query` is produced in terms of a given :class:`~.Session`, using the :func:`~.Query.query` function::

    q = session.query(SomeMappedClass)

Following is the full interface for the :class:`.Query` object.

.. autoclass:: sqlalchemy.orm.query.Query
   :members:
   :undoc-members:

ORM-Specific Query Constructs
-----------------------------

.. autofunction:: sqlalchemy.orm.aliased

.. autoclass:: sqlalchemy.orm.util.AliasedClass

.. autoclass:: sqlalchemy.orm.util.AliasedInsp

.. autoclass:: sqlalchemy.util.KeyedTuple
	:members: keys, _fields, _asdict

.. autofunction:: join

.. autofunction:: outerjoin

.. autofunction:: with_parent

