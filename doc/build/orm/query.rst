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

.. class:: aliased

The public name of the :class:`.AliasedClass` class.

.. autoclass:: sqlalchemy.orm.util.AliasedClass

.. autofunction:: join

.. autofunction:: outerjoin

.. autofunction:: with_parent

