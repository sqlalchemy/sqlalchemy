.. _query_api_toplevel:

Querying
========

.. module:: sqlalchemy.orm

The Query Object
----------------

:class:`~sqlalchemy.orm.query.Query` is produced in terms of a given :class:`~sqlalchemy.orm.session.Session`, using the :func:`~sqlalchemy.orm.query.Query.query` function::

    q = session.query(SomeMappedClass)

Following is the full interface for the :class:`Query` object.

.. autoclass:: sqlalchemy.orm.query.Query
   :members:
   :undoc-members:

ORM-Specific Query Constructs
-----------------------------

.. autoclass:: aliased

.. autoclass:: sqlalchemy.orm.util.AliasedClass

.. autofunction:: join

.. autofunction:: outerjoin

Query Options
-------------

Options which are passed to ``query.options()``, to affect the behavior of loading.

.. autofunction:: contains_alias

.. autofunction:: contains_eager

.. autofunction:: defer

.. autofunction:: eagerload

.. autofunction:: eagerload_all

.. autofunction:: extension

.. autofunction:: joinedload

.. autofunction:: joinedload_all

.. autofunction:: lazyload

.. autofunction:: subqueryload

.. autofunction:: subqueryload_all

.. autofunction:: undefer

