.. currentmodule:: sqlalchemy.orm

.. _query_api_toplevel:

=========
Query API
=========

This section presents the API reference for the ORM :class:`_query.Query` object.  For a walkthrough
of how to use this object, see :ref:`ormtutorial_toplevel`.

The Query Object
================

:class:`_query.Query` is produced in terms of a given :class:`~.Session`, using the :meth:`~.Session.query` method::

    q = session.query(SomeMappedClass)

Following is the full interface for the :class:`_query.Query` object.

.. autoclass:: sqlalchemy.orm.query.Query
   :members:

   .. automethod:: sqlalchemy.orm.query.Query.prefix_with

   .. automethod:: sqlalchemy.orm.query.Query.suffix_with

   .. automethod:: sqlalchemy.orm.query.Query.with_hint

   .. automethod:: sqlalchemy.orm.query.Query.with_statement_hint

ORM-Specific Query Constructs
=============================

.. autofunction:: sqlalchemy.orm.aliased

.. autoclass:: sqlalchemy.orm.util.AliasedClass

.. autoclass:: sqlalchemy.orm.util.AliasedInsp

.. autoclass:: sqlalchemy.orm.util.Bundle
    :members:

.. autoclass:: sqlalchemy.orm.strategy_options.Load
    :members:

.. autofunction:: join

.. autofunction:: outerjoin

.. autofunction:: with_parent

