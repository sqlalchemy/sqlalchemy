.. highlight:: pycon+sql
.. |prev| replace:: :doc:`api`

.. |tutorial_title| replace:: ORM Querying Guide

.. topic:: |tutorial_title|

      This page is part of the :doc:`index`.

      Previous: |prev|


.. currentmodule:: sqlalchemy.orm

.. _query_api_toplevel:

================
Legacy Query API
================

.. admonition:: About the Legacy Query API


    This page contains the Python generated documentation for the
    :class:`_query.Query` construct, which for many years was the sole SQL
    interface when working with the SQLAlchemy ORM.  As of version 2.0, an all
    new way of working is now the standard approach, where the same
    :func:`_sql.select` construct that works for Core works just as well for the
    ORM, providing a consistent interface for building queries.

    For any application that is built on the SQLAlchemy ORM prior to the
    2.0 API, the :class:`_query.Query` API will usually represents the vast
    majority of database access code within an application, and as such the
    majority of the :class:`_query.Query` API is
    **not being removed from SQLAlchemy**.  The :class:`_query.Query` object
    behind the scenes now translates itself into a 2.0 style :func:`_sql.select`
    object when the :class:`_query.Query` object is executed, so it now is
    just a very thin adapter API.

    For a guide to migrating an application based on :class:`_query.Query`
    to 2.0 style, see :ref:`migration_20_query_usage`.

    For an introduction to writing SQL for ORM objects in the 2.0 style,
    start with the :ref:`unified_tutorial`.  Additional reference for 2.0 style
    querying is at :ref:`queryguide_toplevel`.

The Query Object
================

:class:`_query.Query` is produced in terms of a given :class:`~.Session`, using the :meth:`~.Session.query` method::

    q = session.query(SomeMappedClass)

Following is the full interface for the :class:`_query.Query` object.

.. autoclass:: sqlalchemy.orm.Query
   :members:
   :inherited-members:

ORM-Specific Query Constructs
=============================

This section has moved to :ref:`queryguide_additional`.
