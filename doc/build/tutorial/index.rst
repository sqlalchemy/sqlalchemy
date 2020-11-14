.. |tutorial_title| replace:: SQLAlchemy 1.4 / 2.0 Tutorial
.. |next| replace:: :doc:`engine`

.. footer_topic:: |tutorial_title|

      Next Section: |next|

.. _unified_tutorial:

.. rst-class:: orm_core

=============================
SQLAlchemy 1.4 / 2.0 Tutorial
=============================

.. admonition:: About this document

    The new SQLAlchemy Tutorial is now integrated between Core and ORM and
    serves as a unified introduction to SQLAlchemy as a whole.   In the new
    :term:`2.0 style` of working, fully available in the :ref:`1.4 release
    <migration_14_toplevel>`, the ORM now uses Core-style querying with the
    :func:`_sql.select` construct, and transactional semantics between Core
    connections and ORM sessions are equivalent.   Take note of the blue
    border styles for each section, that will tell you how "ORM-ish" a
    particular topic is!

    Users who are already familiar with SQLAlchemy, and especially those
    looking to migrate existing applications to work under SQLAlchemy 2.0
    within the 1.4 transitional phase should check out the
    :ref:`migration_20_toplevel` document as well.

    For the newcomer, this document has a **lot** of detail, however by the
    end they will be considered an **Alchemist**.

SQLAlchemy is presented as two distinct APIs, one building on top of the other.
These APIs are known as **Core** and **ORM**.

.. container:: core-header

    **SQLAlchemy Core** is the foundational architecture for SQLAlchemy as a
    "database toolkit".  The library provides tools for managing connectivity
    to a database, interacting with database queries and results, and
    programmatic construction of SQL statements.

    Sections that have a **dark blue border on the right** will discuss
    concepts that are **primarily Core-only**; when using the ORM, these
    concepts are still in play but are less often explicit in user code.

.. container:: orm-header

    **SQLAlchemy ORM** builds upon the Core to provide optional **object
    relational mapping** capabilities.   The ORM provides an additional
    configuration layer allowing user-defined Python classes to be **mapped**
    to database tables and other constructs, as well as an object persistence
    mechanism known as the **Session**.   It then extends the Core-level
    SQL Expression Language to allow SQL queries to be composed and invoked
    in terms of user-defined objects.

    Sections that have a **light blue border on the left** will discuss
    concepts that are **primarily ORM-only**.  Core-only users
    can skip these.

.. container:: core-header, orm-dependency

    A section that has **both light and dark borders on both sides** will
    discuss a **Core concept that is also used explicitly with the ORM**.


Tutorial Overview
=================

The tutorial will present both concepts in the natural order that they
should be learned, first with a mostly-Core-centric approach and then
spanning out into a more ORM-centric concepts.

The major sections of this tutorial are as follows:

.. toctree::
    :hidden:
    :maxdepth: 10

    engine
    dbapi_transactions
    metadata
    data
    orm_data_manipulation
    orm_related_objects
    further_reading

* :ref:`tutorial_engine` - all SQLAlchemy applications start with an
  :class:`_engine.Engine` object; here's how to create one.

* :ref:`tutorial_working_with_transactions` - the usage API of the
  :class:`_engine.Engine` and it's related objects :class:`_engine.Connection`
  and :class:`_result.Result` are presented here. This content is Core-centric
  however ORM users will want to be familiar with at least the
  :class:`_result.Result` object.

* :ref:`tutorial_working_with_metadata` - SQLAlchemy's SQL abstractions as well
  as the ORM rely upon a system of defining database schema constructs as
  Python objects.   This section introduces how to do that from both a Core and
  an ORM perspective.

* :ref:`tutorial_working_with_data` - here we learn how to create, select,
  update and delete data in the database.   The so-called :term:`CRUD`
  operations here are given in terms of SQLAlchemy Core with links out towards
  their ORM counterparts.  The SELECT operation that is introduced in detail at
  :ref:`tutorial_selecting_data` applies equally well to Core and ORM.

* :ref:`tutorial_orm_data_manipulation` covers the persistence framework of the
  ORM; basically the ORM-centric ways to insert, update and delete, as well as
  how to handle transactions.

* :ref:`tutorial_orm_related_objects` introduces the concept of the
  :func:`_orm.relationship` construct and provides a brief overview
  of how it's used, with links to deeper documentation.

* :ref:`tutorial_further_reading` lists a series of major top-level
  documentation sections which fully documents the concepts introduced in this
  tutorial.


.. rst-class:: core-header, orm-dependency

Version Check
-------------

This tutorial is written using a system called `doctest
<https://docs.python.org/3/library/doctest.html>`_. All of the code excerpts
written with a ``>>>`` are actually run as part of SQLAlchemy's test suite, and
the reader is invited to work with the code examples given in real time with
their own Python interpreter.

If running the examples, it is advised that the reader perform quick check to
verify that we are on  **version 1.4** of SQLAlchemy:

.. sourcecode:: pycon+sql

    >>> import sqlalchemy
    >>> sqlalchemy.__version__  # doctest: +SKIP
    1.4.0

.. rst-class:: core-header, orm-dependency

A Note on the Future
---------------------

This tutorial describes a new API that's released in SQLAlchemy 1.4 known
as :term:`2.0 style`.   The purpose of the 2.0-style API is to provide forwards
compatibility with :ref:`SQLAlchemy 2.0 <migration_20_toplevel>`, which is
planned as the next generation of SQLAlchemy.

In order to provide the full 2.0 API, a new flag called ``future`` will be
used, which will be seen as the tutorial describes the :class:`_engine.Engine`
and :class:`_orm.Session` objects.   These flags fully enable 2.0-compatibility
mode and allow the code in the tutorial to proceed fully.  When using the
``future`` flag with the :func:`_sa.create_engine` function, the object
returned is a sublass of :class:`sqlalchemy.engine.Engine` described as
:class:`sqlalchemy.future.Engine`. This tutorial will be referring to
:class:`sqlalchemy.future.Engine`.





