.. highlight:: pycon+sql

.. |prev| replace:: :doc:`metadata`
.. |next| replace:: :doc:`data_insert`

.. include:: tutorial_nav_include.rst

.. rst-class:: core-header, orm-addin

.. _tutorial_working_with_data:

Working with Data
==================

In :ref:`tutorial_working_with_transactions`, we learned the basics of how to
interact with the Python DBAPI and its transactional state.  Then, in
:ref:`tutorial_working_with_metadata`, we learned how to represent database
tables, columns, and constraints within SQLAlchemy using the
:class:`_schema.MetaData` and related objects.  In this section we will combine
both concepts above to create, select and manipulate data within a relational
database.   Our interaction with the database is **always** in terms
of a transaction, even if we've set our database driver to use :ref:`autocommit
<dbapi_autocommit>` behind the scenes.

The components of this section are as follows:

* :ref:`tutorial_core_insert` - to get some data into the database, we introduce
  and demonstrate the Core :class:`_sql.Insert` construct.   INSERTs from an
  ORM perspective are described in the next section
  :ref:`tutorial_orm_data_manipulation`.

* :ref:`tutorial_selecting_data` - this section will describe in detail
  the :class:`_sql.Select` construct, which is the most commonly used object
  in SQLAlchemy.  The :class:`_sql.Select` construct emits SELECT statements
  for both Core and ORM centric applications and both use cases will be
  described here.   Additional ORM use cases are also noted in the later
  section :ref:`tutorial_select_relationships` as well as the
  :ref:`queryguide_toplevel`.

* :ref:`tutorial_core_update_delete` - Rounding out the INSERT and SELECTion
  of data, this section will describe from a Core perspective the use of the
  :class:`_sql.Update` and :class:`_sql.Delete` constructs.  ORM-specific
  UPDATE and DELETE is similarly described in the
  :ref:`tutorial_orm_data_manipulation` section.


.. toctree::
    :hidden:
    :maxdepth: 10

    data_insert
    data_select
    data_update
