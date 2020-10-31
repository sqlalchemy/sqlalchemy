===============================
Querying Data, Loading Objects
===============================

The following sections refer to techniques for emitting SELECT statements within
an ORM context.  This involves primarily statements that return instances of
ORM mapped objects, but also involves calling forms that deliver individual
column or groups of columns as well.

For an introduction to querying with the SQLAlchemy ORM, one of the
following tutorials shoud be consulted:

* :doc:`/tutorial/index` - for :term:`2.0 style` usage

* :doc:`/orm/tutorial` - for :term:`1.x style` usage.

As SQLAlchemy 1.4 represents a transition from 1.x to 2.0 style, the below
sections are currently mixed as far as which style they are using.

.. toctree::
    :maxdepth: 3

    queryguide
    loading_columns
    loading_relationships
    inheritance_loading
    constructors
    query
