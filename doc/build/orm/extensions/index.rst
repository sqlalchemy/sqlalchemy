.. _plugins:
.. _sqlalchemy.ext:

ORM Extensions
==============

SQLAlchemy has a variety of ORM extensions available, which add additional
functionality to the core behavior.

The extensions build almost entirely on public core and ORM APIs and users should
be encouraged to read their source code to further their understanding of their
behavior.   In particular the "Horizontal Sharding", "Hybrid Attributes", and
"Mutation Tracking" extensions are very succinct.

.. toctree::
    :maxdepth: 1

    associationproxy
    automap
    baked
    declarative/index
    mutable
    orderinglist
    horizontal_shard
    hybrid
    instrumentation

