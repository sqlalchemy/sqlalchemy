.. _declarative_toplevel:

===========
Declarative
===========

The Declarative system is the typically used system provided by the SQLAlchemy
ORM in order to define classes mapped to relational database tables.  However,
as noted in :ref:`classical_mapping`, Declarative is in fact a series of
extensions that ride on top of the SQLAlchemy :func:`.mapper` construct.

While the documentation typically refers to Declarative for most examples,
the following sections will provide detailed information on how the
Declarative API interacts with the basic :func:`.mapper` and Core :class:`.Table`
systems, as well as how sophisticated patterns can be built using systems
such as mixins.


.. toctree::
	:maxdepth: 2

	basic_use
	relationships
	table_config
	inheritance
	mixins
	api





