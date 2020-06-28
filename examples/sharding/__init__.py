"""A basic example of using the SQLAlchemy Sharding API.
Sharding refers to horizontally scaling data across multiple
databases.

The basic components of a "sharded" mapping are:

* multiple :class:`_engine.Engine` instances, each assigned a "shard id".
  These :class:`_engine.Engine` instances may refer to different databases,
  or different schemas / accounts within the same database, or they can
  even be differentiated only by options that will cause them to access
  different schemas or tables when used.

* a function which can return a single shard id, given an instance
  to be saved; this is called "shard_chooser"

* a function which can return a list of shard ids which apply to a particular
  instance identifier; this is called "id_chooser".If it returns all shard ids,
  all shards will be searched.

* a function which can return a list of shard ids to try, given a particular
  Query ("query_chooser").  If it returns all shard ids, all shards will be
  queried and the results joined together.

In these examples, different kinds of shards are used against the same basic
example which accommodates weather data on a per-continent basis. We provide
example shard_chooser, id_chooser and query_chooser functions. The
query_chooser illustrates inspection of the SQL expression element in order to
attempt to determine a single shard being requested.

The construction of generic sharding routines is an ambitious approach
to the issue of organizing instances among multiple databases.   For a
more plain-spoken alternative, the "distinct entity" approach
is a simple method of assigning objects to different tables (and potentially
database nodes) in an explicit way - described on the wiki at
`EntityName <http://www.sqlalchemy.org/trac/wiki/UsageRecipes/EntityName>`_.

.. autosource::

"""
