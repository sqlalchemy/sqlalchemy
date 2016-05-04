=============================
What's new in SQLAlchemy 0.5?
=============================

.. admonition:: About this Document

    This document describes changes between SQLAlchemy version 0.4,
    last released October 12, 2008, and SQLAlchemy version 0.5,
    last released January 16, 2010.

    Document date: August 4, 2009


This guide documents API changes which affect users
migrating their applications from the 0.4 series of
SQLAlchemy to 0.5.   It's also recommended for those working
from  `Essential SQLAlchemy
<http://oreilly.com/catalog/9780596516147/>`_, which only
covers 0.4 and seems to even have some old 0.3isms in it.
Note that SQLAlchemy 0.5 removes many behaviors which were
deprecated throughout the span of the 0.4 series, and also
deprecates more behaviors specific to 0.4.

Major Documentation Changes
===========================

Some sections of the documentation have been completely
rewritten and can serve as an introduction to new ORM
features.  The ``Query`` and ``Session`` objects in
particular have some distinct differences in API and
behavior which fundamentally change many of the basic ways
things are done, particularly with regards to constructing
highly customized ORM queries and dealing with stale session
state, commits and rollbacks.

* `ORM Tutorial
  <http://www.sqlalchemy.org/docs/05/ormtutorial.html>`_

* `Session Documentation
  <http://www.sqlalchemy.org/docs/05/session.html>`_

Deprecations Source
===================

Another source of information is documented within a series
of unit tests illustrating up to date usages of some common
``Query`` patterns; this file can be viewed at
[source:sqlalchemy/trunk/test/orm/test_deprecations.py].

Requirements Changes
====================

* Python 2.4 or higher is required.  The SQLAlchemy 0.4 line
  is the last version with Python 2.3 support.

Object Relational Mapping
=========================

* **Column level expressions within Query.** - as detailed
  in the `tutorial
  <http://www.sqlalchemy.org/docs/05/ormtutorial.html>`_,
  ``Query`` has the capability to create specific SELECT
  statements, not just those against full rows:

  ::

      session.query(User.name, func.count(Address.id).label("numaddresses")).join(Address).group_by(User.name)

  The tuples returned by any multi-column/entity query are
  *named*' tuples:

  ::

      for row in session.query(User.name, func.count(Address.id).label('numaddresses')).join(Address).group_by(User.name):
         print("name", row.name, "number", row.numaddresses)

  ``Query`` has a ``statement`` accessor, as well as a
  ``subquery()`` method which allow ``Query`` to be used to
  create more complex combinations:

  ::

      subq = session.query(Keyword.id.label('keyword_id')).filter(Keyword.name.in_(['beans', 'carrots'])).subquery()
      recipes = session.query(Recipe).filter(exists().
         where(Recipe.id==recipe_keywords.c.recipe_id).
         where(recipe_keywords.c.keyword_id==subq.c.keyword_id)
      )

* **Explicit ORM aliases are recommended for aliased joins**
  - The ``aliased()`` function produces an "alias" of a
  class, which allows fine-grained control of aliases in
  conjunction with ORM queries.  While a table-level alias
  (i.e. ``table.alias()``) is still usable, an ORM level
  alias retains the semantics of the ORM mapped object which
  is significant for inheritance mappings, options, and
  other scenarios.  E.g.:

  ::

      Friend = aliased(Person)
      session.query(Person, Friend).join((Friend, Person.friends)).all()

* **query.join() greatly enhanced.** - You can now specify
  the target and ON clause for a join in multiple ways.   A
  target class alone can be provided where SQLA will attempt
  to form a join to it via foreign key in the same way as
  ``table.join(someothertable)``.  A target and an explicit
  ON condition can be provided, where the ON condition can
  be a ``relation()`` name, an actual class descriptor, or a
  SQL expression.  Or the old way of just a ``relation()``
  name or class descriptor works too.   See the ORM tutorial
  which has several examples.

* **Declarative is recommended for applications which don't
  require (and don't prefer) abstraction between tables and
  mappers** - The [/docs/05/reference/ext/declarative.html
  Declarative] module, which is used to combine the
  expression of ``Table``, ``mapper()``, and user defined
  class objects together, is highly recommended as it
  simplifies application configuration, ensures the "one
  mapper per class" pattern, and allows the full range of
  configuration available to distinct ``mapper()`` calls.
  Separate ``mapper()`` and ``Table`` usage is now referred
  to as "classical SQLAlchemy usage" and of course is freely
  mixable with declarative.

* **The .c. attribute has been removed** from classes (i.e.
  ``MyClass.c.somecolumn``).  As is the case in 0.4, class-
  level properties are usable as query elements, i.e.
  ``Class.c.propname`` is now superseded by
  ``Class.propname``, and the ``c`` attribute continues to
  remain on ``Table`` objects where they indicate the
  namespace of ``Column`` objects present on the table.

  To get at the Table for a mapped class (if you didn't keep
  it around already):

  ::

      table = class_mapper(someclass).mapped_table

  Iterate through columns:

  ::

      for col in table.c:
          print(col)

  Work with a specific column:

  ::

      table.c.somecolumn

  The class-bound descriptors support the full set of Column
  operators as well as the documented relation-oriented
  operators like ``has()``, ``any()``, ``contains()``, etc.

  The reason for the hard removal of ``.c.`` is that in 0.5,
  class-bound descriptors carry potentially different
  meaning, as well as information regarding class mappings,
  versus plain ``Column`` objects - and there are use cases
  where you'd specifically want to use one or the other.
  Generally, using class-bound descriptors invokes a set of
  mapping/polymorphic aware translations, and using table-
  bound columns does not.  In 0.4, these translations were
  applied across the board to all expressions, but 0.5
  differentiates completely between columns and mapped
  descriptors, only applying translations to the latter.  So
  in many cases, particularly when dealing with joined table
  inheritance configurations as well as when using
  ``query(<columns>)``, ``Class.propname`` and
  ``table.c.colname`` are not interchangeable.

  For example, ``session.query(users.c.id, users.c.name)``
  is different versus ``session.query(User.id, User.name)``;
  in the latter case, the ``Query`` is aware of the mapper
  in use and further mapper-specific operations like
  ``query.join(<propname>)``, ``query.with_parent()`` etc.
  may be used, but in the former case cannot.  Additionally,
  in polymorphic inheritance scenarios, the class-bound
  descriptors refer to the columns present in the
  polymorphic selectable in use, not necessarily the table
  column which directly corresponds to the descriptor.  For
  example, a set of classes related by joined-table
  inheritance to the ``person`` table along the
  ``person_id`` column of each table will all have their
  ``Class.person_id`` attribute mapped to the ``person_id``
  column in ``person``, and not their subclass table.
  Version 0.4 would map this behavior onto table-bound
  ``Column`` objects automatically.  In 0.5, this automatic
  conversion has been removed, so that you in fact *can* use
  table-bound columns as a means to override the
  translations which occur with polymorphic querying; this
  allows ``Query`` to be able to create optimized selects
  among joined-table or concrete-table inheritance setups,
  as well as portable subqueries, etc.

* **Session Now Synchronizes Automatically with
  Transactions.** Session now synchronizes against the
  transaction automatically by default, including autoflush
  and autoexpire.  A transaction is present at all times
  unless disabled using the ``autocommit`` option.  When all
  three flags are set to their default, the Session recovers
  gracefully after rollbacks and it's very difficult to get
  stale data into the session.  See the new Session
  documentation for details.

* **Implicit Order By Is Removed**.  This will impact ORM
  users who rely upon SA's "implicit ordering" behavior,
  which states that all Query objects which don't have an
  ``order_by()`` will ORDER BY the "id" or "oid" column of
  the primary mapped table, and all lazy/eagerly loaded
  collections apply a similar ordering.   In 0.5, automatic
  ordering must be explicitly configured on ``mapper()`` and
  ``relation()`` objects (if desired), or otherwise when
  using ``Query``.

  To convert an 0.4 mapping to 0.5, such that its ordering
  behavior will be extremely similar to 0.4 or previous, use
  the ``order_by`` setting on ``mapper()`` and
  ``relation()``:

  ::

          mapper(User, users, properties={
              'addresses':relation(Address, order_by=addresses.c.id)
          }, order_by=users.c.id)

  To set ordering on a backref, use the ``backref()``
  function:

  ::

          'keywords':relation(Keyword, secondary=item_keywords,
                order_by=keywords.c.name, backref=backref('items', order_by=items.c.id))

  Using declarative ?  To help with the new ``order_by``
  requirement, ``order_by`` and friends can now be set using
  strings which are evaluated in Python later on (this works
  **only** with declarative, not plain mappers):

  ::

          class MyClass(MyDeclarativeBase):
              ...
              'addresses':relation("Address", order_by="Address.id")

  It's generally a good idea to set ``order_by`` on
  ``relation()s`` which load list-based collections of
  items, since that ordering cannot otherwise be affected.
  Other than that, the best practice is to use
  ``Query.order_by()`` to control ordering of the primary
  entities being loaded.

* **Session is now
  autoflush=True/autoexpire=True/autocommit=False.** - To
  set it up, just call ``sessionmaker()`` with no arguments.
  The name ``transactional=True`` is now
  ``autocommit=False``.  Flushes occur upon each query
  issued (disable with ``autoflush=False``), within each
  ``commit()`` (as always), and before each
  ``begin_nested()`` (so rolling back to the SAVEPOINT is
  meaningful).   All objects are expired after each
  ``commit()`` and after each ``rollback()``.  After
  rollback, pending objects are expunged, deleted objects
  move back to persistent.  These defaults work together
  very nicely and there's really no more need for old
  techniques like ``clear()`` (which is renamed to
  ``expunge_all()`` as well).

  P.S.:  sessions are now reusable after a ``rollback()``.
  Scalar and collection attribute changes, adds and deletes
  are all rolled back.

* **session.add() replaces session.save(), session.update(),
  session.save_or_update().** - the
  ``session.add(someitem)`` and ``session.add_all([list of
  items])`` methods replace ``save()``, ``update()``, and
  ``save_or_update()``.  Those methods will remain
  deprecated throughout 0.5.

* **backref configuration made less verbose.** - The
  ``backref()`` function now uses the ``primaryjoin`` and
  ``secondaryjoin`` arguments of the forwards-facing
  ``relation()`` when they are not explicitly stated.  It's
  no longer necessary to specify
  ``primaryjoin``/``secondaryjoin`` in both directions
  separately.

* **Simplified polymorphic options.** - The ORM's
  "polymorphic load" behavior has been simplified.  In 0.4,
  mapper() had an argument called ``polymorphic_fetch``
  which could be configured as ``select`` or ``deferred``.
  This option is removed; the mapper will now just defer any
  columns which were not present in the SELECT statement.
  The actual SELECT statement used is controlled by the
  ``with_polymorphic`` mapper argument (which is also in 0.4
  and replaces ``select_table``), as well as the
  ``with_polymorphic()`` method on ``Query`` (also in 0.4).

  An improvement to the deferred loading of inheriting
  classes is that the mapper now produces the "optimized"
  version of the SELECT statement in all cases; that is, if
  class B inherits from A, and several attributes only
  present on class B have been expired, the refresh
  operation will only include B's table in the SELECT
  statement and will not JOIN to A.

* The ``execute()`` method on ``Session`` converts plain
  strings into ``text()`` constructs, so that bind
  parameters may all be specified as ":bindname" without
  needing to call ``text()`` explicitly.  If "raw" SQL is
  desired here, use ``session.connection().execute("raw
  text")``.

* ``session.Query().iterate_instances()`` has been renamed
  to just ``instances()``. The old ``instances()`` method
  returning a list instead of an iterator no longer exists.
  If you were relying on that behavior, you should use
  ``list(your_query.instances())``.

Extending the ORM
=================

In 0.5 we're moving forward with more ways to modify and
extend the ORM.  Heres a summary:

* **MapperExtension.** - This is the classic extension
  class, which remains.   Methods which should rarely be
  needed are ``create_instance()`` and
  ``populate_instance()``.  To control the initialization of
  an object when it's loaded from the database, use the
  ``reconstruct_instance()`` method, or more easily the
  ``@reconstructor`` decorator described in the
  documentation.

* **SessionExtension.** - This is an easy to use extension
  class for session events.  In particular, it provides
  ``before_flush()``, ``after_flush()`` and
  ``after_flush_postexec()`` methods.  This usage is
  recommended over ``MapperExtension.before_XXX`` in many
  cases since within ``before_flush()`` you can modify the
  flush plan of the session freely, something which cannot
  be done from within ``MapperExtension``.

* **AttributeExtension.** - This class is now part of the
  public API, and allows the interception of userland events
  on attributes, including attribute set and delete
  operations, and collection appends and removes.  It also
  allows the value to be set or appended to be modified.
  The ``@validates`` decorator, described in the
  documentation, provides a quick way to mark any mapped
  attributes as being "validated" by a particular class
  method.

* **Attribute Instrumentation Customization.** - An API is
  provided for ambitious efforts to entirely replace
  SQLAlchemy's attribute instrumentation, or just to augment
  it in some cases.  This API was produced for the purposes
  of the Trellis toolkit, but is available as a public API.
  Some examples are provided in the distribution in the
  ``/examples/custom_attributes`` directory.

Schema/Types
============

* **String with no length no longer generates TEXT, it
  generates VARCHAR** - The ``String`` type no longer
  magically converts into a ``Text`` type when specified
  with no length.  This only has an effect when CREATE TABLE
  is issued, as it will issue ``VARCHAR`` with no length
  parameter, which is not valid on many (but not all)
  databases.  To create a TEXT (or CLOB, i.e. unbounded
  string) column, use the ``Text`` type.

* **PickleType() with mutable=True requires an __eq__()
  method** - The ``PickleType`` type needs to compare values
  when mutable=True.  The method of comparing
  ``pickle.dumps()`` is inefficient and unreliable.  If an
  incoming object does not implement ``__eq__()`` and is
  also not ``None``, the ``dumps()`` comparison is used but
  a warning is raised.  For types which implement
  ``__eq__()`` which includes all dictionaries, lists, etc.,
  comparison will use ``==`` and is now reliable by default.

* **convert_bind_param() and convert_result_value() methods
  of TypeEngine/TypeDecorator are removed.** - The O'Reilly
  book unfortunately documented these methods even though
  they were deprecated post 0.3.   For a user-defined type
  which subclasses ``TypeEngine``, the ``bind_processor()``
  and ``result_processor()`` methods should be used for
  bind/result processing.  Any user defined type, whether
  extending ``TypeEngine`` or ``TypeDecorator``, which uses
  the old 0.3 style can be easily adapted to the new style
  using the following adapter:

  ::

      class AdaptOldConvertMethods(object):
          """A mixin which adapts 0.3-style convert_bind_param and
          convert_result_value methods

          """
          def bind_processor(self, dialect):
              def convert(value):
                  return self.convert_bind_param(value, dialect)
              return convert

          def result_processor(self, dialect):
              def convert(value):
                  return self.convert_result_value(value, dialect)
              return convert

          def convert_result_value(self, value, dialect):
              return value

          def convert_bind_param(self, value, dialect):
              return value

  To use the above mixin:

  ::

      class MyType(AdaptOldConvertMethods, TypeEngine):
         # ...

* The ``quote`` flag on ``Column`` and ``Table`` as well as
  the ``quote_schema`` flag on ``Table`` now control quoting
  both positively and negatively.  The default is ``None``,
  meaning let regular quoting rules take effect. When
  ``True``, quoting is forced on.  When ``False``, quoting
  is forced off.

* Column ``DEFAULT`` value DDL can now be more conveniently
  specified with ``Column(..., server_default='val')``,
  deprecating ``Column(..., PassiveDefault('val'))``.
  ``default=`` is now exclusively for Python-initiated
  default values, and can coexist with server_default.  A
  new ``server_default=FetchedValue()`` replaces the
  ``PassiveDefault('')`` idiom for marking columns as
  subject to influence from external triggers and has no DDL
  side effects.

* SQLite's ``DateTime``, ``Time`` and ``Date`` types now
  **only accept datetime objects, not strings** as bind
  parameter input.  If you'd like to create your own
  "hybrid" type which accepts strings and returns results as
  date objects (from whatever format you'd like), create a
  ``TypeDecorator`` that builds on ``String``.  If you only
  want string-based dates, just use ``String``.

* Additionally, the ``DateTime`` and ``Time`` types, when
  used with SQLite, now represent the "microseconds" field
  of the Python ``datetime.datetime`` object in the same
  manner as ``str(datetime)`` - as fractional seconds, not a
  count of microseconds.  That is:

  ::

       dt = datetime.datetime(2008, 6, 27, 12, 0, 0, 125)  # 125 usec

       # old way
       '2008-06-27 12:00:00.125'

       # new way
       '2008-06-27 12:00:00.000125'

  So if an existing SQLite file-based database intends to be
  used across 0.4 and 0.5, you either have to upgrade the
  datetime columns to store the new format (NOTE: please
  test this, I'm pretty sure its correct):

  ::

       UPDATE mytable SET somedatecol =
         substr(somedatecol, 0, 19) || '.' || substr((substr(somedatecol, 21, -1) / 1000000), 3, -1);

  or, enable "legacy" mode as follows:

  ::

       from sqlalchemy.databases.sqlite import DateTimeMixin
       DateTimeMixin.__legacy_microseconds__ = True

Connection Pool no longer threadlocal by default
================================================

0.4 has an unfortunate default setting of
"pool_threadlocal=True", leading to surprise behavior when,
for example, using multiple Sessions within a single thread.
This flag is now off in 0.5.   To re-enable 0.4's behavior,
specify ``pool_threadlocal=True`` to ``create_engine()``, or
alternatively use the "threadlocal" strategy via
``strategy="threadlocal"``.

\*args Accepted, \*args No Longer Accepted
==========================================

The policy with ``method(\*args)`` vs. ``method([args])``
is, if the method accepts a variable-length set of items
which represent a fixed structure, it takes ``\*args``.  If
the method accepts a variable-length set of items that are
data-driven, it takes ``[args]``.

* The various Query.options() functions ``eagerload()``,
  ``eagerload_all()``, ``lazyload()``, ``contains_eager()``,
  ``defer()``, ``undefer()`` all accept variable-length
  ``\*keys`` as their argument now, which allows a path to
  be formulated using descriptors, ie.:

  ::

         query.options(eagerload_all(User.orders, Order.items, Item.keywords))

  A single array argument is still accepted for backwards
  compatibility.

* Similarly, the ``Query.join()`` and ``Query.outerjoin()``
  methods accept a variable length \*args, with a single
  array accepted for backwards compatibility:

  ::

         query.join('orders', 'items')
         query.join(User.orders, Order.items)

* the ``in_()`` method on columns and similar only accepts a
  list argument now.  It no longer accepts ``\*args``.

Removed
=======

* **entity_name** - This feature was always problematic and
  rarely used.  0.5's more deeply fleshed out use cases
  revealed further issues with ``entity_name`` which led to
  its removal.  If different mappings are required for a
  single class, break the class into separate subclasses and
  map them separately.  An example of this is at
  [wiki:UsageRecipes/EntityName].  More information
  regarding rationale is described at http://groups.google.c
  om/group/sqlalchemy/browse_thread/thread/9e23a0641a88b96d?
  hl=en .

* **get()/load() cleanup**


  The ``load()`` method has been removed.  Its
  functionality was kind of arbitrary and basically copied
  from Hibernate, where it's also not a particularly
  meaningful method.

  To get equivalent functionality:

  ::

       x = session.query(SomeClass).populate_existing().get(7)

  ``Session.get(cls, id)`` and ``Session.load(cls, id)``
  have been removed.  ``Session.get()`` is redundant vs.
  ``session.query(cls).get(id)``.

  ``MapperExtension.get()`` is also removed (as is
  ``MapperExtension.load()``).  To override the
  functionality of ``Query.get()``, use a subclass:

  ::

       class MyQuery(Query):
           def get(self, ident):
               # ...

       session = sessionmaker(query_cls=MyQuery)()

       ad1 = session.query(Address).get(1)

* ``sqlalchemy.orm.relation()``


  The following deprecated keyword arguments have been
  removed:

  foreignkey, association, private, attributeext, is_backref

  In particular, ``attributeext`` is replaced with
  ``extension`` - the ``AttributeExtension`` class is now in
  the public API.

* ``session.Query()``


  The following deprecated functions have been removed:

  list, scalar, count_by, select_whereclause, get_by,
  select_by, join_by, selectfirst, selectone, select,
  execute, select_statement, select_text, join_to, join_via,
  selectfirst_by, selectone_by, apply_max, apply_min,
  apply_avg, apply_sum

  Additionally, the ``id`` keyword argument to ``join()``,
  ``outerjoin()``, ``add_entity()`` and ``add_column()`` has
  been removed.  To target table aliases in ``Query`` to
  result columns, use the ``aliased`` construct:

  ::

      from sqlalchemy.orm import aliased
      address_alias = aliased(Address)
      print(session.query(User, address_alias).join((address_alias, User.addresses)).all())

* ``sqlalchemy.orm.Mapper``


  * instances()


  * get_session() - this method was not very noticeable, but
    had the effect of associating lazy loads with a
    particular session even if the parent object was
    entirely detached, when an extension such as
    ``scoped_session()`` or the old ``SessionContextExt``
    was used.  It's possible that some applications which
    relied upon this behavior will no longer work as
    expected;  but the better programming practice here is
    to always ensure objects are present within sessions if
    database access from their attributes are required.

* ``mapper(MyClass, mytable)``


  Mapped classes no are longer instrumented with a "c" class
  attribute; e.g. ``MyClass.c``

* ``sqlalchemy.orm.collections``


  The _prepare_instrumentation alias for
  prepare_instrumentation has been removed.

* ``sqlalchemy.orm``


  Removed the ``EXT_PASS`` alias of ``EXT_CONTINUE``.

* ``sqlalchemy.engine``


  The alias from ``DefaultDialect.preexecute_sequences`` to
  ``.preexecute_pk_sequences`` has been removed.

  The deprecated engine_descriptors() function has been
  removed.

* ``sqlalchemy.ext.activemapper``


  Module removed.

* ``sqlalchemy.ext.assignmapper``


  Module removed.

* ``sqlalchemy.ext.associationproxy``


  Pass-through of keyword args on the proxy's
  ``.append(item, \**kw)`` has been removed and is now
  simply ``.append(item)``

* ``sqlalchemy.ext.selectresults``,
  ``sqlalchemy.mods.selectresults``

  Modules removed.

* ``sqlalchemy.ext.declarative``


  ``declared_synonym()`` removed.

* ``sqlalchemy.ext.sessioncontext``


  Module removed.

* ``sqlalchemy.log``


  The ``SADeprecationWarning`` alias to
  ``sqlalchemy.exc.SADeprecationWarning`` has been removed.

* ``sqlalchemy.exc``


  ``exc.AssertionError`` has been removed and usage replaced
  by the Python built-in of the same name.

* ``sqlalchemy.databases.mysql``


  The deprecated ``get_version_info`` dialect method has
  been removed.

Renamed or Moved
================

* ``sqlalchemy.exceptions`` is now ``sqlalchemy.exc``


  The module may still be imported under the old name until
  0.6.

* ``FlushError``, ``ConcurrentModificationError``,
  ``UnmappedColumnError`` -> sqlalchemy.orm.exc

  These exceptions moved to the orm package.  Importing
  'sqlalchemy.orm' will install aliases in sqlalchemy.exc
  for compatibility until 0.6.

* ``sqlalchemy.logging`` -> ``sqlalchemy.log``


  This internal module was renamed.  No longer needs to be
  special cased when packaging SA with py2app and similar
  tools that scan imports.

* ``session.Query().iterate_instances()`` ->
  ``session.Query().instances()``.

Deprecated
==========

* ``Session.save()``, ``Session.update()``,
  ``Session.save_or_update()``

  All three replaced by ``Session.add()``

* ``sqlalchemy.PassiveDefault``


  Use ``Column(server_default=...)`` Translates to
  sqlalchemy.DefaultClause() under the hood.

* ``session.Query().iterate_instances()``. It has been
  renamed to ``instances()``.

