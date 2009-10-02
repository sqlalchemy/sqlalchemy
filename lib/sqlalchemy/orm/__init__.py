# sqlalchemy/orm/__init__.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009 Michael Bayer mike_mp@zzzcomputing.com
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""
Functional constructs for ORM configuration.

See the SQLAlchemy object relational tutorial and mapper configuration
documentation for an overview of how this module is used.

"""

from sqlalchemy.orm import exc
from sqlalchemy.orm.mapper import (
     Mapper,
     _mapper_registry,
     class_mapper,
     )
from sqlalchemy.orm.interfaces import (
     EXT_CONTINUE,
     EXT_STOP,
     ExtensionOption,
     InstrumentationManager,
     MapperExtension,
     PropComparator,
     SessionExtension,
     AttributeExtension,
     )
from sqlalchemy.orm.util import (
     AliasedClass as aliased,
     Validator,
     join,
     object_mapper,
     outerjoin,
     polymorphic_union,
     with_parent,
     )
from sqlalchemy.orm.properties import (
     BackRef,
     ColumnProperty,
     ComparableProperty,
     CompositeProperty,
     RelationProperty,
     PropertyLoader,
     SynonymProperty,
     )
from sqlalchemy.orm import mapper as mapperlib
from sqlalchemy.orm.mapper import reconstructor, validates
from sqlalchemy.orm import strategies
from sqlalchemy.orm.query import AliasOption, Query
from sqlalchemy.sql import util as sql_util
from sqlalchemy.orm.session import Session as _Session
from sqlalchemy.orm.session import object_session, sessionmaker
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy import util as sa_util


__all__ = (
    'EXT_CONTINUE',
    'EXT_STOP',
    'InstrumentationManager',
    'MapperExtension',
    'AttributeExtension',
    'Validator',
    'PropComparator',
    'Query',
    'aliased',
    'backref',
    'class_mapper',
    'clear_mappers',
    'column_property',
    'comparable_property',
    'compile_mappers',
    'composite',
    'contains_alias',
    'contains_eager',
    'create_session',
    'defer',
    'deferred',
    'dynamic_loader',
    'eagerload',
    'eagerload_all',
    'extension',
    'join',
    'lazyload',
    'mapper',
    'noload',
    'object_mapper',
    'object_session',
    'outerjoin',
    'polymorphic_union',
    'reconstructor',
    'relation',
    'scoped_session',
    'sessionmaker',
    'synonym',
    'undefer',
    'undefer_group',
    'validates'
    )


def scoped_session(session_factory, scopefunc=None):
    """Provides thread-local management of Sessions.

    This is a front-end function to
    :class:`~sqlalchemy.orm.scoping.ScopedSession`.

    :param session_factory: a callable function that produces
      :class:`Session` instances, such as :func:`sessionmaker` or
      :func:`create_session`.

    :param scopefunc: optional, TODO

    :returns: an :class:`~sqlalchemy.orm.scoping.ScopedSession` instance

    Usage::

      Session = scoped_session(sessionmaker(autoflush=True))

    To instantiate a Session object which is part of the scoped context,
    instantiate normally::

      session = Session()

    Most session methods are available as classmethods from the scoped
    session::

      Session.commit()
      Session.close()

    To map classes so that new instances are saved in the current Session
    automatically, as well as to provide session-aware class attributes such
    as "query", use the `mapper` classmethod from the scoped session::

      mapper = Session.mapper
      mapper(Class, table, ...)

    """
    return ScopedSession(session_factory, scopefunc=scopefunc)

def create_session(bind=None, **kwargs):
    """Create a new :class:`~sqlalchemy.orm.session.Session`.

    :param bind: optional, a single Connectable to use for all
      database access in the created
      :class:`~sqlalchemy.orm.session.Session`.

    :param \*\*kwargs: optional, passed through to the
      :class:`Session` constructor.

    :returns: an :class:`~sqlalchemy.orm.session.Session` instance

    The defaults of create_session() are the opposite of that of
    :func:`sessionmaker`; ``autoflush`` and ``expire_on_commit`` are
    False, ``autocommit`` is True.  In this sense the session acts
    more like the "classic" SQLAlchemy 0.3 session with these.

    Usage::

      >>> from sqlalchemy.orm import create_session
      >>> session = create_session()

    It is recommended to use :func:`sessionmaker` instead of
    create_session().

    """
    if 'transactional' in kwargs:
        sa_util.warn_deprecated(
            "The 'transactional' argument to sessionmaker() is deprecated; "
            "use autocommit=True|False instead.")
        if 'autocommit' in kwargs:
            raise TypeError('Specify autocommit *or* transactional, not both.')
        kwargs['autocommit'] = not kwargs.pop('transactional')

    kwargs.setdefault('autoflush', False)
    kwargs.setdefault('autocommit', True)
    kwargs.setdefault('expire_on_commit', False)
    return _Session(bind=bind, **kwargs)

def relation(argument, secondary=None, **kwargs):
    """Provide a relationship of a primary Mapper to a secondary Mapper.

    This corresponds to a parent-child or associative table relationship.  The
    constructed class is an instance of :class:`RelationProperty`.

    A typical :func:`relation`::

       mapper(Parent, properties={
         'children': relation(Children)
       })

    :param argument:
      a class or :class:`Mapper` instance, representing the target of
      the relation.

    :param secondary:
      for a many-to-many relationship, specifies the intermediary
      table. The *secondary* keyword argument should generally only
      be used for a table that is not otherwise expressed in any class
      mapping. In particular, using the Association Object Pattern is
      generally mutually exclusive with the use of the *secondary*
      keyword argument.

    :param backref:
      indicates the string name of a property to be placed on the related
      mapper's class that will handle this relationship in the other
      direction. The other property will be created automatically 
      when the mappers are configured.  Can also be passed as a
      :func:`backref` object to control the configuration of the
      new relation.
      
    :param back_populates:
      Takes a string name and has the same meaning as ``backref``, 
      except the complementing property is **not** created automatically, 
      and instead must be configured explicitly on the other mapper.  The 
      complementing property should also indicate ``back_populates`` 
      to this relation to ensure proper functioning.

    :param cascade:
      a comma-separated list of cascade rules which determines how
      Session operations should be "cascaded" from parent to child.
      This defaults to ``False``, which means the default cascade
      should be used.  The default value is ``"save-update, merge"``.

      Available cascades are:

        ``save-update`` - cascade the "add()" operation (formerly
        known as save() and update())

        ``merge`` - cascade the "merge()" operation

        ``expunge`` - cascade the "expunge()" operation

        ``delete`` - cascade the "delete()" operation

        ``delete-orphan`` - if an item of the child's type with no
        parent is detected, mark it for deletion.  Note that this
        option prevents a pending item of the child's class from being
        persisted without a parent present.

        ``refresh-expire`` - cascade the expire() and refresh()
        operations

        ``all`` - shorthand for "save-update,merge, refresh-expire,
        expunge, delete"

    :param collection_class:
      a class or callable that returns a new list-holding object. will
      be used in place of a plain list for storing elements.

    :param comparator_factory:
      a class which extends :class:`RelationProperty.Comparator` which
      provides custom SQL clause generation for comparison operations.

    :param extension:
      an :class:`AttributeExtension` instance, or list of extensions,
      which will be prepended to the list of attribute listeners for
      the resulting descriptor placed on the class.  These listeners
      will receive append and set events before the operation
      proceeds, and may be used to halt (via exception throw) or
      change the value used in the operation.

    :param foreign_keys:

      a list of columns which are to be used as "foreign key" columns.
      this parameter should be used in conjunction with explicit
      ``primaryjoin`` and ``secondaryjoin`` (if needed) arguments, and
      the columns within the ``foreign_keys`` list should be present
      within those join conditions. Normally, ``relation()`` will
      inspect the columns within the join conditions to determine
      which columns are the "foreign key" columns, based on
      information in the ``Table`` metadata. Use this argument when no
      ForeignKey's are present in the join condition, or to override
      the table-defined foreign keys.

    :param join_depth:
      when non-``None``, an integer value indicating how many levels
      deep eagerload joins should be constructed on a self-referring
      or cyclical relationship.  The number counts how many times the
      same Mapper shall be present in the loading condition along a
      particular join branch.  When left at its default of ``None``,
      eager loads will automatically stop chaining joins when they
      encounter a mapper which is already higher up in the chain.

    :param lazy=(True|False|None|'dynamic'):
      specifies how the related items should be loaded. Values include:

      True - items should be loaded lazily when the property is first
             accessed.

      False - items should be loaded "eagerly" in the same query as
              that of the parent, using a JOIN or LEFT OUTER JOIN.

      None - no loading should occur at any time.  This is to support
             "write-only" attributes, or attributes which are
             populated in some manner specific to the application.

      'dynamic' - a ``DynaLoader`` will be attached, which returns a
                  ``Query`` object for all read operations.  The
                  dynamic- collection supports only ``append()`` and
                  ``remove()`` for write operations; changes to the
                  dynamic property will not be visible until the data
                  is flushed to the database.

    :param order_by:
      indicates the ordering that should be applied when loading these
      items.

    :param passive_deletes=False:
       Indicates loading behavior during delete operations.

       A value of True indicates that unloaded child items should not
       be loaded during a delete operation on the parent.  Normally,
       when a parent item is deleted, all child items are loaded so
       that they can either be marked as deleted, or have their
       foreign key to the parent set to NULL.  Marking this flag as
       True usually implies an ON DELETE <CASCADE|SET NULL> rule is in
       place which will handle updating/deleting child rows on the
       database side.

       Additionally, setting the flag to the string value 'all' will
       disable the "nulling out" of the child foreign keys, when there
       is no delete or delete-orphan cascade enabled.  This is
       typically used when a triggering or error raise scenario is in
       place on the database side.  Note that the foreign key
       attributes on in-session child objects will not be changed
       after a flush occurs so this is a very special use-case
       setting.

    :param passive_updates=True:
      Indicates loading and INSERT/UPDATE/DELETE behavior when the
      source of a foreign key value changes (i.e. an "on update"
      cascade), which are typically the primary key columns of the
      source row.

      When True, it is assumed that ON UPDATE CASCADE is configured on
      the foreign key in the database, and that the database will
      handle propagation of an UPDATE from a source column to
      dependent rows.  Note that with databases which enforce
      referential integrity (i.e. PostgreSQL, MySQL with InnoDB tables),
      ON UPDATE CASCADE is required for this operation.  The
      relation() will update the value of the attribute on related
      items which are locally present in the session during a flush.

      When False, it is assumed that the database does not enforce
      referential integrity and will not be issuing its own CASCADE
      operation for an update.  The relation() will issue the
      appropriate UPDATE statements to the database in response to the
      change of a referenced key, and items locally present in the
      session during a flush will also be refreshed.

      This flag should probably be set to False if primary key changes
      are expected and the database in use doesn't support CASCADE
      (i.e. SQLite, MySQL MyISAM tables).

    :param post_update:
      this indicates that the relationship should be handled by a
      second UPDATE statement after an INSERT or before a
      DELETE. Currently, it also will issue an UPDATE after the
      instance was UPDATEd as well, although this technically should
      be improved. This flag is used to handle saving bi-directional
      dependencies between two individual rows (i.e. each row
      references the other), where it would otherwise be impossible to
      INSERT or DELETE both rows fully since one row exists before the
      other. Use this flag when a particular mapping arrangement will
      incur two rows that are dependent on each other, such as a table
      that has a one-to-many relationship to a set of child rows, and
      also has a column that references a single child row within that
      list (i.e. both tables contain a foreign key to each other). If
      a ``flush()`` operation returns an error that a "cyclical
      dependency" was detected, this is a cue that you might want to
      use ``post_update`` to "break" the cycle.

    :param primaryjoin:
      a ClauseElement that will be used as the primary join of this
      child object against the parent object, or in a many-to-many
      relationship the join of the primary object to the association
      table. By default, this value is computed based on the foreign
      key relationships of the parent and child tables (or association
      table).

    :param remote_side:
      used for self-referential relationships, indicates the column or
      list of columns that form the "remote side" of the relationship.

    :param secondaryjoin:
      a ClauseElement that will be used as the join of an association
      table to the child object. By default, this value is computed
      based on the foreign key relationships of the association and
      child tables.

    :param single_parent=(True|False):
      when True, installs a validator which will prevent objects
      from being associated with more than one parent at a time.
      This is used for many-to-one or many-to-many relationships that
      should be treated either as one-to-one or one-to-many.  Its
      usage is optional unless delete-orphan cascade is also 
      set on this relation(), in which case its required (new in 0.5.2).
      
    :param uselist=(True|False):
      a boolean that indicates if this property should be loaded as a
      list or a scalar. In most cases, this value is determined
      automatically by ``relation()``, based on the type and direction
      of the relationship - one to many forms a list, many to one
      forms a scalar, many to many is a list. If a scalar is desired
      where normally a list would be present, such as a bi-directional
      one-to-one relationship, set uselist to False.

    :param viewonly=False:
      when set to True, the relation is used only for loading objects
      within the relationship, and has no effect on the unit-of-work
      flush process.  Relationships with viewonly can specify any kind of
      join conditions to provide additional views of related objects
      onto a parent object. Note that the functionality of a viewonly
      relationship has its limits - complicated join conditions may
      not compile into eager or lazy loaders properly. If this is the
      case, use an alternative method.

    """
    return RelationProperty(argument, secondary=secondary, **kwargs)

def dynamic_loader(argument, secondary=None, primaryjoin=None,
                   secondaryjoin=None, foreign_keys=None, backref=None,
                   post_update=False, cascade=False, remote_side=None,
                   enable_typechecks=True, passive_deletes=False,
                   order_by=None, comparator_factory=None, query_class=None):
    """Construct a dynamically-loading mapper property.

    This property is similar to :func:`relation`, except read
    operations return an active :class:`Query` object which reads from
    the database when accessed.  Items may be appended to the
    attribute via ``append()``, or removed via ``remove()``; changes
    will be persisted to the database during a :meth:`Sesion.flush`.
    However, no other Python list or collection mutation operations
    are available.

    A subset of arguments available to :func:`relation` are available
    here.

    :param argument:
      a class or :class:`Mapper` instance, representing the target of
      the relation.

    :param secondary:
      for a many-to-many relationship, specifies the intermediary
      table. The *secondary* keyword argument should generally only
      be used for a table that is not otherwise expressed in any class
      mapping. In particular, using the Association Object Pattern is
      generally mutually exclusive with the use of the *secondary*
      keyword argument.

    :param query_class:
      Optional, a custom Query subclass to be used as the basis for
      dynamic collection.

    """
    from sqlalchemy.orm.dynamic import DynaLoader

    return RelationProperty(
        argument, secondary=secondary, primaryjoin=primaryjoin,
        secondaryjoin=secondaryjoin, foreign_keys=foreign_keys, backref=backref,
        post_update=post_update, cascade=cascade, remote_side=remote_side,
        enable_typechecks=enable_typechecks, passive_deletes=passive_deletes,
        order_by=order_by, comparator_factory=comparator_factory,
        strategy_class=DynaLoader, query_class=query_class)

def column_property(*args, **kwargs):
    """Provide a column-level property for use with a Mapper.

    Column-based properties can normally be applied to the mapper's
    ``properties`` dictionary using the ``schema.Column`` element directly.
    Use this function when the given column is not directly present within the
    mapper's selectable; examples include SQL expressions, functions, and
    scalar SELECT queries.

    Columns that aren't present in the mapper's selectable won't be persisted
    by the mapper and are effectively "read-only" attributes.

      \*cols
          list of Column objects to be mapped.

      comparator_factory
        a class which extends ``sqlalchemy.orm.properties.ColumnProperty.Comparator``
        which provides custom SQL clause generation for comparison operations.

      group
          a group name for this property when marked as deferred.

      deferred
          when True, the column property is "deferred", meaning that
          it does not load immediately, and is instead loaded when the
          attribute is first accessed on an instance.  See also
          :func:`~sqlalchemy.orm.deferred`.

      extension
        an :class:`~sqlalchemy.orm.interfaces.AttributeExtension` instance,
        or list of extensions, which will be prepended to the list of
        attribute listeners for the resulting descriptor placed on the class.
        These listeners will receive append and set events before the
        operation proceeds, and may be used to halt (via exception throw)
        or change the value used in the operation.

    """

    return ColumnProperty(*args, **kwargs)

def composite(class_, *cols, **kwargs):
    """Return a composite column-based property for use with a Mapper.

    This is very much like a column-based property except the given class is
    used to represent "composite" values composed of one or more columns.

    The class must implement a constructor with positional arguments matching
    the order of columns supplied here, as well as a __composite_values__()
    method which returns values in the same order.

    A simple example is representing separate two columns in a table as a
    single, first-class "Point" object::

      class Point(object):
          def __init__(self, x, y):
              self.x = x
              self.y = y
          def __composite_values__(self):
              return self.x, self.y
          def __eq__(self, other):
              return other is not None and self.x == other.x and self.y == other.y

      # and then in the mapping:
      ... composite(Point, mytable.c.x, mytable.c.y) ...

    The composite object may have its attributes populated based on the names
    of the mapped columns.  To override the way internal state is set,
    additionally implement ``__set_composite_values__``::

        class Point(object):
            def __init__(self, x, y):
                self.some_x = x
                self.some_y = y
            def __composite_values__(self):
                return self.some_x, self.some_y
            def __set_composite_values__(self, x, y):
                self.some_x = x
                self.some_y = y
            def __eq__(self, other):
                return other is not None and self.some_x == other.x and self.some_y == other.y

    Arguments are:

    class\_
      The "composite type" class.

    \*cols
      List of Column objects to be mapped.

    group
      A group name for this property when marked as deferred.

    deferred
      When True, the column property is "deferred", meaning that it does not
      load immediately, and is instead loaded when the attribute is first
      accessed on an instance.  See also :func:`~sqlalchemy.orm.deferred`.

    comparator_factory
      a class which extends ``sqlalchemy.orm.properties.CompositeProperty.Comparator``
      which provides custom SQL clause generation for comparison operations.

    extension
      an :class:`~sqlalchemy.orm.interfaces.AttributeExtension` instance,
      or list of extensions, which will be prepended to the list of
      attribute listeners for the resulting descriptor placed on the class.
      These listeners will receive append and set events before the
      operation proceeds, and may be used to halt (via exception throw)
      or change the value used in the operation.

    """
    return CompositeProperty(class_, *cols, **kwargs)


def backref(name, **kwargs):
    """Create a BackRef object with explicit arguments, which are the same
    arguments one can send to ``relation()``.

    Used with the `backref` keyword argument to ``relation()`` in
    place of a string argument.

    """
    return BackRef(name, **kwargs)

def deferred(*columns, **kwargs):
    """Return a ``DeferredColumnProperty``, which indicates this
    object attributes should only be loaded from its corresponding
    table column when first accessed.

    Used with the `properties` dictionary sent to ``mapper()``.

    """
    return ColumnProperty(deferred=True, *columns, **kwargs)

def mapper(class_, local_table=None, *args, **params):
    """Return a new :class:`~sqlalchemy.orm.Mapper` object.

      class\_
        The class to be mapped.

      local_table
        The table to which the class is mapped, or None if this mapper
        inherits from another mapper using concrete table inheritance.

      always_refresh
        If True, all query operations for this mapped class will overwrite all
        data within object instances that already exist within the session,
        erasing any in-memory changes with whatever information was loaded
        from the database.  Usage of this flag is highly discouraged; as an
        alternative, see the method `populate_existing()` on
        :class:`~sqlalchemy.orm.query.Query`.

      allow_null_pks
        Indicates that composite primary keys where one or more (but not all)
        columns contain NULL is a valid primary key.  Primary keys which
        contain NULL values usually indicate that a result row does not
        contain an entity and should be skipped.

      batch
        Indicates that save operations of multiple entities can be batched
        together for efficiency.  setting to False indicates that an instance
        will be fully saved before saving the next instance, which includes
        inserting/updating all table rows corresponding to the entity as well
        as calling all ``MapperExtension`` methods corresponding to the save
        operation.

      column_prefix
        A string which will be prepended to the `key` name of all Columns when
        creating column-based properties from the given Table.  Does not
        affect explicitly specified column-based properties

      concrete
        If True, indicates this mapper should use concrete table inheritance
        with its parent mapper.

      extension
        A :class:`~sqlalchemy.orm.MapperExtension` instance or list of
        ``MapperExtension`` instances which will be applied to all
        operations by this ``Mapper``.

      inherits
        Another ``Mapper`` for which this ``Mapper`` will have an inheritance
        relationship with.

      inherit_condition
        For joined table inheritance, a SQL expression (constructed
        ``ClauseElement``) which will define how the two tables are joined;
        defaults to a natural join between the two tables.

      inherit_foreign_keys
        when inherit_condition is used and the condition contains no
        ForeignKey columns, specify the "foreign" columns of the join
        condition in this list.  else leave as None.

      order_by
        A single ``Column`` or list of ``Columns`` for which
        selection operations should use as the default ordering for entities.
        Defaults to the OID/ROWID of the table if any, or the first primary
        key column of the table.

      non_primary
        Construct a ``Mapper`` that will define only the selection of
        instances, not their persistence.  Any number of non_primary mappers
        may be created for a particular class.

      polymorphic_on
        Used with mappers in an inheritance relationship, a ``Column`` which
        will identify the class/mapper combination to be used with a
        particular row.  Requires the ``polymorphic_identity`` value to be set
        for all mappers in the inheritance hierarchy.  The column specified by
        ``polymorphic_on`` is usually a column that resides directly within
        the base mapper's mapped table; alternatively, it may be a column that
        is only present within the <selectable> portion of the
        ``with_polymorphic`` argument.

      _polymorphic_map
        Used internally to propagate the full map of polymorphic identifiers
        to surrogate mappers.

      polymorphic_identity
        A value which will be stored in the Column denoted by polymorphic_on,
        corresponding to the *class identity* of this mapper.

      polymorphic_fetch
        Deprecated. Unloaded columns load as deferred in all cases; loading
        can be controlled using the "with_polymorphic" option.

      properties
        A dictionary mapping the string names of object attributes to
        ``MapperProperty`` instances, which define the persistence behavior of
        that attribute.  Note that the columns in the mapped table are
        automatically converted into ``ColumnProperty`` instances based on the
        `key` property of each ``Column`` (although they can be overridden
        using this dictionary).

      include_properties
        An inclusive list of properties to map.  Columns present in the mapped
        table but not present in this list will not be automatically converted
        into properties.

      exclude_properties
        A list of properties not to map.  Columns present in the mapped table
        and present in this list will not be automatically converted into
        properties.  Note that neither this option nor include_properties will
        allow an end-run around Python inheritance.  If mapped class ``B``
        inherits from mapped class ``A``, no combination of includes or
        excludes will allow ``B`` to have fewer properties than its
        superclass, ``A``.

      primary_key
        A list of ``Column`` objects which define the *primary key* to be used
        against this mapper's selectable unit.  This is normally simply the
        primary key of the `local_table`, but can be overridden here.

      with_polymorphic
        A tuple in the form ``(<classes>, <selectable>)`` indicating the
        default style of "polymorphic" loading, that is, which tables are
        queried at once. <classes> is any single or list of mappers and/or
        classes indicating the inherited classes that should be loaded at
        once. The special value ``'*'`` may be used to indicate all descending
        classes should be loaded immediately. The second tuple argument
        <selectable> indicates a selectable that will be used to query for
        multiple classes. Normally, it is left as None, in which case this
        mapper will form an outer join from the base mapper's table to that of
        all desired sub-mappers.  When specified, it provides the selectable
        to be used for polymorphic loading. When with_polymorphic includes
        mappers which load from a "concrete" inheriting table, the
        <selectable> argument is required, since it usually requires more
        complex UNION queries.

      select_table
        Deprecated.  Synonymous with
        ``with_polymorphic=('*', <selectable>)``.

      version_id_col
        A ``Column`` which must have an integer type that will be used to keep
        a running *version id* of mapped entities in the database.  this is
        used during save operations to ensure that no other thread or process
        has updated the instance during the lifetime of the entity, else a
        ``ConcurrentModificationError`` exception is thrown.

    """
    return Mapper(class_, local_table, *args, **params)

def synonym(name, map_column=False, descriptor=None, comparator_factory=None, proxy=False):
    """Set up `name` as a synonym to another mapped property.

    Used with the ``properties`` dictionary sent to  :func:`~sqlalchemy.orm.mapper`.

    Any existing attributes on the class which map the key name sent
    to the ``properties`` dictionary will be used by the synonym to provide
    instance-attribute behavior (that is, any Python property object, provided
    by the ``property`` builtin or providing a ``__get__()``, ``__set__()``
    and ``__del__()`` method).  If no name exists for the key, the
    ``synonym()`` creates a default getter/setter object automatically and
    applies it to the class.

    `name` refers to the name of the existing mapped property, which can be
    any other ``MapperProperty`` including column-based properties and
    relations.

    If `map_column` is ``True``, an additional ``ColumnProperty`` is created
    on the mapper automatically, using the synonym's name as the keyname of
    the property, and the keyname of this ``synonym()`` as the name of the
    column to map.  For example, if a table has a column named ``status``::

        class MyClass(object):
            def _get_status(self):
                return self._status
            def _set_status(self, value):
                self._status = value
            status = property(_get_status, _set_status)

        mapper(MyClass, sometable, properties={
            "status":synonym("_status", map_column=True)
        })

    The column named ``status`` will be mapped to the attribute named
    ``_status``, and the ``status`` attribute on ``MyClass`` will be used to
    proxy access to the column-based attribute.

    The `proxy` keyword argument is deprecated and currently does nothing;
    synonyms now always establish an attribute getter/setter function if one
    is not already available.

    """
    return SynonymProperty(name, map_column=map_column, descriptor=descriptor, comparator_factory=comparator_factory)

def comparable_property(comparator_factory, descriptor=None):
    """Provide query semantics for an unmanaged attribute.

    Allows a regular Python @property (descriptor) to be used in Queries and
    SQL constructs like a managed attribute.  comparable_property wraps a
    descriptor with a proxy that directs operator overrides such as ==
    (__eq__) to the supplied comparator but proxies everything else through to
    the original descriptor::

      class MyClass(object):
          @property
          def myprop(self):
              return 'foo'

      class MyComparator(sqlalchemy.orm.interfaces.PropComparator):
          def __eq__(self, other):
              ....

      mapper(MyClass, mytable, properties=dict(
               'myprop': comparable_property(MyComparator)))

    Used with the ``properties`` dictionary sent to  :func:`~sqlalchemy.orm.mapper`.

    comparator_factory
      A PropComparator subclass or factory that defines operator behavior
      for this property.

    descriptor
      Optional when used in a ``properties={}`` declaration.  The Python
      descriptor or property to layer comparison behavior on top of.

      The like-named descriptor will be automatically retreived from the
      mapped class if left blank in a ``properties`` declaration.

    """
    return ComparableProperty(comparator_factory, descriptor)

def compile_mappers():
    """Compile all mappers that have been defined.

    This is equivalent to calling ``compile()`` on any individual mapper.

    """
    for m in list(_mapper_registry):
        m.compile()

def clear_mappers():
    """Remove all mappers that have been created thus far.

    The mapped classes will return to their initial "unmapped" state and can
    be re-mapped with new mappers.

    """
    mapperlib._COMPILE_MUTEX.acquire()
    try:
        for mapper in list(_mapper_registry):
            mapper.dispose()
    finally:
        mapperlib._COMPILE_MUTEX.release()

def extension(ext):
    """Return a ``MapperOption`` that will insert the given
    ``MapperExtension`` to the beginning of the list of extensions
    that will be called in the context of the ``Query``.

    Used with ``query.options()``.

    """
    return ExtensionOption(ext)

@sa_util.accepts_a_list_as_starargs(list_deprecation='pending')
def eagerload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name into an eager load.

    Used with ``query.options()``.

    """
    return strategies.EagerLazyOption(keys, lazy=False)

@sa_util.accepts_a_list_as_starargs(list_deprecation='pending')
def eagerload_all(*keys):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path into an eager load.

    For example, this::

        query.options(eagerload_all('orders.items.keywords'))...

    will set all of 'orders', 'orders.items', and 'orders.items.keywords' to
    load in one eager load.

    Used with ``query.options()``.

    """
    return strategies.EagerLazyOption(keys, lazy=False, chained=True)

@sa_util.accepts_a_list_as_starargs(list_deprecation='pending')
def lazyload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name into a lazy load.

    Used with ``query.options()``.

    """
    return strategies.EagerLazyOption(keys, lazy=True)

def noload(*keys):
    """Return a ``MapperOption`` that will convert the property of the
    given name into a non-load.

    Used with ``query.options()``.

    """
    return strategies.EagerLazyOption(keys, lazy=None)

def contains_alias(alias):
    """Return a ``MapperOption`` that will indicate to the query that
    the main table has been aliased.

    `alias` is the string name or ``Alias`` object representing the
    alias.

    """
    return AliasOption(alias)

@sa_util.accepts_a_list_as_starargs(list_deprecation='pending')
def contains_eager(*keys, **kwargs):
    """Return a ``MapperOption`` that will indicate to the query that
    the given attribute will be eagerly loaded.

    Used when feeding SQL result sets directly into ``query.instances()``.
    Also bundles an ``EagerLazyOption`` to turn on eager loading in case it
    isn't already.

    `alias` is the string name of an alias, **or** an ``sql.Alias`` object,
    which represents the aliased columns in the query.  This argument is
    optional.

    """
    alias = kwargs.pop('alias', None)
    if kwargs:
        raise exceptions.ArgumentError("Invalid kwargs for contains_eager: %r" % kwargs.keys())

    return (strategies.EagerLazyOption(keys, lazy=False, propagate_to_loaders=False), strategies.LoadEagerFromAliasOption(keys, alias=alias))

@sa_util.accepts_a_list_as_starargs(list_deprecation='pending')
def defer(*keys):
    """Return a ``MapperOption`` that will convert the column property of the
    given name into a deferred load.

    Used with ``query.options()``
    """
    return strategies.DeferredOption(keys, defer=True)

@sa_util.accepts_a_list_as_starargs(list_deprecation='pending')
def undefer(*keys):
    """Return a ``MapperOption`` that will convert the column property of the
    given name into a non-deferred (regular column) load.

    Used with ``query.options()``.

    """
    return strategies.DeferredOption(keys, defer=False)

def undefer_group(name):
    """Return a ``MapperOption`` that will convert the given group of deferred
    column properties into a non-deferred (regular column) load.

    Used with ``query.options()``.

    """
    return strategies.UndeferGroupOption(name)
