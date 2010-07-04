# sqlalchemy/orm/__init__.py
# Copyright (C) 2005, 2006, 2007, 2008, 2009, 2010 Michael Bayer mike_mp@zzzcomputing.com
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
     ColumnProperty,
     ComparableProperty,
     CompositeProperty,
     RelationshipProperty,
     PropertyLoader,
     SynonymProperty,
     )
from sqlalchemy.orm import mapper as mapperlib
from sqlalchemy.orm.mapper import reconstructor, validates
from sqlalchemy.orm import strategies
from sqlalchemy.orm.query import AliasOption, Query
from sqlalchemy.sql import util as sql_util
from sqlalchemy.orm.session import Session as _Session
from sqlalchemy.orm.session import object_session, sessionmaker, make_transient
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
    'joinedload',
    'joinedload_all',
    'lazyload',
    'mapper',
    'make_transient',
    'noload',
    'object_mapper',
    'object_session',
    'outerjoin',
    'polymorphic_union',
    'reconstructor',
    'relationship',
    'relation',
    'scoped_session',
    'sessionmaker',
    'subqueryload',
    'subqueryload_all',
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
    kwargs.setdefault('autoflush', False)
    kwargs.setdefault('autocommit', True)
    kwargs.setdefault('expire_on_commit', False)
    return _Session(bind=bind, **kwargs)

def relationship(argument, secondary=None, **kwargs):
    """Provide a relationship of a primary Mapper to a secondary Mapper.
    
    .. note:: This function is known as :func:`relation` in all versions
      of SQLAlchemy prior to version 0.6beta2, including the 0.5 and 0.4 series.
      :func:`~sqlalchemy.orm.relationship()` is only available starting with 
      SQLAlchemy 0.6beta2.  The :func:`relation` name will remain available for 
      the foreseeable future in order to enable cross-compatibility.
    
    This corresponds to a parent-child or associative table relationship.  The
    constructed class is an instance of :class:`RelationshipProperty`.

    A typical :func:`relationship`::

       mapper(Parent, properties={
         'children': relationship(Children)
       })

    :param argument:
      a class or :class:`Mapper` instance, representing the target of
      the relationship.

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
      new relationship.
      
    :param back_populates:
      Takes a string name and has the same meaning as ``backref``, 
      except the complementing property is **not** created automatically, 
      and instead must be configured explicitly on the other mapper.  The 
      complementing property should also indicate ``back_populates`` 
      to this relationship to ensure proper functioning.

    :param cascade:
      a comma-separated list of cascade rules which determines how
      Session operations should be "cascaded" from parent to child.
      This defaults to ``False``, which means the default cascade
      should be used.  The default value is ``"save-update, merge"``.

      Available cascades are:

      * ``save-update`` - cascade the :meth:`~sqlalchemy.orm.session.Session.add` 
        operation.  This cascade applies both to future and
        past calls to :meth:`~sqlalchemy.orm.session.Session.add`, 
        meaning new items added to a collection or scalar relationship
        get placed into the same session as that of the parent, and 
        also applies to items which have been removed from this 
        relationship but are still part of unflushed history.

      * ``merge`` - cascade the :meth:`~sqlalchemy.orm.session.Session.merge`
        operation

      * ``expunge`` - cascade the :meth:`~sqlalchemy.orm.session.Session.expunge`
        operation

      * ``delete`` - cascade the :meth:`~sqlalchemy.orm.session.Session.delete`
        operation

      * ``delete-orphan`` - if an item of the child's type with no
        parent is detected, mark it for deletion.  Note that this
        option prevents a pending item of the child's class from being
        persisted without a parent present.

      * ``refresh-expire`` - cascade the :meth:`~sqlalchemy.orm.session.Session.expire` 
        and :meth:`~sqlalchemy.orm.session.Session.refresh` operations

      * ``all`` - shorthand for "save-update,merge, refresh-expire,
        expunge, delete"

    :param collection_class:
      a class or callable that returns a new list-holding object. will
      be used in place of a plain list for storing elements.

    :param comparator_factory:
      a class which extends :class:`RelationshipProperty.Comparator` which
      provides custom SQL clause generation for comparison operations.

    :param doc:
      docstring which will be applied to the resulting descriptor.
      
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
      within those join conditions. Normally, ``relationship()`` will
      inspect the columns within the join conditions to determine
      which columns are the "foreign key" columns, based on
      information in the ``Table`` metadata. Use this argument when no
      ForeignKey's are present in the join condition, or to override
      the table-defined foreign keys.

    :param innerjoin=False:
      when ``True``, joined eager loads will use an inner join to join
      against related tables instead of an outer join.  The purpose
      of this option is strictly one of performance, as inner joins
      generally perform better than outer joins.  This flag can
      be set to ``True`` when the relationship references an object
      via many-to-one using local foreign keys that are not nullable,
      or when the reference is one-to-one or a collection that is 
      guaranteed to have one or at least one entry.
      
    :param join_depth:
      when non-``None``, an integer value indicating how many levels
      deep "eager" loaders should join on a self-referring or cyclical 
      relationship.  The number counts how many times the same Mapper 
      shall be present in the loading condition along a particular join 
      branch.  When left at its default of ``None``, eager loaders
      will stop chaining when they encounter a the same target mapper 
      which is already higher up in the chain.  This option applies
      both to joined- and subquery- eager loaders.

    :param lazy=('select'|'joined'|'subquery'|'noload'|'dynamic'): specifies 
      how the related items should be loaded. Values include:

      * 'select' - items should be loaded lazily when the property is first
        accessed.

      * 'joined' - items should be loaded "eagerly" in the same query as
        that of the parent, using a JOIN or LEFT OUTER JOIN.
              
      * 'subquery' - items should be loaded "eagerly" within the same
        query as that of the parent, using a second SQL statement
        which issues a JOIN to a subquery of the original
        statement.

      * 'noload' - no loading should occur at any time.  This is to 
        support "write-only" attributes, or attributes which are
        populated in some manner specific to the application.

      * 'dynamic' - the attribute will return a pre-configured
        :class:`~sqlalchemy.orm.query.Query` object for all read 
        operations, onto which further filtering operations can be
        applied before iterating the results.  The dynamic 
        collection supports a limited set of mutation operations,
        allowing ``append()`` and ``remove()``.  Changes to the
        collection will not be visible until flushed 
        to the database, where it is then refetched upon iteration.
       
      * True - a synonym for 'select'
       
      * False - a synonyn for 'joined'
       
      * None - a synonym for 'noload'
       
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
      relationship() will update the value of the attribute on related
      items which are locally present in the session during a flush.

      When False, it is assumed that the database does not enforce
      referential integrity and will not be issuing its own CASCADE
      operation for an update.  The relationship() will issue the
      appropriate UPDATE statements to the database in response to the
      change of a referenced key, and items locally present in the
      session during a flush will also be refreshed.

      This flag should probably be set to False if primary key changes
      are expected and the database in use doesn't support CASCADE
      (i.e. SQLite, MySQL MyISAM tables).

      Also see the passive_updates flag on ``mapper()``.
      
      A future SQLAlchemy release will provide a "detect" feature for
      this flag.

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
      a ColumnElement (i.e. WHERE criterion) that will be used as the primary
      join of this child object against the parent object, or in a
      many-to-many relationship the join of the primary object to the
      association table. By default, this value is computed based on the
      foreign key relationships of the parent and child tables (or association
      table).

    :param remote_side:
      used for self-referential relationships, indicates the column or
      list of columns that form the "remote side" of the relationship.

    :param secondaryjoin:
      a ColumnElement (i.e. WHERE criterion) that will be used as the join of
      an association table to the child object. By default, this value is
      computed based on the foreign key relationships of the association and
      child tables.

    :param single_parent=(True|False):
      when True, installs a validator which will prevent objects
      from being associated with more than one parent at a time.
      This is used for many-to-one or many-to-many relationships that
      should be treated either as one-to-one or one-to-many.  Its
      usage is optional unless delete-orphan cascade is also 
      set on this relationship(), in which case its required (new in 0.5.2).
      
    :param uselist=(True|False):
      a boolean that indicates if this property should be loaded as a
      list or a scalar. In most cases, this value is determined
      automatically by ``relationship()``, based on the type and direction
      of the relationship - one to many forms a list, many to one
      forms a scalar, many to many is a list. If a scalar is desired
      where normally a list would be present, such as a bi-directional
      one-to-one relationship, set uselist to False.

    :param viewonly=False:
      when set to True, the relationship is used only for loading objects
      within the relationship, and has no effect on the unit-of-work
      flush process.  Relationships with viewonly can specify any kind of
      join conditions to provide additional views of related objects
      onto a parent object. Note that the functionality of a viewonly
      relationship has its limits - complicated join conditions may
      not compile into eager or lazy loaders properly. If this is the
      case, use an alternative method.

    """
    return RelationshipProperty(argument, secondary=secondary, **kwargs)

def relation(*arg, **kw):
    """A synonym for :func:`relationship`."""
    
    return relationship(*arg, **kw)
    
def dynamic_loader(argument, secondary=None, primaryjoin=None,
                   secondaryjoin=None, foreign_keys=None, backref=None,
                   post_update=False, cascade=False, remote_side=None,
                   enable_typechecks=True, passive_deletes=False, doc=None,
                   order_by=None, comparator_factory=None, query_class=None):
    """Construct a dynamically-loading mapper property.

    This property is similar to :func:`relationship`, except read
    operations return an active :class:`Query` object which reads from
    the database when accessed.  Items may be appended to the
    attribute via ``append()``, or removed via ``remove()``; changes
    will be persisted to the database during a :meth:`Sesion.flush`.
    However, no other Python list or collection mutation operations
    are available.

    A subset of arguments available to :func:`relationship` are available
    here.

    :param argument:
      a class or :class:`Mapper` instance, representing the target of
      the relationship.

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

    return RelationshipProperty(
        argument, secondary=secondary, primaryjoin=primaryjoin,
        secondaryjoin=secondaryjoin, foreign_keys=foreign_keys, backref=backref,
        post_update=post_update, cascade=cascade, remote_side=remote_side,
        enable_typechecks=enable_typechecks, passive_deletes=passive_deletes,
        order_by=order_by, comparator_factory=comparator_factory,doc=doc,
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
      
      doc
          optional string that will be applied as the doc on the
          class-bound descriptor.
          
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
      a class which extends :class:`~sqlalchemy.orm.properties.CompositeProperty.Comparator`
      which provides custom SQL clause generation for comparison operations.

    doc
      optional string that will be applied as the doc on the
      class-bound descriptor.

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
    """Create a back reference with explicit arguments, which are the same
    arguments one can send to :func:`relationship`.

    Used with the `backref` keyword argument to :func:`relationship` in
    place of a string argument.

    """
    return (name, kwargs)

def deferred(*columns, **kwargs):
    """Return a :class:`DeferredColumnProperty`, which indicates this
    object attributes should only be loaded from its corresponding
    table column when first accessed.

    Used with the `properties` dictionary sent to :func:`mapper`.

    """
    return ColumnProperty(deferred=True, *columns, **kwargs)

def mapper(class_, local_table=None, *args, **params):
    """Return a new :class:`~sqlalchemy.orm.Mapper` object.

        :param class\_: The class to be mapped.

        :param local_table: The table to which the class is mapped, or None if this mapper
            inherits from another mapper using concrete table inheritance.

        :param always_refresh: If True, all query operations for this mapped class will overwrite all
            data within object instances that already exist within the session,
            erasing any in-memory changes with whatever information was loaded
            from the database.  Usage of this flag is highly discouraged; as an
            alternative, see the method `populate_existing()` on
            :class:`~sqlalchemy.orm.query.Query`.

        :param allow_null_pks: This flag is deprecated - this is stated as allow_partial_pks
            which defaults to True.

        :param allow_partial_pks: Defaults to True.  Indicates that a composite primary key with
            some NULL values should be considered as possibly existing
            within the database.   This affects whether a mapper will assign
            an incoming row to an existing identity, as well as if 
            session.merge() will check the database first for a particular
            primary key value.  A "partial primary key" can occur if one 
            has mapped to an OUTER JOIN, for example.

        :param batch: Indicates that save operations of multiple entities can be batched
            together for efficiency.  setting to False indicates that an instance
            will be fully saved before saving the next instance, which includes
            inserting/updating all table rows corresponding to the entity as well
            as calling all :class:`MapperExtension` methods corresponding to the save
            operation.

        :param column_prefix: A string which will be prepended to the `key` name of all Columns when
            creating column-based properties from the given Table.  Does not
            affect explicitly specified column-based properties

        :param concrete: If True, indicates this mapper should use concrete table inheritance
            with its parent mapper.

        :param exclude_properties: A list of properties not to map.  Columns present in the mapped table
            and present in this list will not be automatically converted into
            properties.  Note that neither this option nor include_properties will
            allow an end-run around Python inheritance.  If mapped class ``B``
            inherits from mapped class ``A``, no combination of includes or
            excludes will allow ``B`` to have fewer properties than its
            superclass, ``A``.


        :param extension: A :class:`~sqlalchemy.orm.interfaces.MapperExtension` instance or list of
            :class:`~sqlalchemy.orm.interfaces.MapperExtension` instances which will be applied to all
            operations by this :class:`~sqlalchemy.orm.mapper.Mapper`.

        :param include_properties: An inclusive list of properties to map.  Columns present in the mapped
            table but not present in this list will not be automatically converted
            into properties.

        :param inherits: Another :class:`~sqlalchemy.orm.Mapper` for which 
            this :class:`~sqlalchemy.orm.Mapper` will have an inheritance
            relationship with.


        :param inherit_condition: For joined table inheritance, a SQL expression (constructed
            :class:`~sqlalchemy.expression.sql.ClauseElement`) which will define how the two tables are joined;
            defaults to a natural join between the two tables.

        :param inherit_foreign_keys: When inherit_condition is used and the condition contains no
            ForeignKey columns, specify the "foreign" columns of the join
            condition in this list.  else leave as None.

        :param non_primary: Construct a :class:`Mapper` that will define only the selection of
            instances, not their persistence.  Any number of non_primary mappers
            may be created for a particular class.

        :param order_by: A single :class:`Column` or list of :class:`Column` objects for which
            selection operations should use as the default ordering for entities.
            Defaults to the OID/ROWID of the table if any, or the first primary
            key column of the table.

        :param passive_updates: Indicates UPDATE behavior of foreign keys when a primary key changes 
            on a joined-table inheritance or other joined table mapping.

            When True, it is assumed that ON UPDATE CASCADE is configured on
            the foreign key in the database, and that the database will
            handle propagation of an UPDATE from a source column to
            dependent rows.  Note that with databases which enforce
            referential integrity (i.e. PostgreSQL, MySQL with InnoDB tables),
            ON UPDATE CASCADE is required for this operation.  The
            relationship() will update the value of the attribute on related
            items which are locally present in the session during a flush.

            When False, it is assumed that the database does not enforce
            referential integrity and will not be issuing its own CASCADE
            operation for an update.  The relationship() will issue the
            appropriate UPDATE statements to the database in response to the
            change of a referenced key, and items locally present in the
            session during a flush will also be refreshed.

            This flag should probably be set to False if primary key changes
            are expected and the database in use doesn't support CASCADE
            (i.e. SQLite, MySQL MyISAM tables).

            Also see the passive_updates flag on :func:`relationship()`.

            A future SQLAlchemy release will provide a "detect" feature for
            this flag.

        :param polymorphic_on: Used with mappers in an inheritance relationship, a ``Column`` which
            will identify the class/mapper combination to be used with a
            particular row.  Requires the ``polymorphic_identity`` value to be set
            for all mappers in the inheritance hierarchy.  The column specified by
            ``polymorphic_on`` is usually a column that resides directly within
            the base mapper's mapped table; alternatively, it may be a column that
            is only present within the <selectable> portion of the
            ``with_polymorphic`` argument.

        :param polymorphic_identity: A value which will be stored in the Column denoted by polymorphic_on,
            corresponding to the *class identity* of this mapper.

        :param properties: A dictionary mapping the string names of object attributes to
            ``MapperProperty`` instances, which define the persistence behavior of
            that attribute.  Note that the columns in the mapped table are
            automatically converted into ``ColumnProperty`` instances based on the
            `key` property of each ``Column`` (although they can be overridden
            using this dictionary).

        :param primary_key: A list of ``Column`` objects which define the *primary key* to be used
            against this mapper's selectable unit.  This is normally simply the
            primary key of the `local_table`, but can be overridden here.

        :param version_id_col: A ``Column`` which must have an integer type that will be used to keep
            a running *version id* of mapped entities in the database.  this is
            used during save operations to ensure that no other thread or process
            has updated the instance during the lifetime of the entity, else a
            ``ConcurrentModificationError`` exception is thrown.

        :param version_id_generator: A callable which defines the algorithm used to generate new version 
            ids.  Defaults to an integer generator.  Can be replaced with one that
            generates timestamps, uuids, etc.  e.g.::

                import uuid

                mapper(Cls, table, 
                version_id_col=table.c.version_uuid,
                version_id_generator=lambda version:uuid.uuid4().hex
                )

            The callable receives the current version identifier as its 
            single argument.

        :param with_polymorphic: A tuple in the form ``(<classes>, <selectable>)`` indicating the
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

          
    """
    return Mapper(class_, local_table, *args, **params)

def synonym(name, map_column=False, descriptor=None, comparator_factory=None, doc=None):
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
    relationships.

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

    """
    return SynonymProperty(name, map_column=map_column, 
                            descriptor=descriptor, 
                            comparator_factory=comparator_factory,
                            doc=doc)

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
        while _mapper_registry:
            try:
                # can't even reliably call list(weakdict) in jython
                mapper, b = _mapper_registry.popitem()
                mapper.dispose()
            except KeyError:
                pass
    finally:
        mapperlib._COMPILE_MUTEX.release()

def extension(ext):
    """Return a ``MapperOption`` that will insert the given
    ``MapperExtension`` to the beginning of the list of extensions
    that will be called in the context of the ``Query``.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    """
    return ExtensionOption(ext)

@sa_util.accepts_a_list_as_starargs(list_deprecation='deprecated')
def joinedload(*keys, **kw):
    """Return a ``MapperOption`` that will convert the property of the given
    name into an joined eager load.

    .. note:: This function is known as :func:`eagerload` in all versions
      of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4 series.
      :func:`eagerload` will remain available for 
      the foreseeable future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::
    
        # joined-load the "orders" colleciton on "User"
        query(User).options(joinedload(User.orders))
        
        # joined-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those 
        # remain lazily loaded.
        query(Order).options(joinedload(Order.items, Item.keywords))

        # to joined-load across both, use joinedload_all()
        query(Order).options(joinedload_all(Order.items, Item.keywords))

    :func:`joinedload` also accepts a keyword argument `innerjoin=True` which
    indicates using an inner join instead of an outer::
    
        query(Order).options(joinedload(Order.user, innerjoin=True))
        
    Note that the join created by :func:`joinedload` is aliased such that
    no other aspects of the query will affect what it loads.  To use joined eager
    loading with a join that is constructed manually using :meth:`~sqlalchemy.orm.query.Query.join`
    or :func:`~sqlalchemy.orm.join`, see :func:`contains_eager`.
    
    See also:  :func:`subqueryload`, :func:`lazyload`
    
    """
    innerjoin = kw.pop('innerjoin', None)
    if innerjoin is not None:
        return (
             strategies.EagerLazyOption(keys, lazy='joined'), 
             strategies.EagerJoinOption(keys, innerjoin)
         )
    else:
        return strategies.EagerLazyOption(keys, lazy='joined')

@sa_util.accepts_a_list_as_starargs(list_deprecation='deprecated')
def joinedload_all(*keys, **kw):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path into an joined eager load.

    .. note:: This function is known as :func:`eagerload_all` in all versions
      of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4 series.
      :func:`eagerload_all` will remain available for 
      the foreseeable future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(joinedload_all('orders.items.keywords'))...

    will set all of 'orders', 'orders.items', and 'orders.items.keywords' to
    load in one joined eager load.

    Individual descriptors are accepted as arguments as well::
    
        query.options(joinedload_all(User.orders, Order.items, Item.keywords))

    The keyword arguments accept a flag `innerjoin=True|False` which will 
    override the value of the `innerjoin` flag specified on the relationship().

    See also:  :func:`subqueryload_all`, :func:`lazyload`

    """
    innerjoin = kw.pop('innerjoin', None)
    if innerjoin is not None:
        return (
            strategies.EagerLazyOption(keys, lazy='joined', chained=True), 
            strategies.EagerJoinOption(keys, innerjoin, chained=True)
        )
    else:
        return strategies.EagerLazyOption(keys, lazy='joined', chained=True)

def eagerload(*args, **kwargs):
    """A synonym for :func:`joinedload()`."""
    return joinedload(*args, **kwargs)
    
def eagerload_all(*args, **kwargs):
    """A synonym for :func:`joinedload_all()`"""
    return joinedload_all(*args, **kwargs)
    
def subqueryload(*keys):
    """Return a ``MapperOption`` that will convert the property 
    of the given name into an subquery eager load.

    .. note:: This function is new as of SQLAlchemy version 0.6beta3.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::
    
        # subquery-load the "orders" colleciton on "User"
        query(User).options(subqueryload(User.orders))
        
        # subquery-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those 
        # remain lazily loaded.
        query(Order).options(subqueryload(Order.items, Item.keywords))

        # to subquery-load across both, use subqueryload_all()
        query(Order).options(subqueryload_all(Order.items, Item.keywords))

    See also:  :func:`joinedload`, :func:`lazyload`
    
    """
    return strategies.EagerLazyOption(keys, lazy="subquery")

def subqueryload_all(*keys):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path into a subquery eager load.

    .. note:: This function is new as of SQLAlchemy version 0.6beta3.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(subqueryload_all('orders.items.keywords'))...

    will set all of 'orders', 'orders.items', and 'orders.items.keywords' to
    load in one subquery eager load.

    Individual descriptors are accepted as arguments as well::
    
        query.options(subqueryload_all(User.orders, Order.items, Item.keywords))

    See also:  :func:`joinedload_all`, :func:`lazyload`

    """
    return strategies.EagerLazyOption(keys, lazy="subquery", chained=True)
    
@sa_util.accepts_a_list_as_starargs(list_deprecation='deprecated')
def lazyload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`

    """
    return strategies.EagerLazyOption(keys, lazy=True)

def noload(*keys):
    """Return a ``MapperOption`` that will convert the property of the
    given name into a non-load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`, :func:`subqueryload`

    """
    return strategies.EagerLazyOption(keys, lazy=None)

def contains_alias(alias):
    """Return a ``MapperOption`` that will indicate to the query that
    the main table has been aliased.

    `alias` is the string name or ``Alias`` object representing the
    alias.

    """
    return AliasOption(alias)

@sa_util.accepts_a_list_as_starargs(list_deprecation='deprecated')
def contains_eager(*keys, **kwargs):
    """Return a ``MapperOption`` that will indicate to the query that
    the given attribute should be eagerly loaded from columns currently
    in the query.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    The option is used in conjunction with an explicit join that loads 
    the desired rows, i.e.::
    
        sess.query(Order).\\
                join(Order.user).\\
                options(contains_eager(Order.user))
                
    The above query would join from the ``Order`` entity to its related
    ``User`` entity, and the returned ``Order`` objects would have the
    ``Order.user`` attribute pre-populated.

    :func:`contains_eager` also accepts an `alias` argument, which
    is the string name of an alias, an :func:`~sqlalchemy.sql.expression.alias`
    construct, or an :func:`~sqlalchemy.orm.aliased` construct.  Use this
    when the eagerly-loaded rows are to come from an aliased table::
    
        user_alias = aliased(User)
        sess.query(Order).\\
                join((user_alias, Order.user)).\\
                options(contains_eager(Order.user, alias=user_alias))

    See also :func:`eagerload` for the "automatic" version of this 
    functionality.

    """
    alias = kwargs.pop('alias', None)
    if kwargs:
        raise exceptions.ArgumentError("Invalid kwargs for contains_eager: %r" % kwargs.keys())

    return (
            strategies.EagerLazyOption(keys, lazy='joined', propagate_to_loaders=False), 
            strategies.LoadEagerFromAliasOption(keys, alias=alias)
        )

@sa_util.accepts_a_list_as_starargs(list_deprecation='deprecated')
def defer(*keys):
    """Return a ``MapperOption`` that will convert the column property of the
    given name into a deferred load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    """
    return strategies.DeferredOption(keys, defer=True)

@sa_util.accepts_a_list_as_starargs(list_deprecation='deprecated')
def undefer(*keys):
    """Return a ``MapperOption`` that will convert the column property of the
    given name into a non-deferred (regular column) load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    """
    return strategies.DeferredOption(keys, defer=False)

def undefer_group(name):
    """Return a ``MapperOption`` that will convert the given group of deferred
    column properties into a non-deferred (regular column) load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    """
    return strategies.UndeferGroupOption(name)
