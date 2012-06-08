# orm/__init__.py
# Copyright (C) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>
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
     configure_mappers
     )
from sqlalchemy.orm.interfaces import (
     EXT_CONTINUE,
     EXT_STOP,
     InstrumentationManager,
     MapperExtension,
     PropComparator,
     SessionExtension,
     AttributeExtension,
     )
from sqlalchemy.orm.util import (
     aliased,
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
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.session import object_session, sessionmaker, \
    make_transient
from sqlalchemy.orm.scoping import ScopedSession
from sqlalchemy import util as sa_util

__all__ = (
    'EXT_CONTINUE',
    'EXT_STOP',
    'InstrumentationManager',
    'MapperExtension',
    'AttributeExtension',
    'PropComparator',
    'Query',
    'Session',
    'aliased',
    'backref',
    'class_mapper',
    'clear_mappers',
    'column_property',
    'comparable_property',
    'compile_mappers',
    'configure_mappers',
    'composite',
    'contains_alias',
    'contains_eager',
    'create_session',
    'defer',
    'deferred',
    'dynamic_loader',
    'eagerload',
    'eagerload_all',
    'immediateload',
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
    """Provides thread-local or scoped management of :class:`.Session` objects.

    This is a front-end function to
    :class:`.ScopedSession`::

      Session = scoped_session(sessionmaker(autoflush=True))

    To instantiate a Session object which is part of the scoped context,
    instantiate normally::

      session = Session()

    Most session methods are available as classmethods from the scoped
    session::

      Session.commit()
      Session.close()

    See also: :ref:`unitofwork_contextual`.

    :param session_factory: a callable function that produces
      :class:`.Session` instances, such as :func:`sessionmaker`.

    :param scopefunc: Optional "scope" function which would be
      passed to the :class:`.ScopedRegistry`.  If None, the
      :class:`.ThreadLocalRegistry` is used by default.

    :returns: a :class:`.ScopedSession` instance


    """
    return ScopedSession(session_factory, scopefunc=scopefunc)

def create_session(bind=None, **kwargs):
    """Create a new :class:`.Session` 
    with no automation enabled by default.

    This function is used primarily for testing.   The usual
    route to :class:`.Session` creation is via its constructor
    or the :func:`.sessionmaker` function.

    :param bind: optional, a single Connectable to use for all
      database access in the created
      :class:`~sqlalchemy.orm.session.Session`.

    :param \*\*kwargs: optional, passed through to the
      :class:`.Session` constructor.

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
    return Session(bind=bind, **kwargs)

def relationship(argument, secondary=None, **kwargs):
    """Provide a relationship of a primary Mapper to a secondary Mapper.

    .. versionchanged:: 0.6
        :func:`relationship` is historically known as :func:`relation`.

    This corresponds to a parent-child or associative table relationship.  The
    constructed class is an instance of :class:`.RelationshipProperty`.

    A typical :func:`.relationship`, used in a classical mapping::

       mapper(Parent, properties={
         'children': relationship(Child)
       })

    Some arguments accepted by :func:`.relationship` optionally accept a 
    callable function, which when called produces the desired value.
    The callable is invoked by the parent :class:`.Mapper` at "mapper initialization"
    time, which happens only when mappers are first used, and is assumed
    to be after all mappings have been constructed.  This can be used
    to resolve order-of-declaration and other dependency issues, such as 
    if ``Child`` is declared below ``Parent`` in the same file::
    
        mapper(Parent, properties={
            "children":relationship(lambda: Child, 
                                order_by=lambda: Child.id)
        })
    
    When using the :ref:`declarative_toplevel` extension, the Declarative
    initializer allows string arguments to be passed to :func:`.relationship`.
    These string arguments are converted into callables that evaluate 
    the string as Python code, using the Declarative
    class-registry as a namespace.  This allows the lookup of related
    classes to be automatic via their string name, and removes the need to import
    related classes at all into the local module space::
    
        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()
        
        class Parent(Base):
            __tablename__ = 'parent'
            id = Column(Integer, primary_key=True)
            children = relationship("Child", order_by="Child.id")
    
    A full array of examples and reference documentation regarding
    :func:`.relationship` is at :ref:`relationship_config_toplevel`.
    
    :param argument:
      a mapped class, or actual :class:`.Mapper` instance, representing the target of
      the relationship.  
      
      ``argument`` may also be passed as a callable function
      which is evaluated at mapper initialization time, and may be passed as a 
      Python-evaluable string when using Declarative.

    :param secondary:
      for a many-to-many relationship, specifies the intermediary
      table, and is an instance of :class:`.Table`.  The ``secondary`` keyword 
      argument should generally only
      be used for a table that is not otherwise expressed in any class
      mapping, unless this relationship is declared as view only, otherwise
      conflicting persistence operations can occur.   
      
      ``secondary`` may
      also be passed as a callable function which is evaluated at 
      mapper initialization time.

    :param active_history=False:
      When ``True``, indicates that the "previous" value for a
      many-to-one reference should be loaded when replaced, if
      not already loaded. Normally, history tracking logic for
      simple many-to-ones only needs to be aware of the "new"
      value in order to perform a flush. This flag is available
      for applications that make use of
      :func:`.attributes.get_history` which also need to know
      the "previous" value of the attribute.

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

      * ``save-update`` - cascade the :meth:`.Session.add` 
        operation.  This cascade applies both to future and
        past calls to :meth:`~sqlalchemy.orm.session.Session.add`, 
        meaning new items added to a collection or scalar relationship
        get placed into the same session as that of the parent, and 
        also applies to items which have been removed from this 
        relationship but are still part of unflushed history.

      * ``merge`` - cascade the :meth:`~sqlalchemy.orm.session.Session.merge`
        operation

      * ``expunge`` - cascade the :meth:`.Session.expunge`
        operation

      * ``delete`` - cascade the :meth:`.Session.delete`
        operation

      * ``delete-orphan`` - if an item of the child's type is 
        detached from its parent, mark it for deletion.  

        .. versionchanged:: 0.7
            This option does not prevent
            a new instance of the child object from being persisted
            without a parent to start with; to constrain against
            that case, ensure the child's foreign key column(s)
            is configured as NOT NULL

      * ``refresh-expire`` - cascade the :meth:`.Session.expire` 
        and :meth:`~sqlalchemy.orm.session.Session.refresh` operations

      * ``all`` - shorthand for "save-update,merge, refresh-expire,
        expunge, delete"

     See the section :ref:`unitofwork_cascades` for more background
     on configuring cascades.
     
    :param cascade_backrefs=True:
      a boolean value indicating if the ``save-update`` cascade should
      operate along an assignment event intercepted by a backref.   
      When set to ``False``,
      the attribute managed by this relationship will not cascade
      an incoming transient object into the session of a
      persistent parent, if the event is received via backref.
      
      That is::
      
        mapper(A, a_table, properties={
            'bs':relationship(B, backref="a", cascade_backrefs=False)
        })
        
      If an ``A()`` is present in the session, assigning it to
      the "a" attribute on a transient ``B()`` will not place
      the ``B()`` into the session.   To set the flag in the other 
      direction, i.e. so that ``A().bs.append(B())`` won't add 
      a transient ``A()`` into the session for a persistent ``B()``::
      
        mapper(A, a_table, properties={
            'bs':relationship(B, 
                    backref=backref("a", cascade_backrefs=False)
                )
        })
      
      See the section :ref:`unitofwork_cascades` for more background
      on configuring cascades.

    :param collection_class:
      a class or callable that returns a new list-holding object. will
      be used in place of a plain list for storing elements.
      Behavior of this attribute is described in detail at
      :ref:`custom_collections`.

    :param comparator_factory:
      a class which extends :class:`.RelationshipProperty.Comparator` which
      provides custom SQL clause generation for comparison operations.

    :param doc:
      docstring which will be applied to the resulting descriptor.

    :param extension:
      an :class:`.AttributeExtension` instance, or list of extensions,
      which will be prepended to the list of attribute listeners for
      the resulting descriptor placed on the class.
      **Deprecated.**  Please see :class:`.AttributeEvents`.

    :param foreign_keys:
      a list of columns which are to be used as "foreign key" columns.
      Normally, :func:`relationship` uses the :class:`.ForeignKey`
      and :class:`.ForeignKeyConstraint` objects present within the
      mapped or secondary :class:`.Table` to determine the "foreign" side of 
      the join condition.  This is used to construct SQL clauses in order
      to load objects, as well as to "synchronize" values from 
      primary key columns to referencing foreign key columns.
      The ``foreign_keys`` parameter overrides the notion of what's
      "foreign" in the table metadata, allowing the specification
      of a list of :class:`.Column` objects that should be considered
      part of the foreign key.

      There are only two use cases for ``foreign_keys`` - one, when it is not
      convenient for :class:`.Table` metadata to contain its own foreign key
      metadata (which should be almost never, unless reflecting a large amount of
      tables from a MySQL MyISAM schema, or a schema that doesn't actually
      have foreign keys on it). The other is for extremely
      rare and exotic composite foreign key setups where some columns
      should artificially not be considered as foreign.

      ``foreign_keys`` may also be passed as a callable function
      which is evaluated at mapper initialization time, and may be passed as a 
      Python-evaluable string when using Declarative.
      
    :param innerjoin=False:
      when ``True``, joined eager loads will use an inner join to join
      against related tables instead of an outer join.  The purpose
      of this option is generally one of performance, as inner joins
      generally perform better than outer joins. Another reason can be
      the use of ``with_lockmode``, which does not support outer joins.

      This flag can be set to ``True`` when the relationship references an
      object via many-to-one using local foreign keys that are not nullable,
      or when the reference is one-to-one or a collection that is guaranteed
      to have one or at least one entry.

    :param join_depth:
      when non-``None``, an integer value indicating how many levels
      deep "eager" loaders should join on a self-referring or cyclical 
      relationship.  The number counts how many times the same Mapper 
      shall be present in the loading condition along a particular join 
      branch.  When left at its default of ``None``, eager loaders
      will stop chaining when they encounter a the same target mapper 
      which is already higher up in the chain.  This option applies
      both to joined- and subquery- eager loaders.

    :param lazy='select': specifies 
      how the related items should be loaded.  Default value is 
      ``select``.  Values include:

      * ``select`` - items should be loaded lazily when the property is first
        accessed, using a separate SELECT statement, or identity map
        fetch for simple many-to-one references.

      * ``immediate`` - items should be loaded as the parents are loaded,
        using a separate SELECT statement, or identity map fetch for
        simple many-to-one references.

        .. versionadded:: 0.6.5

      * ``joined`` - items should be loaded "eagerly" in the same query as
        that of the parent, using a JOIN or LEFT OUTER JOIN.  Whether
        the join is "outer" or not is determined by the ``innerjoin``
        parameter.

      * ``subquery`` - items should be loaded "eagerly" within the same
        query as that of the parent, using a second SQL statement
        which issues a JOIN to a subquery of the original
        statement.

      * ``noload`` - no loading should occur at any time.  This is to 
        support "write-only" attributes, or attributes which are
        populated in some manner specific to the application.

      * ``dynamic`` - the attribute will return a pre-configured
        :class:`~sqlalchemy.orm.query.Query` object for all read 
        operations, onto which further filtering operations can be
        applied before iterating the results.  See
        the section :ref:`dynamic_relationship` for more details.

      * True - a synonym for 'select'

      * False - a synonym for 'joined'

      * None - a synonym for 'noload'

      Detailed discussion of loader strategies is at :ref:`loading_toplevel`.

    :param load_on_pending=False:
      Indicates loading behavior for transient or pending parent objects.

      When set to ``True``, causes the lazy-loader to
      issue a query for a parent object that is not persistent, meaning it has
      never been flushed.  This may take effect for a pending object when
      autoflush is disabled, or for a transient object that has been
      "attached" to a :class:`.Session` but is not part of its pending
      collection. Attachment of transient objects to the session without
      moving to the "pending" state is not a supported behavior at this time.

      Note that the load of related objects on a pending or transient object
      also does not trigger any attribute change events - no user-defined
      events will be emitted for these attributes, and if and when the 
      object is ultimately flushed, only the user-specific foreign key 
      attributes will be part of the modified state.

      The load_on_pending flag does not improve behavior
      when the ORM is used normally - object references should be constructed
      at the object level, not at the foreign key level, so that they
      are present in an ordinary way before flush() proceeds.  This flag
      is not not intended for general use.

      New in 0.6.5.

    :param order_by:
      indicates the ordering that should be applied when loading these
      items.  ``order_by`` is expected to refer to one of the :class:`.Column`
      objects to which the target class is mapped, or 
      the attribute itself bound to the target class which refers
      to the column.

      ``order_by`` may also be passed as a callable function
      which is evaluated at mapper initialization time, and may be passed as a 
      Python-evaluable string when using Declarative.
      
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
      a SQL expression that will be used as the primary
      join of this child object against the parent object, or in a
      many-to-many relationship the join of the primary object to the
      association table. By default, this value is computed based on the
      foreign key relationships of the parent and child tables (or association
      table).

      ``primaryjoin`` may also be passed as a callable function
      which is evaluated at mapper initialization time, and may be passed as a 
      Python-evaluable string when using Declarative.

    :param remote_side:
      used for self-referential relationships, indicates the column or
      list of columns that form the "remote side" of the relationship.

      ``remote_side`` may also be passed as a callable function
      which is evaluated at mapper initialization time, and may be passed as a 
      Python-evaluable string when using Declarative.

    :param query_class:
      a :class:`.Query` subclass that will be used as the base of the
      "appender query" returned by a "dynamic" relationship, that
      is, a relationship that specifies ``lazy="dynamic"`` or was 
      otherwise constructed using the :func:`.orm.dynamic_loader`
      function.
      
    :param secondaryjoin:
      a SQL expression that will be used as the join of
      an association table to the child object. By default, this value is
      computed based on the foreign key relationships of the association and
      child tables.

      ``secondaryjoin`` may also be passed as a callable function
      which is evaluated at mapper initialization time, and may be passed as a 
      Python-evaluable string when using Declarative.

    :param single_parent=(True|False):
      when True, installs a validator which will prevent objects
      from being associated with more than one parent at a time.
      This is used for many-to-one or many-to-many relationships that
      should be treated either as one-to-one or one-to-many.  Its
      usage is optional unless delete-orphan cascade is also 
      set on this relationship(), in which case its required.

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

def dynamic_loader(argument, **kw):
    """Construct a dynamically-loading mapper property.

    This is essentially the same as 
    using the ``lazy='dynamic'`` argument with :func:`relationship`::

        dynamic_loader(SomeClass)
        
        # is the same as
        
        relationship(SomeClass, lazy="dynamic")

    See the section :ref:`dynamic_relationship` for more details
    on dynamic loading.

    """
    kw['lazy'] = 'dynamic'
    return relationship(argument, **kw)

def column_property(*cols, **kw):
    """Provide a column-level property for use with a Mapper.

    Column-based properties can normally be applied to the mapper's
    ``properties`` dictionary using the :class:`.Column` element directly.
    Use this function when the given column is not directly present within the
    mapper's selectable; examples include SQL expressions, functions, and
    scalar SELECT queries.

    Columns that aren't present in the mapper's selectable won't be persisted
    by the mapper and are effectively "read-only" attributes.

    :param \*cols:
          list of Column objects to be mapped.

    :param active_history=False:
      When ``True``, indicates that the "previous" value for a
      scalar attribute should be loaded when replaced, if not
      already loaded. Normally, history tracking logic for
      simple non-primary-key scalar values only needs to be
      aware of the "new" value in order to perform a flush. This
      flag is available for applications that make use of
      :func:`.attributes.get_history` or :meth:`.Session.is_modified`
      which also need to know
      the "previous" value of the attribute.

      .. versionadded:: 0.6.6

    :param comparator_factory: a class which extends
       :class:`.ColumnProperty.Comparator` which provides custom SQL clause
       generation for comparison operations.

    :param group:
        a group name for this property when marked as deferred.

    :param deferred:
          when True, the column property is "deferred", meaning that
          it does not load immediately, and is instead loaded when the
          attribute is first accessed on an instance.  See also
          :func:`~sqlalchemy.orm.deferred`.

    :param doc:
          optional string that will be applied as the doc on the
          class-bound descriptor.
    
    :param expire_on_flush=True:
        Disable expiry on flush.   A column_property() which refers
        to a SQL expression (and not a single table-bound column)
        is considered to be a "read only" property; populating it
        has no effect on the state of data, and it can only return
        database state.   For this reason a column_property()'s value
        is expired whenever the parent object is involved in a 
        flush, that is, has any kind of "dirty" state within a flush.
        Setting this parameter to ``False`` will have the effect of
        leaving any existing value present after the flush proceeds.
        Note however that the :class:`.Session` with default expiration
        settings still expires 
        all attributes after a :meth:`.Session.commit` call, however.

        .. versionadded:: 0.7.3

    :param extension:
        an
        :class:`.AttributeExtension`
        instance, or list of extensions, which will be prepended
        to the list of attribute listeners for the resulting
        descriptor placed on the class. 
        **Deprecated.** Please see :class:`.AttributeEvents`.


    """

    return ColumnProperty(*cols, **kw)

def composite(class_, *cols, **kwargs):
    """Return a composite column-based property for use with a Mapper.

    See the mapping documentation section :ref:`mapper_composite` for a full
    usage example.

    :param class\_:
      The "composite type" class.

    :param \*cols:
      List of Column objects to be mapped.

    :param active_history=False:
      When ``True``, indicates that the "previous" value for a
      scalar attribute should be loaded when replaced, if not
      already loaded.  See the same flag on :func:`.column_property`.

      .. versionchanged:: 0.7
          This flag specifically becomes meaningful
          - previously it was a placeholder.

    :param group:
      A group name for this property when marked as deferred.

    :param deferred:
      When True, the column property is "deferred", meaning that it does not
      load immediately, and is instead loaded when the attribute is first
      accessed on an instance.  See also :func:`~sqlalchemy.orm.deferred`.

    :param comparator_factory:  a class which extends
      :class:`.CompositeProperty.Comparator` which provides custom SQL clause
      generation for comparison operations.

    :param doc:
      optional string that will be applied as the doc on the
      class-bound descriptor.

    :param extension:
      an :class:`.AttributeExtension` instance,
      or list of extensions, which will be prepended to the list of
      attribute listeners for the resulting descriptor placed on the class.
      **Deprecated.**  Please see :class:`.AttributeEvents`.

    """
    return CompositeProperty(class_, *cols, **kwargs)


def backref(name, **kwargs):
    """Create a back reference with explicit keyword arguments, which are the same
    arguments one can send to :func:`relationship`.

    Used with the ``backref`` keyword argument to :func:`relationship` in
    place of a string argument, e.g.::
    
        'items':relationship(SomeItem, backref=backref('parent', lazy='subquery'))

    """
    return (name, kwargs)

def deferred(*columns, **kwargs):
    """Return a :class:`.DeferredColumnProperty`, which indicates this
    object attributes should only be loaded from its corresponding
    table column when first accessed.

    Used with the "properties" dictionary sent to :func:`mapper`.

    See also:
    
    :ref:`deferred`

    """
    return ColumnProperty(deferred=True, *columns, **kwargs)

def mapper(class_, local_table=None, *args, **params):
    """Return a new :class:`~.Mapper` object.
    
        This function is typically used behind the scenes
        via the Declarative extension.   When using Declarative,
        many of the usual :func:`.mapper` arguments are handled
        by the Declarative extension itself, including ``class_``,
        ``local_table``, ``properties``, and  ``inherits``.
        Other options are passed to :func:`.mapper` using 
        the ``__mapper_args__`` class variable::
   
           class MyClass(Base):
               __tablename__ = 'my_table'
               id = Column(Integer, primary_key=True)
               type = Column(String(50))
               alt = Column("some_alt", Integer)
               
               __mapper_args__ = {
                   'polymorphic_on' : type
               }


        Explicit use of :func:`.mapper`
        is often referred to as *classical mapping*.  The above 
        declarative example is equivalent in classical form to::
        
            my_table = Table("my_table", metadata,
                Column('id', Integer, primary_key=True),
                Column('type', String(50)),
                Column("some_alt", Integer)
            )
            
            class MyClass(object):
                pass
            
            mapper(MyClass, my_table, 
                polymorphic_on=my_table.c.type, 
                properties={
                    'alt':my_table.c.some_alt
                })
        
        See also:
        
        :ref:`classical_mapping` - discussion of direct usage of
        :func:`.mapper`

        :param class\_: The class to be mapped.  When using Declarative,
          this argument is automatically passed as the declared class
          itself.

        :param local_table: The :class:`.Table` or other selectable 
           to which the class is mapped.  May be ``None`` if 
           this mapper inherits from another mapper using single-table
           inheritance.   When using Declarative, this argument is 
           automatically passed by the extension, based on what
           is configured via the ``__table__`` argument or via the :class:`.Table`
           produced as a result of the ``__tablename__`` and :class:`.Column`
           arguments present.

        :param always_refresh: If True, all query operations for this mapped
           class will overwrite all data within object instances that already
           exist within the session, erasing any in-memory changes with
           whatever information was loaded from the database. Usage of this
           flag is highly discouraged; as an alternative, see the method
           :meth:`.Query.populate_existing`.

        :param allow_null_pks: This flag is deprecated - this is stated as
           allow_partial_pks which defaults to True.

        :param allow_partial_pks: Defaults to True.  Indicates that a
           composite primary key with some NULL values should be considered as
           possibly existing within the database. This affects whether a
           mapper will assign an incoming row to an existing identity, as well
           as if :meth:`.Session.merge` will check the database first for a
           particular primary key value. A "partial primary key" can occur if
           one has mapped to an OUTER JOIN, for example.

        :param batch: Defaults to ``True``, indicating that save operations 
           of multiple entities can be batched together for efficiency. 
           Setting to False indicates
           that an instance will be fully saved before saving the next
           instance.  This is used in the extremely rare case that a 
           :class:`.MapperEvents` listener requires being called 
           in between individual row persistence operations.

        :param column_prefix: A string which will be prepended 
           to the mapped attribute name when :class:`.Column`
           objects are automatically assigned as attributes to the
           mapped class.  Does not affect explicitly specified 
           column-based properties.   
           
           See the section :ref:`column_prefix` for an example.

        :param concrete: If True, indicates this mapper should use concrete
           table inheritance with its parent mapper.
           
           See the section :ref:`concrete_inheritance` for an example.

        :param exclude_properties: A list or set of string column names to 
          be excluded from mapping.  
          
          See :ref:`include_exclude_cols` for an example.

        :param extension: A :class:`.MapperExtension` instance or
           list of :class:`.MapperExtension`
           instances which will be applied to all operations by this
           :class:`.Mapper`.  **Deprecated.**  Please see :class:`.MapperEvents`.

        :param include_properties: An inclusive list or set of string column
          names to map.   
          
          See :ref:`include_exclude_cols` for an example.

        :param inherits: A mapped class or the corresponding :class:`.Mapper` 
          of one indicating a superclass to which this :class:`.Mapper`
          should *inherit* from.   The mapped class here must be a subclass of the
          other mapper's class.   When using Declarative, this argument
          is passed automatically as a result of the natural class
          hierarchy of the declared classes.   
          
          See also:
          
          :ref:`inheritance_toplevel`
          
        :param inherit_condition: For joined table inheritance, a SQL
           expression which will
           define how the two tables are joined; defaults to a natural join
           between the two tables.

        :param inherit_foreign_keys: When ``inherit_condition`` is used and the
           columns present are missing a :class:`.ForeignKey` configuration, 
           this parameter can be used to specify which columns are "foreign".  
           In most cases can be left as ``None``.

        :param non_primary: Specify that this :class:`.Mapper` is in addition
          to the "primary" mapper, that is, the one used for persistence.
          The :class:`.Mapper` created here may be used for ad-hoc
          mapping of the class to an alternate selectable, for loading
          only.   
          
          The ``non_primary`` feature is rarely needed with modern
          usage.

        :param order_by: A single :class:`.Column` or list of :class:`.Column`
           objects for which selection operations should use as the default
           ordering for entities.  By default mappers have no pre-defined 
           ordering.

        :param passive_updates: Indicates UPDATE behavior of foreign key
           columns when a primary key column changes on a joined-table inheritance 
           mapping.   Defaults to ``True``.

           When True, it is assumed that ON UPDATE CASCADE is configured on
           the foreign key in the database, and that the database will handle
           propagation of an UPDATE from a source column to dependent columns
           on joined-table rows.

           When False, it is assumed that the database does not enforce
           referential integrity and will not be issuing its own CASCADE
           operation for an update.  The :class:`.Mapper` here will
           emit an UPDATE statement for the dependent columns during a
           primary key change.
           
           See also:
           
           :ref:`passive_updates` - description of a similar feature as 
           used with :func:`.relationship`

        :param polymorphic_on: Specifies the column, attribute, or 
          SQL expression used to determine the target class for an 
          incoming row, when inheriting classes are present.
          
          This value is commonly a :class:`.Column` object that's
          present in the mapped :class:`.Table`::
          
            class Employee(Base):
                __tablename__ = 'employee'
                
                id = Column(Integer, primary_key=True)
                discriminator = Column(String(50))
                
                __mapper_args__ = {
                    "polymorphic_on":discriminator,
                    "polymorphic_identity":"employee"
                }
        
          It may also be specified
          as a SQL expression, as in this example where we 
          use the :func:`.case` construct to provide a conditional
          approach::

            class Employee(Base):
                __tablename__ = 'employee'
                
                id = Column(Integer, primary_key=True)
                discriminator = Column(String(50))
                
                __mapper_args__ = {
                    "polymorphic_on":case([
                        (discriminator == "EN", "engineer"),
                        (discriminator == "MA", "manager"),
                    ], else_="employee"),
                    "polymorphic_identity":"employee"
                }
        
          It may also refer to any attribute 
          configured with :func:`.column_property`, or to the
          string name of one::
            
                class Employee(Base):
                    __tablename__ = 'employee'
                
                    id = Column(Integer, primary_key=True)
                    discriminator = Column(String(50))
                    employee_type = column_property(
                        case([
                            (discriminator == "EN", "engineer"),
                            (discriminator == "MA", "manager"),
                        ], else_="employee")
                    )
                
                    __mapper_args__ = {
                        "polymorphic_on":employee_type,
                        "polymorphic_identity":"employee"
                    }

          .. versionchanged:: 0.7.4
              ``polymorphic_on`` may be specified as a SQL expression,
              or refer to any attribute configured with
              :func:`.column_property`, or to the string name of one.

          When setting ``polymorphic_on`` to reference an
          attribute or expression that's not present in the
          locally mapped :class:`.Table`, yet the value 
          of the discriminator should be persisted to the database, 
          the value of the
          discriminator is not automatically set on new
          instances; this must be handled by the user,
          either through manual means or via event listeners.
          A typical approach to establishing such a listener
          looks like::

                from sqlalchemy import event
                from sqlalchemy.orm import object_mapper
            
                @event.listens_for(Employee, "init", propagate=True)
                def set_identity(instance, *arg, **kw):
                    mapper = object_mapper(instance)
                    instance.discriminator = mapper.polymorphic_identity
        
          Where above, we assign the value of ``polymorphic_identity``
          for the mapped class to the ``discriminator`` attribute,
          thus persisting the value to the ``discriminator`` column
          in the database.
      
          See also:
      
          :ref:`inheritance_toplevel`
        
        :param polymorphic_identity: Specifies the value which 
          identifies this particular class as returned by the
          column expression referred to by the ``polymorphic_on``
          setting.  As rows are received, the value corresponding
          to the ``polymorphic_on`` column expression is compared
          to this value, indicating which subclass should 
          be used for the newly reconstructed object.

        :param properties: A dictionary mapping the string names of object
           attributes to :class:`.MapperProperty` instances, which define the
           persistence behavior of that attribute.  Note that :class:`.Column`
           objects present in
           the mapped :class:`.Table` are automatically placed into
           ``ColumnProperty`` instances upon mapping, unless overridden.
           When using Declarative, this argument is passed automatically,
           based on all those :class:`.MapperProperty` instances declared
           in the declared class body.

        :param primary_key: A list of :class:`.Column` objects which define the
           primary key to be used against this mapper's selectable unit.
           This is normally simply the primary key of the ``local_table``, but
           can be overridden here.

        :param version_id_col: A :class:`.Column` 
           that will be used to keep a running version id of mapped entities
           in the database.  This is used during save operations to ensure that
           no other thread or process has updated the instance during the
           lifetime of the entity, else a :class:`~sqlalchemy.orm.exc.StaleDataError` 
           exception is
           thrown.  By default the column must be of :class:`.Integer` type,
           unless ``version_id_generator`` specifies a new generation
           algorithm.

        :param version_id_generator: A callable which defines the algorithm
            used to generate new version ids. Defaults to an integer
            generator. Can be replaced with one that generates timestamps,
            uuids, etc. e.g.::

                import uuid

                class MyClass(Base):
                    __tablename__ = 'mytable'
                    id = Column(Integer, primary_key=True)
                    version_uuid = Column(String(32))
                    
                    __mapper_args__ = {
                        'version_id_col':version_uuid,
                        'version_id_generator':lambda version:uuid.uuid4().hex
                    }

            The callable receives the current version identifier as its 
            single argument.

        :param with_polymorphic: A tuple in the form ``(<classes>,
            <selectable>)`` indicating the default style of "polymorphic"
            loading, that is, which tables are queried at once. <classes> is
            any single or list of mappers and/or classes indicating the
            inherited classes that should be loaded at once. The special value
            ``'*'`` may be used to indicate all descending classes should be
            loaded immediately. The second tuple argument <selectable>
            indicates a selectable that will be used to query for multiple
            classes. 
            
            See also:
            
            :ref:`concrete_inheritance` - typically uses ``with_polymorphic``
            to specify a UNION statement to select from.
            
            :ref:`with_polymorphic` - usage example of the related 
            :meth:`.Query.with_polymorphic` method
            
    """
    return Mapper(class_, local_table, *args, **params)

def synonym(name, map_column=False, descriptor=None, 
                        comparator_factory=None, doc=None):
    """Denote an attribute name as a synonym to a mapped property.

    .. versionchanged:: 0.7
        :func:`.synonym` is superseded by the :mod:`~sqlalchemy.ext.hybrid`
        extension.  See  the documentation for hybrids
        at :ref:`hybrids_toplevel`.

    Used with the ``properties`` dictionary sent to
    :func:`~sqlalchemy.orm.mapper`::

        class MyClass(object):
            def _get_status(self):
                return self._status
            def _set_status(self, value):
                self._status = value
            status = property(_get_status, _set_status)

        mapper(MyClass, sometable, properties={
            "status":synonym("_status", map_column=True)
        })
    
    Above, the ``status`` attribute of MyClass will produce
    expression behavior against the table column named ``status``,
    using the Python attribute ``_status`` on the mapped class
    to represent the underlying value.

    :param name: the name of the existing mapped property, which can be
      any other ``MapperProperty`` including column-based properties and
      relationships.

    :param map_column: if ``True``, an additional ``ColumnProperty`` is created
      on the mapper automatically, using the synonym's name as the keyname of
      the property, and the keyname of this ``synonym()`` as the name of the
      column to map.

    """
    return SynonymProperty(name, map_column=map_column, 
                            descriptor=descriptor, 
                            comparator_factory=comparator_factory,
                            doc=doc)

def comparable_property(comparator_factory, descriptor=None):
    """Provides a method of applying a :class:`.PropComparator` 
    to any Python descriptor attribute.

    .. versionchanged:: 0.7
        :func:`.comparable_property` is superseded by
        the :mod:`~sqlalchemy.ext.hybrid` extension.  See the example 
        at :ref:`hybrid_custom_comparators`.

    Allows any Python descriptor to behave like a SQL-enabled 
    attribute when used at the class level in queries, allowing
    redefinition of expression operator behavior.
    
    In the example below we redefine :meth:`.PropComparator.operate`
    to wrap both sides of an expression in ``func.lower()`` to produce
    case-insensitive comparison::

        from sqlalchemy.orm import comparable_property
        from sqlalchemy.orm.interfaces import PropComparator
        from sqlalchemy.sql import func
        from sqlalchemy import Integer, String, Column
        from sqlalchemy.ext.declarative import declarative_base

        class CaseInsensitiveComparator(PropComparator):
            def __clause_element__(self):
                return self.prop
            
            def operate(self, op, other):
                return op(
                    func.lower(self.__clause_element__()),
                    func.lower(other)
                )

        Base = declarative_base()

        class SearchWord(Base):
            __tablename__ = 'search_word'
            id = Column(Integer, primary_key=True)
            word = Column(String)
            word_insensitive = comparable_property(lambda prop, mapper:
                                    CaseInsensitiveComparator(mapper.c.word, mapper)
                                )

    
    A mapping like the above allows the ``word_insensitive`` attribute 
    to render an expression like::
    
        >>> print SearchWord.word_insensitive == "Trucks"
        lower(search_word.word) = lower(:lower_1)
        
    :param comparator_factory:
      A PropComparator subclass or factory that defines operator behavior
      for this property.

    :param descriptor:
      Optional when used in a ``properties={}`` declaration.  The Python
      descriptor or property to layer comparison behavior on top of.

      The like-named descriptor will be automatically retrieved from the
      mapped class if left blank in a ``properties`` declaration.

    """
    return ComparableProperty(comparator_factory, descriptor)

@sa_util.deprecated("0.7", message=":func:`.compile_mappers` "
                            "is renamed to :func:`.configure_mappers`")
def compile_mappers():
    """Initialize the inter-mapper relationships of all mappers that have been defined."""

    configure_mappers()

def clear_mappers():
    """Remove all mappers from all classes.

    This function removes all instrumentation from classes and disposes
    of their associated mappers.  Once called, the classes are unmapped 
    and can be later re-mapped with new mappers.

    :func:`.clear_mappers` is *not* for normal use, as there is literally no
    valid usage for it outside of very specific testing scenarios. Normally,
    mappers are permanent structural components of user-defined classes, and
    are never discarded independently of their class.  If a mapped class itself
    is garbage collected, its mapper is automatically disposed of as well. As
    such, :func:`.clear_mappers` is only for usage in test suites that re-use
    the same classes with different mappings, which is itself an extremely rare
    use case - the only such use case is in fact SQLAlchemy's own test suite,
    and possibly the test suites of other ORM extension libraries which 
    intend to test various combinations of mapper construction upon a fixed
    set of classes.

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

def joinedload(*keys, **kw):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into an joined eager load.

    .. versionchanged:: 0.6beta3
        This function is known as :func:`eagerload` in all versions
        of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4
        series. :func:`eagerload` will remain available for the foreseeable
        future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::

        # joined-load the "orders" collection on "User"
        query(User).options(joinedload(User.orders))

        # joined-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those 
        # remain lazily loaded.
        query(Order).options(joinedload(Order.items, Item.keywords))

        # to joined-load across both, use joinedload_all()
        query(Order).options(joinedload_all(Order.items, Item.keywords))

        # set the default strategy to be 'joined'
        query(Order).options(joinedload('*'))

    :func:`joinedload` also accepts a keyword argument `innerjoin=True` which
    indicates using an inner join instead of an outer::

        query(Order).options(joinedload(Order.user, innerjoin=True))

    .. note:: 
    
       The join created by :func:`joinedload` is anonymously aliased such that
       it **does not affect the query results**.   An :meth:`.Query.order_by`
       or :meth:`.Query.filter` call **cannot** reference these aliased
       tables - so-called "user space" joins are constructed using 
       :meth:`.Query.join`.   The rationale for this is that :func:`joinedload` is only
       applied in order to affect how related objects or collections are loaded
       as an optimizing detail - it can be added or removed with no impact
       on actual results.   See the section :ref:`zen_of_eager_loading` for 
       a detailed description of how this is used, including how to use a single 
       explicit JOIN for filtering/ordering and eager loading simultaneously.

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

def joinedload_all(*keys, **kw):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path or series of mapped attributes 
    into an joined eager load.

    .. versionchanged:: 0.6beta3
        This function is known as :func:`eagerload_all` in all versions
        of SQLAlchemy prior to version 0.6beta3, including the 0.5 and 0.4
        series. :func:`eagerload_all` will remain available for the
        foreseeable future in order to enable cross-compatibility.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(joinedload_all('orders.items.keywords'))...

    will set all of ``orders``, ``orders.items``, and ``orders.items.keywords`` to
    load in one joined eager load.

    Individual descriptors are accepted as arguments as well::

        query.options(joinedload_all(User.orders, Order.items, Item.keywords))

    The keyword arguments accept a flag `innerjoin=True|False` which will
    override the value of the `innerjoin` flag specified on the
    relationship().

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
    of the given name or series of mapped attributes 
    into an subquery eager load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    examples::

        # subquery-load the "orders" collection on "User"
        query(User).options(subqueryload(User.orders))

        # subquery-load the "keywords" collection on each "Item",
        # but not the "items" collection on "Order" - those 
        # remain lazily loaded.
        query(Order).options(subqueryload(Order.items, Item.keywords))

        # to subquery-load across both, use subqueryload_all()
        query(Order).options(subqueryload_all(Order.items, Item.keywords))

        # set the default strategy to be 'subquery'
        query(Order).options(subqueryload('*'))

    See also:  :func:`joinedload`, :func:`lazyload`

    """
    return strategies.EagerLazyOption(keys, lazy="subquery")

def subqueryload_all(*keys):
    """Return a ``MapperOption`` that will convert all properties along the
    given dot-separated path or series of mapped attributes 
    into a subquery eager load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    For example::

        query.options(subqueryload_all('orders.items.keywords'))...

    will set all of ``orders``, ``orders.items``, and ``orders.items.keywords`` to
    load in one subquery eager load.

    Individual descriptors are accepted as arguments as well::

        query.options(subqueryload_all(User.orders, Order.items,
        Item.keywords))

    See also:  :func:`joinedload_all`, :func:`lazyload`, :func:`immediateload`

    """
    return strategies.EagerLazyOption(keys, lazy="subquery", chained=True)

def lazyload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given
    name or series of mapped attributes into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

    """
    return strategies.EagerLazyOption(keys, lazy=True)

def lazyload_all(*keys):
    """Return a ``MapperOption`` that will convert all the properties
    along the given dot-separated path or series of mapped attributes 
    into a lazy load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`eagerload`, :func:`subqueryload`, :func:`immediateload`

    """
    return strategies.EagerLazyOption(keys, lazy=True, chained=True)

def noload(*keys):
    """Return a ``MapperOption`` that will convert the property of the
    given name or series of mapped attributes into a non-load.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`, 
    :func:`subqueryload`, :func:`immediateload`

    """
    return strategies.EagerLazyOption(keys, lazy=None)

def immediateload(*keys):
    """Return a ``MapperOption`` that will convert the property of the given 
    name or series of mapped attributes into an immediate load.
    
    The "immediate" load means the attribute will be fetched
    with a separate SELECT statement per parent in the 
    same way as lazy loading - except the loader is guaranteed
    to be called at load time before the parent object
    is returned in the result.
    
    The normal behavior of lazy loading applies - if
    the relationship is a simple many-to-one, and the child
    object is already present in the :class:`.Session`,
    no SELECT statement will be emitted.

    Used with :meth:`~sqlalchemy.orm.query.Query.options`.

    See also:  :func:`lazyload`, :func:`eagerload`, :func:`subqueryload`

    .. versionadded:: 0.6.5

    """
    return strategies.EagerLazyOption(keys, lazy='immediate')

def contains_alias(alias):
    """Return a :class:`.MapperOption` that will indicate to the query that
    the main table has been aliased.

    This is used in the very rare case that :func:`.contains_eager`
    is being used in conjunction with a user-defined SELECT 
    statement that aliases the parent table.  E.g.::

        # define an aliased UNION called 'ulist'
        statement = users.select(users.c.user_id==7).\\
                        union(users.select(users.c.user_id>7)).\\
                        alias('ulist')

        # add on an eager load of "addresses"
        statement = statement.outerjoin(addresses).\\
                        select().apply_labels()

        # create query, indicating "ulist" will be an 
        # alias for the main table, "addresses" 
        # property should be eager loaded
        query = session.query(User).options(
                                contains_alias('ulist'), 
                                contains_eager('addresses'))

        # then get results via the statement
        results = query.from_statement(statement).all()

    :param alias: is the string name of an alias, or a 
     :class:`~.sql.expression.Alias` object representing 
     the alias.

    """
    return AliasOption(alias)

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

    :func:`contains_eager` also accepts an `alias` argument, which is the
    string name of an alias, an :func:`~sqlalchemy.sql.expression.alias`
    construct, or an :func:`~sqlalchemy.orm.aliased` construct. Use this when
    the eagerly-loaded rows are to come from an aliased table::

        user_alias = aliased(User)
        sess.query(Order).\\
                join((user_alias, Order.user)).\\
                options(contains_eager(Order.user, alias=user_alias))

    See also :func:`eagerload` for the "automatic" version of this 
    functionality.

    For additional examples of :func:`contains_eager` see
    :ref:`contains_eager`.

    """
    alias = kwargs.pop('alias', None)
    if kwargs:
        raise exceptions.ArgumentError('Invalid kwargs for contains_eag'
                'er: %r' % kwargs.keys())
    return strategies.EagerLazyOption(keys, lazy='joined',
            propagate_to_loaders=False, chained=True), \
        strategies.LoadEagerFromAliasOption(keys, alias=alias, chained=True)

def defer(*key):
    """Return a :class:`.MapperOption` that will convert the column property
    of the given name into a deferred load.

    Used with :meth:`.Query.options`.
    
    e.g.::
    
        from sqlalchemy.orm import defer

        query(MyClass).options(defer("attribute_one"), 
                            defer("attribute_two"))
    
    A class bound descriptor is also accepted::
        
        query(MyClass).options(
                            defer(MyClass.attribute_one), 
                            defer(MyClass.attribute_two))
    
    A "path" can be specified onto a related or collection object using a
    dotted name. The :func:`.orm.defer` option will be applied to that object
    when loaded::
    
        query(MyClass).options(
                            defer("related.attribute_one"), 
                            defer("related.attribute_two"))
    
    To specify a path via class, send multiple arguments::

        query(MyClass).options(
                            defer(MyClass.related, MyOtherClass.attribute_one), 
                            defer(MyClass.related, MyOtherClass.attribute_two))
    
    See also:
    
    :ref:`deferred`

    :param \*key: A key representing an individual path.   Multiple entries
     are accepted to allow a multiple-token path for a single target, not
     multiple targets.

    """
    return strategies.DeferredOption(key, defer=True)

def undefer(*key):
    """Return a :class:`.MapperOption` that will convert the column property
    of the given name into a non-deferred (regular column) load.

    Used with :meth:`.Query.options`.
    
    e.g.::
    
        from sqlalchemy.orm import undefer

        query(MyClass).options(undefer("attribute_one"), 
                                undefer("attribute_two"))
    
    A class bound descriptor is also accepted::
        
        query(MyClass).options(
                            undefer(MyClass.attribute_one), 
                            undefer(MyClass.attribute_two))
    
    A "path" can be specified onto a related or collection object using a
    dotted name. The :func:`.orm.undefer` option will be applied to that
    object when loaded::
    
        query(MyClass).options(
                            undefer("related.attribute_one"), 
                            undefer("related.attribute_two"))
    
    To specify a path via class, send multiple arguments::

        query(MyClass).options(
                            undefer(MyClass.related, MyOtherClass.attribute_one), 
                            undefer(MyClass.related, MyOtherClass.attribute_two))
    
    See also:
    
    :func:`.orm.undefer_group` as a means to "undefer" a group
    of attributes at once.
    
    :ref:`deferred`
    
    :param \*key: A key representing an individual path.   Multiple entries
     are accepted to allow a multiple-token path for a single target, not
     multiple targets.

    """
    return strategies.DeferredOption(key, defer=False)

def undefer_group(name):
    """Return a :class:`.MapperOption` that will convert the given group of deferred
    column properties into a non-deferred (regular column) load.

    Used with :meth:`.Query.options`.
    
    e.g.::
    
        query(MyClass).options(undefer("group_one"))

    See also:
    
    :ref:`deferred`
    
    :param name: String name of the deferred group.   This name is 
     established using the "group" name to the :func:`.orm.deferred` 
     configurational function.

    """
    return strategies.UndeferGroupOption(name)

from sqlalchemy import util as _sa_util
_sa_util.importlater.resolve_all()
