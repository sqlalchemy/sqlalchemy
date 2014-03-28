# orm/relationships.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Heuristics related to join conditions as used in
:func:`.relationship`.

Provides the :class:`.JoinCondition` object, which encapsulates
SQL annotation and aliasing behavior focused on the `primaryjoin`
and `secondaryjoin` aspects of :func:`.relationship`.

"""

from .. import sql, util, exc as sa_exc, schema, log

from .util import CascadeOptions, _orm_annotate, _orm_deannotate
from . import dependency
from . import attributes
from ..sql.util import (
    ClauseAdapter,
    join_condition, _shallow_annotate, visit_binary_product,
    _deep_deannotate, selectables_overlap
    )
from ..sql import operators, expression, visitors
from .interfaces import MANYTOMANY, MANYTOONE, ONETOMANY, StrategizedProperty, PropComparator
from ..inspection import inspect
from . import mapper as mapperlib

def remote(expr):
    """Annotate a portion of a primaryjoin expression
    with a 'remote' annotation.

    See the section :ref:`relationship_custom_foreign` for a
    description of use.

    .. versionadded:: 0.8

    .. seealso::

        :ref:`relationship_custom_foreign`

        :func:`.foreign`

    """
    return _annotate_columns(expression._clause_element_as_expr(expr),
                    {"remote": True})


def foreign(expr):
    """Annotate a portion of a primaryjoin expression
    with a 'foreign' annotation.

    See the section :ref:`relationship_custom_foreign` for a
    description of use.

    .. versionadded:: 0.8

    .. seealso::

        :ref:`relationship_custom_foreign`

        :func:`.remote`

    """

    return _annotate_columns(expression._clause_element_as_expr(expr),
                        {"foreign": True})


@log.class_logger
@util.langhelpers.dependency_for("sqlalchemy.orm.properties")
class RelationshipProperty(StrategizedProperty):
    """Describes an object property that holds a single item or list
    of items that correspond to a related database table.

    Public constructor is the :func:`.orm.relationship` function.

    See also:

    :ref:`relationship_config_toplevel`

    """

    strategy_wildcard_key = 'relationship'

    _dependency_processor = None

    def __init__(self, argument,
        secondary=None, primaryjoin=None,
        secondaryjoin=None,
        foreign_keys=None,
        uselist=None,
        order_by=False,
        backref=None,
        back_populates=None,
        post_update=False,
        cascade=False, extension=None,
        viewonly=False, lazy=True,
        collection_class=None, passive_deletes=False,
        passive_updates=True, remote_side=None,
        enable_typechecks=True, join_depth=None,
        comparator_factory=None,
        single_parent=False, innerjoin=False,
        distinct_target_key=None,
        doc=None,
        active_history=False,
        cascade_backrefs=True,
        load_on_pending=False,
        strategy_class=None, _local_remote_pairs=None,
        query_class=None,
            info=None):
        """Provide a relationship between two mapped classes.

        This corresponds to a parent-child or associative table relationship.  The
        constructed class is an instance of :class:`.RelationshipProperty`.

        A typical :func:`.relationship`, used in a classical mapping::

           mapper(Parent, properties={
             'children': relationship(Child)
           })

        Some arguments accepted by :func:`.relationship` optionally accept a
        callable function, which when called produces the desired value.
        The callable is invoked by the parent :class:`.Mapper` at "mapper
        initialization" time, which happens only when mappers are first used, and
        is assumed to be after all mappings have been constructed.  This can be
        used to resolve order-of-declaration and other dependency issues, such as
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
        classes to be automatic via their string name, and removes the need to
        import related classes at all into the local module space::

            from sqlalchemy.ext.declarative import declarative_base

            Base = declarative_base()

            class Parent(Base):
                __tablename__ = 'parent'
                id = Column(Integer, primary_key=True)
                children = relationship("Child", order_by="Child.id")

        .. seealso::

          :ref:`relationship_config_toplevel` - Full introductory and reference
          documentation for :func:`.relationship`.

          :ref:`orm_tutorial_relationship` - ORM tutorial introduction.

        :param argument:
          a mapped class, or actual :class:`.Mapper` instance, representing the
          target of the relationship.

          :paramref:`~.relationship.argument` may also be passed as a callable function
          which is evaluated at mapper initialization time, and may be passed as a
          Python-evaluable string when using Declarative.

          .. seealso::

            :ref:`declarative_configuring_relationships` - further detail
            on relationship configuration when using Declarative.

        :param secondary:
          for a many-to-many relationship, specifies the intermediary
          table, and is typically an instance of :class:`.Table`.
          In less common circumstances, the argument may also be specified
          as an :class:`.Alias` construct, or even a :class:`.Join` construct.

          :paramref:`~.relationship.secondary` may
          also be passed as a callable function which is evaluated at
          mapper initialization time.  When using Declarative, it may also
          be a string argument noting the name of a :class:`.Table` that is
          present in the :class:`.MetaData` collection associated with the
          parent-mapped :class:`.Table`.

          The :paramref:`~.relationship.secondary` keyword argument is typically
          applied in the case where the intermediary :class:`.Table` is not
          otherwise exprssed in any direct class mapping. If the "secondary" table
          is also explicitly mapped elsewhere
          (e.g. as in :ref:`association_pattern`), one should consider applying
          the :paramref:`~.relationship.viewonly` flag so that this :func:`.relationship`
          is not used for persistence operations which may conflict with those
          of the association object pattern.

          .. seealso::

              :ref:`relationships_many_to_many` - Reference example of "many to many".

              :ref:`orm_tutorial_many_to_many` - ORM tutorial introduction to
              many-to-many relationships.

              :ref:`self_referential_many_to_many` - Specifics on using many-to-many
              in a self-referential case.

              :ref:`declarative_many_to_many` - Additional options when using
              Declarative.

              :ref:`association_pattern` - an alternative to :paramref:`~.relationship.secondary`
              when composing association table relationships, allowing additional
              attributes to be specified on the association table.

              :ref:`composite_secondary_join` - a lesser-used pattern which in some
              cases can enable complex :func:`.relationship` SQL conditions
              to be used.

          .. versionadded:: 0.9.2 :paramref:`~.relationship.secondary` works
             more effectively when referring to a :class:`.Join` instance.

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
          :func:`.backref` object to control the configuration of the
          new relationship.

          .. seealso::

            :ref:`relationships_backref` - Introductory documentation and
            examples.

            :paramref:`~.relationship.back_populates` - alternative form
            of backref specification.

            :func:`.backref` - allows control over :func:`.relationship`
            configuration when using :paramref:`~.relationship.backref`.


        :param back_populates:
          Takes a string name and has the same meaning as :paramref:`~.relationship.backref`,
          except the complementing property is **not** created automatically,
          and instead must be configured explicitly on the other mapper.  The
          complementing property should also indicate :paramref:`~.relationship.back_populates`
          to this relationship to ensure proper functioning.

          .. seealso::

            :ref:`relationships_backref` - Introductory documentation and
            examples.

            :paramref:`~.relationship.backref` - alternative form
            of backref specification.

        :param cascade:
          a comma-separated list of cascade rules which determines how
          Session operations should be "cascaded" from parent to child.
          This defaults to ``False``, which means the default cascade
          should be used - this default cascade is ``"save-update, merge"``.

          The available cascades are ``save-update``, ``merge``,
          ``expunge``, ``delete``, ``delete-orphan``, and ``refresh-expire``.
          An additional option, ``all`` indicates shorthand for
          ``"save-update, merge, refresh-expire,
          expunge, delete"``, and is often used as in ``"all, delete-orphan"``
          to indicate that related objects should follow along with the
          parent object in all cases, and be deleted when de-associated.

          .. seealso::

            :ref:`unitofwork_cascades` - Full detail on each of the available
            cascade options.

            :ref:`tutorial_delete_cascade` - Tutorial example describing
            a delete cascade.

        :param cascade_backrefs=True:
          a boolean value indicating if the ``save-update`` cascade should
          operate along an assignment event intercepted by a backref.
          When set to ``False``, the attribute managed by this relationship
          will not cascade an incoming transient object into the session of a
          persistent parent, if the event is received via backref.

          .. seealso::

            :ref:`backref_cascade` - Full discussion and examples on how
            the :paramref:`~.relationship.cascade_backrefs` option is used.

        :param collection_class:
          a class or callable that returns a new list-holding object. will
          be used in place of a plain list for storing elements.

          .. seealso::

            :ref:`custom_collections` - Introductory documentation and
            examples.

        :param comparator_factory:
          a class which extends :class:`.RelationshipProperty.Comparator` which
          provides custom SQL clause generation for comparison operations.

          .. seealso::

            :class:`.PropComparator` - some detail on redefining comparators
            at this level.

            :ref:`custom_comparators` - Brief intro to this feature.


        :param distinct_target_key=None:
          Indicate if a "subquery" eager load should apply the DISTINCT
          keyword to the innermost SELECT statement.  When left as ``None``,
          the DISTINCT keyword will be applied in those cases when the target
          columns do not comprise the full primary key of the target table.
          When set to ``True``, the DISTINCT keyword is applied to the innermost
          SELECT unconditionally.

          It may be desirable to set this flag to False when the DISTINCT is
          reducing performance of the innermost subquery beyond that of what
          duplicate innermost rows may be causing.

          .. versionadded:: 0.8.3 - :paramref:`~.relationship.distinct_target_key`
             allows the
             subquery eager loader to apply a DISTINCT modifier to the
             innermost SELECT.

          .. versionchanged:: 0.9.0 - :paramref:`~.relationship.distinct_target_key`
             now defaults to ``None``, so that the feature enables itself automatically for
             those cases where the innermost query targets a non-unique
             key.

          .. seealso::

            :ref:`loading_toplevel` - includes an introduction to subquery
            eager loading.

        :param doc:
          docstring which will be applied to the resulting descriptor.

        :param extension:
          an :class:`.AttributeExtension` instance, or list of extensions,
          which will be prepended to the list of attribute listeners for
          the resulting descriptor placed on the class.

          .. deprecated:: 0.7 Please see :class:`.AttributeEvents`.

        :param foreign_keys:

          a list of columns which are to be used as "foreign key"
          columns, or columns which refer to the value in a remote
          column, within the context of this :func:`.relationship`
          object's :paramref:`~.relationship.primaryjoin` condition.
          That is, if the :paramref:`~.relationship.primaryjoin`
          condition of this :func:`.relationship` is ``a.id ==
          b.a_id``, and the values in ``b.a_id`` are required to be
          present in ``a.id``, then the "foreign key" column of this
          :func:`.relationship` is ``b.a_id``.

          In normal cases, the :paramref:`~.relationship.foreign_keys`
          parameter is **not required.** :func:`.relationship` will
          automatically determine which columns in the
          :paramref:`~.relationship.primaryjoin` conditition are to be
          considered "foreign key" columns based on those
          :class:`.Column` objects that specify :class:`.ForeignKey`,
          or are otherwise listed as referencing columns in a
          :class:`.ForeignKeyConstraint` construct.
          :paramref:`~.relationship.foreign_keys` is only needed when:

            1. There is more than one way to construct a join from the local
               table to the remote table, as there are multiple foreign key
               references present.  Setting ``foreign_keys`` will limit the
               :func:`.relationship` to consider just those columns specified
               here as "foreign".

               .. versionchanged:: 0.8
                    A multiple-foreign key join ambiguity can be resolved by
                    setting the :paramref:`~.relationship.foreign_keys` parameter alone, without the
                    need to explicitly set :paramref:`~.relationship.primaryjoin` as well.

            2. The :class:`.Table` being mapped does not actually have
               :class:`.ForeignKey` or :class:`.ForeignKeyConstraint`
               constructs present, often because the table
               was reflected from a database that does not support foreign key
               reflection (MySQL MyISAM).

            3. The :paramref:`~.relationship.primaryjoin` argument is used to construct a non-standard
               join condition, which makes use of columns or expressions that do
               not normally refer to their "parent" column, such as a join condition
               expressed by a complex comparison using a SQL function.

          The :func:`.relationship` construct will raise informative
          error messages that suggest the use of the
          :paramref:`~.relationship.foreign_keys` parameter when
          presented with an ambiguous condition.   In typical cases,
          if :func:`.relationship` doesn't raise any exceptions, the
          :paramref:`~.relationship.foreign_keys` parameter is usually
          not needed.

          :paramref:`~.relationship.foreign_keys` may also be passed as a callable function
          which is evaluated at mapper initialization time, and may be passed as a
          Python-evaluable string when using Declarative.

          .. seealso::

            :ref:`relationship_foreign_keys`

            :ref:`relationship_custom_foreign`

            :func:`.foreign` - allows direct annotation of the "foreign" columns
            within a :paramref:`~.relationship.primaryjoin` condition.

          .. versionadded:: 0.8
              The :func:`.foreign` annotation can also be applied
              directly to the :paramref:`~.relationship.primaryjoin` expression, which is an alternate,
              more specific system of describing which columns in a particular
              :paramref:`~.relationship.primaryjoin` should be considered "foreign".

        :param info: Optional data dictionary which will be populated into the
            :attr:`.MapperProperty.info` attribute of this object.

            .. versionadded:: 0.8

        :param innerjoin=False:
          when ``True``, joined eager loads will use an inner join to join
          against related tables instead of an outer join.  The purpose
          of this option is generally one of performance, as inner joins
          generally perform better than outer joins.

          This flag can be set to ``True`` when the relationship references an
          object via many-to-one using local foreign keys that are not nullable,
          or when the reference is one-to-one or a collection that is guaranteed
          to have one or at least one entry.

          If the joined-eager load is chained onto an existing LEFT OUTER JOIN,
          ``innerjoin=True`` will be bypassed and the join will continue to
          chain as LEFT OUTER JOIN so that the results don't change.  As an alternative,
          specify the value ``"nested"``.  This will instead nest the join
          on the right side, e.g. using the form "a LEFT OUTER JOIN (b JOIN c)".

          .. versionadded:: 0.9.4 Added ``innerjoin="nested"`` option to support
             nesting of eager "inner" joins.

          .. seealso::

            :ref:`what_kind_of_loading` - Discussion of some details of
            various loader options.

            :paramref:`.joinedload.innerjoin` - loader option version

        :param join_depth:
          when non-``None``, an integer value indicating how many levels
          deep "eager" loaders should join on a self-referring or cyclical
          relationship.  The number counts how many times the same Mapper
          shall be present in the loading condition along a particular join
          branch.  When left at its default of ``None``, eager loaders
          will stop chaining when they encounter a the same target mapper
          which is already higher up in the chain.  This option applies
          both to joined- and subquery- eager loaders.

          .. seealso::

            :ref:`self_referential_eager_loading` - Introductory documentation
            and examples.

        :param lazy='select': specifies
          how the related items should be loaded.  Default value is
          ``select``.  Values include:

          * ``select`` - items should be loaded lazily when the property is first
            accessed, using a separate SELECT statement, or identity map
            fetch for simple many-to-one references.

          * ``immediate`` - items should be loaded as the parents are loaded,
            using a separate SELECT statement, or identity map fetch for
            simple many-to-one references.

          * ``joined`` - items should be loaded "eagerly" in the same query as
            that of the parent, using a JOIN or LEFT OUTER JOIN.  Whether
            the join is "outer" or not is determined by the
            :paramref:`~.relationship.innerjoin` parameter.

          * ``subquery`` - items should be loaded "eagerly" as the parents are
            loaded, using one additional SQL statement, which issues a JOIN to a
            subquery of the original statement, for each collection requested.

          * ``noload`` - no loading should occur at any time.  This is to
            support "write-only" attributes, or attributes which are
            populated in some manner specific to the application.

          * ``dynamic`` - the attribute will return a pre-configured
            :class:`.Query` object for all read
            operations, onto which further filtering operations can be
            applied before iterating the results.  See
            the section :ref:`dynamic_relationship` for more details.

          * True - a synonym for 'select'

          * False - a synonym for 'joined'

          * None - a synonym for 'noload'

          .. seealso::

            :doc:`/orm/loading` - Full documentation on relationship loader
            configuration.

            :ref:`dynamic_relationship` - detail on the ``dynamic`` option.

        :param load_on_pending=False:
          Indicates loading behavior for transient or pending parent objects.

          When set to ``True``, causes the lazy-loader to
          issue a query for a parent object that is not persistent, meaning it has
          never been flushed.  This may take effect for a pending object when
          autoflush is disabled, or for a transient object that has been
          "attached" to a :class:`.Session` but is not part of its pending
          collection.

          The :paramref:`~.relationship.load_on_pending` flag does not improve behavior
          when the ORM is used normally - object references should be constructed
          at the object level, not at the foreign key level, so that they
          are present in an ordinary way before a flush proceeds.  This flag
          is not not intended for general use.

          .. seealso::

              :meth:`.Session.enable_relationship_loading` - this method establishes
              "load on pending" behavior for the whole object, and also allows
              loading on objects that remain transient or detached.

        :param order_by:
          indicates the ordering that should be applied when loading these
          items.  :paramref:`~.relationship.order_by` is expected to refer to one
          of the :class:`.Column`
          objects to which the target class is mapped, or
          the attribute itself bound to the target class which refers
          to the column.

          :paramref:`~.relationship.order_by` may also be passed as a callable function
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

           .. seealso::

                :ref:`passive_deletes` - Introductory documentation
                and examples.

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

          .. seealso::

              :ref:`passive_updates` - Introductory documentation and
              examples.

              :paramref:`.mapper.passive_updates` - a similar flag which
              takes effect for joined-table inheritance mappings.

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
          a flush operation returns an error that a "cyclical
          dependency" was detected, this is a cue that you might want to
          use :paramref:`~.relationship.post_update` to "break" the cycle.

          .. seealso::

              :ref:`post_update` - Introductory documentation and examples.

        :param primaryjoin:
          a SQL expression that will be used as the primary
          join of this child object against the parent object, or in a
          many-to-many relationship the join of the primary object to the
          association table. By default, this value is computed based on the
          foreign key relationships of the parent and child tables (or association
          table).

          :paramref:`~.relationship.primaryjoin` may also be passed as a callable function
          which is evaluated at mapper initialization time, and may be passed as a
          Python-evaluable string when using Declarative.

          .. seealso::

              :ref:`relationship_primaryjoin`

        :param remote_side:
          used for self-referential relationships, indicates the column or
          list of columns that form the "remote side" of the relationship.

          :paramref:`.relationship.remote_side` may also be passed as a callable function
          which is evaluated at mapper initialization time, and may be passed as a
          Python-evaluable string when using Declarative.

          .. versionchanged:: 0.8
              The :func:`.remote` annotation can also be applied
              directly to the ``primaryjoin`` expression, which is an alternate,
              more specific system of describing which columns in a particular
              ``primaryjoin`` should be considered "remote".

          .. seealso::

            :ref:`self_referential` - in-depth explaination of how
            :paramref:`~.relationship.remote_side`
            is used to configure self-referential relationships.

            :func:`.remote` - an annotation function that accomplishes the same
            purpose as :paramref:`~.relationship.remote_side`, typically
            when a custom :paramref:`~.relationship.primaryjoin` condition
            is used.

        :param query_class:
          a :class:`.Query` subclass that will be used as the base of the
          "appender query" returned by a "dynamic" relationship, that
          is, a relationship that specifies ``lazy="dynamic"`` or was
          otherwise constructed using the :func:`.orm.dynamic_loader`
          function.

          .. seealso::

            :ref:`dynamic_relationship` - Introduction to "dynamic" relationship
            loaders.

        :param secondaryjoin:
          a SQL expression that will be used as the join of
          an association table to the child object. By default, this value is
          computed based on the foreign key relationships of the association and
          child tables.

          :paramref:`~.relationship.secondaryjoin` may also be passed as a callable function
          which is evaluated at mapper initialization time, and may be passed as a
          Python-evaluable string when using Declarative.

          .. seealso::

              :ref:`relationship_primaryjoin`

        :param single_parent:
          when True, installs a validator which will prevent objects
          from being associated with more than one parent at a time.
          This is used for many-to-one or many-to-many relationships that
          should be treated either as one-to-one or one-to-many.  Its usage
          is optional, except for :func:`.relationship` constructs which
          are many-to-one or many-to-many and also
          specify the ``delete-orphan`` cascade option.  The :func:`.relationship`
          construct itself will raise an error instructing when this option
          is required.

          .. seealso::

            :ref:`unitofwork_cascades` - includes detail on when the
            :paramref:`~.relationship.single_parent` flag may be appropriate.

        :param uselist:
          a boolean that indicates if this property should be loaded as a
          list or a scalar. In most cases, this value is determined
          automatically by :func:`.relationship` at mapper configuration
          time, based on the type and direction
          of the relationship - one to many forms a list, many to one
          forms a scalar, many to many is a list. If a scalar is desired
          where normally a list would be present, such as a bi-directional
          one-to-one relationship, set :paramref:`~.relationship.uselist` to False.

          The :paramref:`~.relationship.uselist` flag is also available on an
          existing :func:`.relationship` construct as a read-only attribute, which
          can be used to determine if this :func:`.relationship` deals with
          collections or scalar attributes::

              >>> User.addresses.property.uselist
              True

          .. seealso::

              :ref:`relationships_one_to_one` - Introduction to the "one to one"
              relationship pattern, which is typically when the
              :paramref:`~.relationship.uselist` flag is needed.

        :param viewonly=False:
          when set to True, the relationship is used only for loading objects,
          and not for any persistence operation.  A :func:`.relationship`
          which specifies :paramref:`~.relationship.viewonly` can work
          with a wider range of SQL operations within the :paramref:`~.relationship.primaryjoin`
          condition, including operations that feature the use of
          a variety of comparison operators as well as SQL functions such
          as :func:`~.sql.expression.cast`.  The :paramref:`~.relationship.viewonly`
          flag is also of general use when defining any kind of :func:`~.relationship`
          that doesn't represent the full set of related objects, to prevent
          modifications of the collection from resulting in persistence operations.


        """

        self.uselist = uselist
        self.argument = argument
        self.secondary = secondary
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.post_update = post_update
        self.direction = None
        self.viewonly = viewonly
        self.lazy = lazy
        self.single_parent = single_parent
        self._user_defined_foreign_keys = foreign_keys
        self.collection_class = collection_class
        self.passive_deletes = passive_deletes
        self.cascade_backrefs = cascade_backrefs
        self.passive_updates = passive_updates
        self.remote_side = remote_side
        self.enable_typechecks = enable_typechecks
        self.query_class = query_class
        self.innerjoin = innerjoin
        self.distinct_target_key = distinct_target_key
        self.doc = doc
        self.active_history = active_history
        self.join_depth = join_depth
        self.local_remote_pairs = _local_remote_pairs
        self.extension = extension
        self.load_on_pending = load_on_pending
        self.comparator_factory = comparator_factory or \
                                    RelationshipProperty.Comparator
        self.comparator = self.comparator_factory(self, None)
        util.set_creation_order(self)

        if info is not None:
            self.info = info

        if strategy_class:
            self.strategy_class = strategy_class
        else:
            self.strategy_class = self._strategy_lookup(("lazy", self.lazy))

        self._reverse_property = set()

        self.cascade = cascade if cascade is not False \
                            else "save-update, merge"

        self.order_by = order_by

        self.back_populates = back_populates

        if self.back_populates:
            if backref:
                raise sa_exc.ArgumentError(
                            "backref and back_populates keyword arguments "
                            "are mutually exclusive")
            self.backref = None
        else:
            self.backref = backref

    def instrument_class(self, mapper):
        attributes.register_descriptor(
            mapper.class_,
            self.key,
            comparator=self.comparator_factory(self, mapper),
            parententity=mapper,
            doc=self.doc,
            )

    class Comparator(PropComparator):
        """Produce boolean, comparison, and other operators for
        :class:`.RelationshipProperty` attributes.

        See the documentation for :class:`.PropComparator` for a brief overview
        of ORM level operator definition.

        See also:

        :class:`.PropComparator`

        :class:`.ColumnProperty.Comparator`

        :class:`.ColumnOperators`

        :ref:`types_operators`

        :attr:`.TypeEngine.comparator_factory`

        """

        _of_type = None

        def __init__(self, prop, parentmapper, adapt_to_entity=None, of_type=None):
            """Construction of :class:`.RelationshipProperty.Comparator`
            is internal to the ORM's attribute mechanics.

            """
            self.prop = prop
            self._parentmapper = parentmapper
            self._adapt_to_entity = adapt_to_entity
            if of_type:
                self._of_type = of_type

        def adapt_to_entity(self, adapt_to_entity):
            return self.__class__(self.property, self._parentmapper,
                                  adapt_to_entity=adapt_to_entity,
                                    of_type=self._of_type)

        @util.memoized_property
        def mapper(self):
            """The target :class:`.Mapper` referred to by this
            :class:`.RelationshipProperty.Comparator`.

            This is the "target" or "remote" side of the
            :func:`.relationship`.

            """
            return self.property.mapper

        @util.memoized_property
        def _parententity(self):
            return self.property.parent

        def _source_selectable(self):
            if self._adapt_to_entity:
                return self._adapt_to_entity.selectable
            else:
                return self.property.parent._with_polymorphic_selectable

        def __clause_element__(self):
            adapt_from = self._source_selectable()
            if self._of_type:
                of_type = inspect(self._of_type).mapper
            else:
                of_type = None

            pj, sj, source, dest, \
            secondary, target_adapter = self.property._create_joins(
                            source_selectable=adapt_from,
                            source_polymorphic=True,
                            of_type=of_type)
            if sj is not None:
                return pj & sj
            else:
                return pj

        def of_type(self, cls):
            """Produce a construct that represents a particular 'subtype' of
            attribute for the parent class.

            Currently this is usable in conjunction with :meth:`.Query.join`
            and :meth:`.Query.outerjoin`.

            """
            return RelationshipProperty.Comparator(
                                        self.property,
                                        self._parentmapper,
                                        adapt_to_entity=self._adapt_to_entity,
                                        of_type=cls)

        def in_(self, other):
            """Produce an IN clause - this is not implemented
            for :func:`~.orm.relationship`-based attributes at this time.

            """
            raise NotImplementedError('in_() not yet supported for '
                    'relationships.  For a simple many-to-one, use '
                    'in_() against the set of foreign key values.')

        __hash__ = None

        def __eq__(self, other):
            """Implement the ``==`` operator.

            In a many-to-one context, such as::

              MyClass.some_prop == <some object>

            this will typically produce a
            clause such as::

              mytable.related_id == <some id>

            Where ``<some id>`` is the primary key of the given
            object.

            The ``==`` operator provides partial functionality for non-
            many-to-one comparisons:

            * Comparisons against collections are not supported.
              Use :meth:`~.RelationshipProperty.Comparator.contains`.
            * Compared to a scalar one-to-many, will produce a
              clause that compares the target columns in the parent to
              the given target.
            * Compared to a scalar many-to-many, an alias
              of the association table will be rendered as
              well, forming a natural join that is part of the
              main body of the query. This will not work for
              queries that go beyond simple AND conjunctions of
              comparisons, such as those which use OR. Use
              explicit joins, outerjoins, or
              :meth:`~.RelationshipProperty.Comparator.has` for
              more comprehensive non-many-to-one scalar
              membership tests.
            * Comparisons against ``None`` given in a one-to-many
              or many-to-many context produce a NOT EXISTS clause.

            """
            if isinstance(other, (util.NoneType, expression.Null)):
                if self.property.direction in [ONETOMANY, MANYTOMANY]:
                    return ~self._criterion_exists()
                else:
                    return _orm_annotate(self.property._optimized_compare(
                            None, adapt_source=self.adapter))
            elif self.property.uselist:
                raise sa_exc.InvalidRequestError("Can't compare a colle"
                        "ction to an object or collection; use "
                        "contains() to test for membership.")
            else:
                return _orm_annotate(self.property._optimized_compare(other,
                        adapt_source=self.adapter))

        def _criterion_exists(self, criterion=None, **kwargs):
            if getattr(self, '_of_type', None):
                info = inspect(self._of_type)
                target_mapper, to_selectable, is_aliased_class = \
                    info.mapper, info.selectable, info.is_aliased_class
                if self.property._is_self_referential and not is_aliased_class:
                    to_selectable = to_selectable.alias()

                single_crit = target_mapper._single_table_criterion
                if single_crit is not None:
                    if criterion is not None:
                        criterion = single_crit & criterion
                    else:
                        criterion = single_crit
            else:
                is_aliased_class = False
                to_selectable = None

            if self.adapter:
                source_selectable = self._source_selectable()
            else:
                source_selectable = None

            pj, sj, source, dest, secondary, target_adapter = \
                self.property._create_joins(dest_polymorphic=True,
                        dest_selectable=to_selectable,
                        source_selectable=source_selectable)

            for k in kwargs:
                crit = getattr(self.property.mapper.class_, k) == kwargs[k]
                if criterion is None:
                    criterion = crit
                else:
                    criterion = criterion & crit

            # annotate the *local* side of the join condition, in the case
            # of pj + sj this is the full primaryjoin, in the case of just
            # pj its the local side of the primaryjoin.
            if sj is not None:
                j = _orm_annotate(pj) & sj
            else:
                j = _orm_annotate(pj, exclude=self.property.remote_side)

            if criterion is not None and target_adapter and not is_aliased_class:
                # limit this adapter to annotated only?
                criterion = target_adapter.traverse(criterion)

            # only have the "joined left side" of what we
            # return be subject to Query adaption.  The right
            # side of it is used for an exists() subquery and
            # should not correlate or otherwise reach out
            # to anything in the enclosing query.
            if criterion is not None:
                criterion = criterion._annotate(
                    {'no_replacement_traverse': True})

            crit = j & sql.True_._ifnone(criterion)

            ex = sql.exists([1], crit, from_obj=dest).correlate_except(dest)
            if secondary is not None:
                ex = ex.correlate_except(secondary)
            return ex

        def any(self, criterion=None, **kwargs):
            """Produce an expression that tests a collection against
            particular criterion, using EXISTS.

            An expression like::

                session.query(MyClass).filter(
                    MyClass.somereference.any(SomeRelated.x==2)
                )


            Will produce a query like::

                SELECT * FROM my_table WHERE
                EXISTS (SELECT 1 FROM related WHERE related.my_id=my_table.id
                AND related.x=2)

            Because :meth:`~.RelationshipProperty.Comparator.any` uses
            a correlated subquery, its performance is not nearly as
            good when compared against large target tables as that of
            using a join.

            :meth:`~.RelationshipProperty.Comparator.any` is particularly
            useful for testing for empty collections::

                session.query(MyClass).filter(
                    ~MyClass.somereference.any()
                )

            will produce::

                SELECT * FROM my_table WHERE
                NOT EXISTS (SELECT 1 FROM related WHERE
                related.my_id=my_table.id)

            :meth:`~.RelationshipProperty.Comparator.any` is only
            valid for collections, i.e. a :func:`.relationship`
            that has ``uselist=True``.  For scalar references,
            use :meth:`~.RelationshipProperty.Comparator.has`.

            """
            if not self.property.uselist:
                raise sa_exc.InvalidRequestError(
                            "'any()' not implemented for scalar "
                            "attributes. Use has()."
                        )

            return self._criterion_exists(criterion, **kwargs)

        def has(self, criterion=None, **kwargs):
            """Produce an expression that tests a scalar reference against
            particular criterion, using EXISTS.

            An expression like::

                session.query(MyClass).filter(
                    MyClass.somereference.has(SomeRelated.x==2)
                )


            Will produce a query like::

                SELECT * FROM my_table WHERE
                EXISTS (SELECT 1 FROM related WHERE
                related.id==my_table.related_id AND related.x=2)

            Because :meth:`~.RelationshipProperty.Comparator.has` uses
            a correlated subquery, its performance is not nearly as
            good when compared against large target tables as that of
            using a join.

            :meth:`~.RelationshipProperty.Comparator.has` is only
            valid for scalar references, i.e. a :func:`.relationship`
            that has ``uselist=False``.  For collection references,
            use :meth:`~.RelationshipProperty.Comparator.any`.

            """
            if self.property.uselist:
                raise sa_exc.InvalidRequestError(
                            "'has()' not implemented for collections.  "
                            "Use any().")
            return self._criterion_exists(criterion, **kwargs)

        def contains(self, other, **kwargs):
            """Return a simple expression that tests a collection for
            containment of a particular item.

            :meth:`~.RelationshipProperty.Comparator.contains` is
            only valid for a collection, i.e. a
            :func:`~.orm.relationship` that implements
            one-to-many or many-to-many with ``uselist=True``.

            When used in a simple one-to-many context, an
            expression like::

                MyClass.contains(other)

            Produces a clause like::

                mytable.id == <some id>

            Where ``<some id>`` is the value of the foreign key
            attribute on ``other`` which refers to the primary
            key of its parent object. From this it follows that
            :meth:`~.RelationshipProperty.Comparator.contains` is
            very useful when used with simple one-to-many
            operations.

            For many-to-many operations, the behavior of
            :meth:`~.RelationshipProperty.Comparator.contains`
            has more caveats. The association table will be
            rendered in the statement, producing an "implicit"
            join, that is, includes multiple tables in the FROM
            clause which are equated in the WHERE clause::

                query(MyClass).filter(MyClass.contains(other))

            Produces a query like::

                SELECT * FROM my_table, my_association_table AS
                my_association_table_1 WHERE
                my_table.id = my_association_table_1.parent_id
                AND my_association_table_1.child_id = <some id>

            Where ``<some id>`` would be the primary key of
            ``other``. From the above, it is clear that
            :meth:`~.RelationshipProperty.Comparator.contains`
            will **not** work with many-to-many collections when
            used in queries that move beyond simple AND
            conjunctions, such as multiple
            :meth:`~.RelationshipProperty.Comparator.contains`
            expressions joined by OR. In such cases subqueries or
            explicit "outer joins" will need to be used instead.
            See :meth:`~.RelationshipProperty.Comparator.any` for
            a less-performant alternative using EXISTS, or refer
            to :meth:`.Query.outerjoin` as well as :ref:`ormtutorial_joins`
            for more details on constructing outer joins.

            """
            if not self.property.uselist:
                raise sa_exc.InvalidRequestError(
                            "'contains' not implemented for scalar "
                            "attributes.  Use ==")
            clause = self.property._optimized_compare(other,
                    adapt_source=self.adapter)

            if self.property.secondaryjoin is not None:
                clause.negation_clause = \
                    self.__negated_contains_or_equals(other)

            return clause

        def __negated_contains_or_equals(self, other):
            if self.property.direction == MANYTOONE:
                state = attributes.instance_state(other)

                def state_bindparam(x, state, col):
                    o = state.obj()  # strong ref
                    return sql.bindparam(x, unique=True, callable_=lambda: \
                        self.property.mapper._get_committed_attr_by_column(o, col))

                def adapt(col):
                    if self.adapter:
                        return self.adapter(col)
                    else:
                        return col

                if self.property._use_get:
                    return sql.and_(*[
                        sql.or_(
                            adapt(x) != state_bindparam(adapt(x), state, y),
                            adapt(x) == None)
                        for (x, y) in self.property.local_remote_pairs])

            criterion = sql.and_(*[x == y for (x, y) in
                                zip(
                                    self.property.mapper.primary_key,
                                    self.property.\
                                            mapper.\
                                            primary_key_from_instance(other))
                                    ])
            return ~self._criterion_exists(criterion)

        def __ne__(self, other):
            """Implement the ``!=`` operator.

            In a many-to-one context, such as::

              MyClass.some_prop != <some object>

            This will typically produce a clause such as::

              mytable.related_id != <some id>

            Where ``<some id>`` is the primary key of the
            given object.

            The ``!=`` operator provides partial functionality for non-
            many-to-one comparisons:

            * Comparisons against collections are not supported.
              Use
              :meth:`~.RelationshipProperty.Comparator.contains`
              in conjunction with :func:`~.expression.not_`.
            * Compared to a scalar one-to-many, will produce a
              clause that compares the target columns in the parent to
              the given target.
            * Compared to a scalar many-to-many, an alias
              of the association table will be rendered as
              well, forming a natural join that is part of the
              main body of the query. This will not work for
              queries that go beyond simple AND conjunctions of
              comparisons, such as those which use OR. Use
              explicit joins, outerjoins, or
              :meth:`~.RelationshipProperty.Comparator.has` in
              conjunction with :func:`~.expression.not_` for
              more comprehensive non-many-to-one scalar
              membership tests.
            * Comparisons against ``None`` given in a one-to-many
              or many-to-many context produce an EXISTS clause.

            """
            if isinstance(other, (util.NoneType, expression.Null)):
                if self.property.direction == MANYTOONE:
                    return sql.or_(*[x != None for x in
                                   self.property._calculated_foreign_keys])
                else:
                    return self._criterion_exists()
            elif self.property.uselist:
                raise sa_exc.InvalidRequestError("Can't compare a collection"
                        " to an object or collection; use "
                        "contains() to test for membership.")
            else:
                return self.__negated_contains_or_equals(other)

        @util.memoized_property
        def property(self):
            if mapperlib.Mapper._new_mappers:
                mapperlib.Mapper._configure_all()
            return self.prop

    def compare(self, op, value,
                            value_is_parent=False,
                            alias_secondary=True):
        if op == operators.eq:
            if value is None:
                if self.uselist:
                    return ~sql.exists([1], self.primaryjoin)
                else:
                    return self._optimized_compare(None,
                                    value_is_parent=value_is_parent,
                                    alias_secondary=alias_secondary)
            else:
                return self._optimized_compare(value,
                                value_is_parent=value_is_parent,
                                alias_secondary=alias_secondary)
        else:
            return op(self.comparator, value)

    def _optimized_compare(self, value, value_is_parent=False,
                                    adapt_source=None,
                                    alias_secondary=True):
        if value is not None:
            value = attributes.instance_state(value)
        return self._lazy_strategy.lazy_clause(value,
                reverse_direction=not value_is_parent,
                alias_secondary=alias_secondary,
                adapt_source=adapt_source)

    def __str__(self):
        return str(self.parent.class_.__name__) + "." + self.key

    def merge(self,
                    session,
                    source_state,
                    source_dict,
                    dest_state,
                    dest_dict,
                    load, _recursive):

        if load:
            for r in self._reverse_property:
                if (source_state, r) in _recursive:
                    return

        if not "merge" in self._cascade:
            return

        if self.key not in source_dict:
            return

        if self.uselist:
            instances = source_state.get_impl(self.key).\
                            get(source_state, source_dict)
            if hasattr(instances, '_sa_adapter'):
                # convert collections to adapters to get a true iterator
                instances = instances._sa_adapter

            if load:
                # for a full merge, pre-load the destination collection,
                # so that individual _merge of each item pulls from identity
                # map for those already present.
                # also assumes CollectionAttrbiuteImpl behavior of loading
                # "old" list in any case
                dest_state.get_impl(self.key).get(dest_state, dest_dict)

            dest_list = []
            for current in instances:
                current_state = attributes.instance_state(current)
                current_dict = attributes.instance_dict(current)
                _recursive[(current_state, self)] = True
                obj = session._merge(current_state, current_dict,
                        load=load, _recursive=_recursive)
                if obj is not None:
                    dest_list.append(obj)

            if not load:
                coll = attributes.init_state_collection(dest_state,
                        dest_dict, self.key)
                for c in dest_list:
                    coll.append_without_event(c)
            else:
                dest_state.get_impl(self.key)._set_iterable(dest_state,
                        dest_dict, dest_list)
        else:
            current = source_dict[self.key]
            if current is not None:
                current_state = attributes.instance_state(current)
                current_dict = attributes.instance_dict(current)
                _recursive[(current_state, self)] = True
                obj = session._merge(current_state, current_dict,
                        load=load, _recursive=_recursive)
            else:
                obj = None

            if not load:
                dest_dict[self.key] = obj
            else:
                dest_state.get_impl(self.key).set(dest_state,
                        dest_dict, obj, None)

    def _value_as_iterable(self, state, dict_, key,
                                    passive=attributes.PASSIVE_OFF):
        """Return a list of tuples (state, obj) for the given
        key.

        returns an empty list if the value is None/empty/PASSIVE_NO_RESULT
        """

        impl = state.manager[key].impl
        x = impl.get(state, dict_, passive=passive)
        if x is attributes.PASSIVE_NO_RESULT or x is None:
            return []
        elif hasattr(impl, 'get_collection'):
            return [
                (attributes.instance_state(o), o) for o in
                impl.get_collection(state, dict_, x, passive=passive)
            ]
        else:
            return [(attributes.instance_state(x), x)]

    def cascade_iterator(self, type_, state, dict_,
                         visited_states, halt_on=None):
        #assert type_ in self._cascade

        # only actively lazy load on the 'delete' cascade
        if type_ != 'delete' or self.passive_deletes:
            passive = attributes.PASSIVE_NO_INITIALIZE
        else:
            passive = attributes.PASSIVE_OFF

        if type_ == 'save-update':
            tuples = state.manager[self.key].impl.\
                        get_all_pending(state, dict_)

        else:
            tuples = self._value_as_iterable(state, dict_, self.key,
                            passive=passive)

        skip_pending = type_ == 'refresh-expire' and 'delete-orphan' \
            not in self._cascade

        for instance_state, c in tuples:
            if instance_state in visited_states:
                continue

            if c is None:
                # would like to emit a warning here, but
                # would not be consistent with collection.append(None)
                # current behavior of silently skipping.
                # see [ticket:2229]
                continue

            instance_dict = attributes.instance_dict(c)

            if halt_on and halt_on(instance_state):
                continue

            if skip_pending and not instance_state.key:
                continue

            instance_mapper = instance_state.manager.mapper

            if not instance_mapper.isa(self.mapper.class_manager.mapper):
                raise AssertionError("Attribute '%s' on class '%s' "
                                    "doesn't handle objects "
                                    "of type '%s'" % (
                                        self.key,
                                        self.parent.class_,
                                        c.__class__
                                    ))

            visited_states.add(instance_state)

            yield c, instance_mapper, instance_state, instance_dict

    def _add_reverse_property(self, key):
        other = self.mapper.get_property(key, _configure_mappers=False)
        self._reverse_property.add(other)
        other._reverse_property.add(self)

        if not other.mapper.common_parent(self.parent):
            raise sa_exc.ArgumentError('reverse_property %r on '
                    'relationship %s references relationship %s, which '
                    'does not reference mapper %s' % (key, self, other,
                    self.parent))
        if self.direction in (ONETOMANY, MANYTOONE) and self.direction \
                        == other.direction:
            raise sa_exc.ArgumentError('%s and back-reference %s are '
                    'both of the same direction %r.  Did you mean to '
                    'set remote_side on the many-to-one side ?'
                    % (other, self, self.direction))

    @util.memoized_property
    def mapper(self):
        """Return the targeted :class:`.Mapper` for this
        :class:`.RelationshipProperty`.

        This is a lazy-initializing static attribute.

        """
        if util.callable(self.argument) and \
            not isinstance(self.argument, (type, mapperlib.Mapper)):
            argument = self.argument()
        else:
            argument = self.argument

        if isinstance(argument, type):
            mapper_ = mapperlib.class_mapper(argument,
                    configure=False)
        elif isinstance(self.argument, mapperlib.Mapper):
            mapper_ = argument
        else:
            raise sa_exc.ArgumentError("relationship '%s' expects "
                    "a class or a mapper argument (received: %s)"
                    % (self.key, type(argument)))
        return mapper_

    @util.memoized_property
    @util.deprecated("0.7", "Use .target")
    def table(self):
        """Return the selectable linked to this
        :class:`.RelationshipProperty` object's target
        :class:`.Mapper`.
        """
        return self.target

    def do_init(self):
        self._check_conflicts()
        self._process_dependent_arguments()
        self._setup_join_conditions()
        self._check_cascade_settings(self._cascade)
        self._post_init()
        self._generate_backref()
        super(RelationshipProperty, self).do_init()
        self._lazy_strategy = self._get_strategy((("lazy", "select"),))


    def _process_dependent_arguments(self):
        """Convert incoming configuration arguments to their
        proper form.

        Callables are resolved, ORM annotations removed.

        """
        # accept callables for other attributes which may require
        # deferred initialization.  This technique is used
        # by declarative "string configs" and some recipes.
        for attr in (
            'order_by', 'primaryjoin', 'secondaryjoin',
            'secondary', '_user_defined_foreign_keys', 'remote_side',
                ):
            attr_value = getattr(self, attr)
            if util.callable(attr_value):
                setattr(self, attr, attr_value())

        # remove "annotations" which are present if mapped class
        # descriptors are used to create the join expression.
        for attr in 'primaryjoin', 'secondaryjoin':
            val = getattr(self, attr)
            if val is not None:
                setattr(self, attr, _orm_deannotate(
                    expression._only_column_elements(val, attr))
                )

        # ensure expressions in self.order_by, foreign_keys,
        # remote_side are all columns, not strings.
        if self.order_by is not False and self.order_by is not None:
            self.order_by = [
                    expression._only_column_elements(x, "order_by")
                    for x in
                    util.to_list(self.order_by)]

        self._user_defined_foreign_keys = \
            util.column_set(
                    expression._only_column_elements(x, "foreign_keys")
                    for x in util.to_column_set(
                        self._user_defined_foreign_keys
                    ))

        self.remote_side = \
            util.column_set(
                    expression._only_column_elements(x, "remote_side")
                    for x in
                    util.to_column_set(self.remote_side))

        self.target = self.mapper.mapped_table


    def _setup_join_conditions(self):
        self._join_condition = jc = JoinCondition(
                    parent_selectable=self.parent.mapped_table,
                    child_selectable=self.mapper.mapped_table,
                    parent_local_selectable=self.parent.local_table,
                    child_local_selectable=self.mapper.local_table,
                    primaryjoin=self.primaryjoin,
                    secondary=self.secondary,
                    secondaryjoin=self.secondaryjoin,
                    parent_equivalents=self.parent._equivalent_columns,
                    child_equivalents=self.mapper._equivalent_columns,
                    consider_as_foreign_keys=self._user_defined_foreign_keys,
                    local_remote_pairs=self.local_remote_pairs,
                    remote_side=self.remote_side,
                    self_referential=self._is_self_referential,
                    prop=self,
                    support_sync=not self.viewonly,
                    can_be_synced_fn=self._columns_are_mapped
        )
        self.primaryjoin = jc.deannotated_primaryjoin
        self.secondaryjoin = jc.deannotated_secondaryjoin
        self.direction = jc.direction
        self.local_remote_pairs = jc.local_remote_pairs
        self.remote_side = jc.remote_columns
        self.local_columns = jc.local_columns
        self.synchronize_pairs = jc.synchronize_pairs
        self._calculated_foreign_keys = jc.foreign_key_columns
        self.secondary_synchronize_pairs = jc.secondary_synchronize_pairs

    def _check_conflicts(self):
        """Test that this relationship is legal, warn about
        inheritance conflicts."""

        if not self.is_primary() \
            and not mapperlib.class_mapper(
                                self.parent.class_,
                                configure=False).has_property(self.key):
            raise sa_exc.ArgumentError("Attempting to assign a new "
                    "relationship '%s' to a non-primary mapper on "
                    "class '%s'.  New relationships can only be added "
                    "to the primary mapper, i.e. the very first mapper "
                    "created for class '%s' " % (self.key,
                    self.parent.class_.__name__,
                    self.parent.class_.__name__))

        # check for conflicting relationship() on superclass
        if not self.parent.concrete:
            for inheriting in self.parent.iterate_to_root():
                if inheriting is not self.parent \
                        and inheriting.has_property(self.key):
                    util.warn("Warning: relationship '%s' on mapper "
                              "'%s' supersedes the same relationship "
                              "on inherited mapper '%s'; this can "
                              "cause dependency issues during flush"
                              % (self.key, self.parent, inheriting))

    def _get_cascade(self):
        """Return the current cascade setting for this
        :class:`.RelationshipProperty`.
        """
        return self._cascade

    def _set_cascade(self, cascade):
        cascade = CascadeOptions(cascade)
        if 'mapper' in self.__dict__:
            self._check_cascade_settings(cascade)
        self._cascade = cascade

        if self._dependency_processor:
            self._dependency_processor.cascade = cascade

    cascade = property(_get_cascade, _set_cascade)

    def _check_cascade_settings(self, cascade):
        if cascade.delete_orphan and not self.single_parent \
            and (self.direction is MANYTOMANY or self.direction
                 is MANYTOONE):
            raise sa_exc.ArgumentError(
                    'On %s, delete-orphan cascade is not supported '
                    'on a many-to-many or many-to-one relationship '
                    'when single_parent is not set.   Set '
                    'single_parent=True on the relationship().'
                    % self)
        if self.direction is MANYTOONE and self.passive_deletes:
            util.warn("On %s, 'passive_deletes' is normally configured "
                      "on one-to-many, one-to-one, many-to-many "
                      "relationships only."
                       % self)

        if self.passive_deletes == 'all' and \
                    ("delete" in cascade or
                    "delete-orphan" in cascade):
            raise sa_exc.ArgumentError(
                    "On %s, can't set passive_deletes='all' in conjunction "
                    "with 'delete' or 'delete-orphan' cascade" % self)

        if cascade.delete_orphan:
            self.mapper.primary_mapper()._delete_orphans.append(
                            (self.key, self.parent.class_)
                        )

    def _columns_are_mapped(self, *cols):
        """Return True if all columns in the given collection are
        mapped by the tables referenced by this :class:`.Relationship`.

        """
        for c in cols:
            if self.secondary is not None \
                    and self.secondary.c.contains_column(c):
                continue
            if not self.parent.mapped_table.c.contains_column(c) and \
                    not self.target.c.contains_column(c):
                return False
        return True

    def _generate_backref(self):
        """Interpret the 'backref' instruction to create a
        :func:`.relationship` complementary to this one."""

        if not self.is_primary():
            return
        if self.backref is not None and not self.back_populates:
            if isinstance(self.backref, util.string_types):
                backref_key, kwargs = self.backref, {}
            else:
                backref_key, kwargs = self.backref
            mapper = self.mapper.primary_mapper()

            check = set(mapper.iterate_to_root()).\
                        union(mapper.self_and_descendants)
            for m in check:
                if m.has_property(backref_key):
                    raise sa_exc.ArgumentError("Error creating backref "
                            "'%s' on relationship '%s': property of that "
                            "name exists on mapper '%s'" % (backref_key,
                            self, m))

            # determine primaryjoin/secondaryjoin for the
            # backref.  Use the one we had, so that
            # a custom join doesn't have to be specified in
            # both directions.
            if self.secondary is not None:
                # for many to many, just switch primaryjoin/
                # secondaryjoin.   use the annotated
                # pj/sj on the _join_condition.
                pj = kwargs.pop('primaryjoin',
                                self._join_condition.secondaryjoin_minus_local)
                sj = kwargs.pop('secondaryjoin',
                                self._join_condition.primaryjoin_minus_local)
            else:
                pj = kwargs.pop('primaryjoin',
                        self._join_condition.primaryjoin_reverse_remote)
                sj = kwargs.pop('secondaryjoin', None)
                if sj:
                    raise sa_exc.InvalidRequestError(
                        "Can't assign 'secondaryjoin' on a backref "
                        "against a non-secondary relationship."
                    )

            foreign_keys = kwargs.pop('foreign_keys',
                    self._user_defined_foreign_keys)
            parent = self.parent.primary_mapper()
            kwargs.setdefault('viewonly', self.viewonly)
            kwargs.setdefault('post_update', self.post_update)
            kwargs.setdefault('passive_updates', self.passive_updates)
            self.back_populates = backref_key
            relationship = RelationshipProperty(
                parent, self.secondary,
                pj, sj,
                foreign_keys=foreign_keys,
                back_populates=self.key,
                **kwargs)
            mapper._configure_property(backref_key, relationship)

        if self.back_populates:
            self._add_reverse_property(self.back_populates)

    def _post_init(self):
        if self.uselist is None:
            self.uselist = self.direction is not MANYTOONE
        if not self.viewonly:
            self._dependency_processor = \
                dependency.DependencyProcessor.from_relationship(self)

    @util.memoized_property
    def _use_get(self):
        """memoize the 'use_get' attribute of this RelationshipLoader's
        lazyloader."""

        strategy = self._lazy_strategy
        return strategy.use_get

    @util.memoized_property
    def _is_self_referential(self):
        return self.mapper.common_parent(self.parent)

    def _create_joins(self, source_polymorphic=False,
                            source_selectable=None, dest_polymorphic=False,
                            dest_selectable=None, of_type=None):
        if source_selectable is None:
            if source_polymorphic and self.parent.with_polymorphic:
                source_selectable = self.parent._with_polymorphic_selectable

        aliased = False
        if dest_selectable is None:
            if dest_polymorphic and self.mapper.with_polymorphic:
                dest_selectable = self.mapper._with_polymorphic_selectable
                aliased = True
            else:
                dest_selectable = self.mapper.mapped_table

            if self._is_self_referential and source_selectable is None:
                dest_selectable = dest_selectable.alias()
                aliased = True
        else:
            aliased = True

        dest_mapper = of_type or self.mapper

        single_crit = dest_mapper._single_table_criterion
        aliased = aliased or (source_selectable is not None)

        primaryjoin, secondaryjoin, secondary, target_adapter, dest_selectable = \
            self._join_condition.join_targets(
                source_selectable, dest_selectable, aliased, single_crit
            )
        if source_selectable is None:
            source_selectable = self.parent.local_table
        if dest_selectable is None:
            dest_selectable = self.mapper.local_table
        return (primaryjoin, secondaryjoin, source_selectable,
            dest_selectable, secondary, target_adapter)

def _annotate_columns(element, annotations):
    def clone(elem):
        if isinstance(elem, expression.ColumnClause):
            elem = elem._annotate(annotations.copy())
        elem._copy_internals(clone=clone)
        return elem

    if element is not None:
        element = clone(element)
    return element


class JoinCondition(object):
    def __init__(self,
                    parent_selectable,
                    child_selectable,
                    parent_local_selectable,
                    child_local_selectable,
                    primaryjoin=None,
                    secondary=None,
                    secondaryjoin=None,
                    parent_equivalents=None,
                    child_equivalents=None,
                    consider_as_foreign_keys=None,
                    local_remote_pairs=None,
                    remote_side=None,
                    self_referential=False,
                    prop=None,
                    support_sync=True,
                    can_be_synced_fn=lambda *c: True
                    ):
        self.parent_selectable = parent_selectable
        self.parent_local_selectable = parent_local_selectable
        self.child_selectable = child_selectable
        self.child_local_selectable = child_local_selectable
        self.parent_equivalents = parent_equivalents
        self.child_equivalents = child_equivalents
        self.primaryjoin = primaryjoin
        self.secondaryjoin = secondaryjoin
        self.secondary = secondary
        self.consider_as_foreign_keys = consider_as_foreign_keys
        self._local_remote_pairs = local_remote_pairs
        self._remote_side = remote_side
        self.prop = prop
        self.self_referential = self_referential
        self.support_sync = support_sync
        self.can_be_synced_fn = can_be_synced_fn
        self._determine_joins()
        self._annotate_fks()
        self._annotate_remote()
        self._annotate_local()
        self._setup_pairs()
        self._check_foreign_cols(self.primaryjoin, True)
        if self.secondaryjoin is not None:
            self._check_foreign_cols(self.secondaryjoin, False)
        self._determine_direction()
        self._check_remote_side()
        self._log_joins()

    def _log_joins(self):
        if self.prop is None:
            return
        log = self.prop.logger
        log.info('%s setup primary join %s', self.prop,
                         self.primaryjoin)
        log.info('%s setup secondary join %s', self.prop,
                         self.secondaryjoin)
        log.info('%s synchronize pairs [%s]', self.prop,
                         ','.join('(%s => %s)' % (l, r) for (l, r) in
                         self.synchronize_pairs))
        log.info('%s secondary synchronize pairs [%s]', self.prop,
                         ','.join('(%s => %s)' % (l, r) for (l, r) in
                         self.secondary_synchronize_pairs or []))
        log.info('%s local/remote pairs [%s]', self.prop,
                         ','.join('(%s / %s)' % (l, r) for (l, r) in
                         self.local_remote_pairs))
        log.info('%s remote columns [%s]', self.prop,
                        ','.join('%s' % col for col in self.remote_columns)
            )
        log.info('%s local columns [%s]', self.prop,
                        ','.join('%s' % col for col in self.local_columns)
            )
        log.info('%s relationship direction %s', self.prop,
                         self.direction)

    def _determine_joins(self):
        """Determine the 'primaryjoin' and 'secondaryjoin' attributes,
        if not passed to the constructor already.

        This is based on analysis of the foreign key relationships
        between the parent and target mapped selectables.

        """
        if self.secondaryjoin is not None and self.secondary is None:
            raise sa_exc.ArgumentError(
                    "Property %s specified with secondary "
                    "join condition but "
                    "no secondary argument" % self.prop)

        # find a join between the given mapper's mapped table and
        # the given table. will try the mapper's local table first
        # for more specificity, then if not found will try the more
        # general mapped table, which in the case of inheritance is
        # a join.
        try:
            consider_as_foreign_keys = self.consider_as_foreign_keys or None
            if self.secondary is not None:
                if self.secondaryjoin is None:
                    self.secondaryjoin = \
                        join_condition(
                            self.child_selectable,
                            self.secondary,
                            a_subset=self.child_local_selectable,
                            consider_as_foreign_keys=consider_as_foreign_keys
                        )
                if self.primaryjoin is None:
                    self.primaryjoin = \
                        join_condition(
                            self.parent_selectable,
                            self.secondary,
                            a_subset=self.parent_local_selectable,
                            consider_as_foreign_keys=consider_as_foreign_keys
                        )
            else:
                if self.primaryjoin is None:
                    self.primaryjoin = \
                        join_condition(
                            self.parent_selectable,
                            self.child_selectable,
                            a_subset=self.parent_local_selectable,
                            consider_as_foreign_keys=consider_as_foreign_keys
                        )
        except sa_exc.NoForeignKeysError:
            if self.secondary is not None:
                raise sa_exc.NoForeignKeysError("Could not determine join "
                        "condition between parent/child tables on "
                        "relationship %s - there are no foreign keys "
                        "linking these tables via secondary table '%s'.  "
                        "Ensure that referencing columns are associated "
                        "with a ForeignKey or ForeignKeyConstraint, or "
                        "specify 'primaryjoin' and 'secondaryjoin' "
                        "expressions."
                        % (self.prop, self.secondary))
            else:
                raise sa_exc.NoForeignKeysError("Could not determine join "
                        "condition between parent/child tables on "
                        "relationship %s - there are no foreign keys "
                        "linking these tables.  "
                        "Ensure that referencing columns are associated "
                        "with a ForeignKey or ForeignKeyConstraint, or "
                        "specify a 'primaryjoin' expression."
                        % self.prop)
        except sa_exc.AmbiguousForeignKeysError:
            if self.secondary is not None:
                raise sa_exc.AmbiguousForeignKeysError(
                        "Could not determine join "
                        "condition between parent/child tables on "
                        "relationship %s - there are multiple foreign key "
                        "paths linking the tables via secondary table '%s'.  "
                        "Specify the 'foreign_keys' "
                        "argument, providing a list of those columns which "
                        "should be counted as containing a foreign key "
                        "reference from the secondary table to each of the "
                        "parent and child tables."
                        % (self.prop, self.secondary))
            else:
                raise sa_exc.AmbiguousForeignKeysError(
                        "Could not determine join "
                        "condition between parent/child tables on "
                        "relationship %s - there are multiple foreign key "
                        "paths linking the tables.  Specify the "
                        "'foreign_keys' argument, providing a list of those "
                        "columns which should be counted as containing a "
                        "foreign key reference to the parent table."
                        % self.prop)

    @property
    def primaryjoin_minus_local(self):
        return _deep_deannotate(self.primaryjoin, values=("local", "remote"))

    @property
    def secondaryjoin_minus_local(self):
        return _deep_deannotate(self.secondaryjoin, values=("local", "remote"))

    @util.memoized_property
    def primaryjoin_reverse_remote(self):
        """Return the primaryjoin condition suitable for the
        "reverse" direction.

        If the primaryjoin was delivered here with pre-existing
        "remote" annotations, the local/remote annotations
        are reversed.  Otherwise, the local/remote annotations
        are removed.

        """
        if self._has_remote_annotations:
            def replace(element):
                if "remote" in element._annotations:
                    v = element._annotations.copy()
                    del v['remote']
                    v['local'] = True
                    return element._with_annotations(v)
                elif "local" in element._annotations:
                    v = element._annotations.copy()
                    del v['local']
                    v['remote'] = True
                    return element._with_annotations(v)
            return visitors.replacement_traverse(
                                self.primaryjoin, {}, replace)
        else:
            if self._has_foreign_annotations:
                # TODO: coverage
                return _deep_deannotate(self.primaryjoin,
                                values=("local", "remote"))
            else:
                return _deep_deannotate(self.primaryjoin)

    def _has_annotation(self, clause, annotation):
        for col in visitors.iterate(clause, {}):
            if annotation in col._annotations:
                return True
        else:
            return False

    @util.memoized_property
    def _has_foreign_annotations(self):
        return self._has_annotation(self.primaryjoin, "foreign")

    @util.memoized_property
    def _has_remote_annotations(self):
        return self._has_annotation(self.primaryjoin, "remote")

    def _annotate_fks(self):
        """Annotate the primaryjoin and secondaryjoin
        structures with 'foreign' annotations marking columns
        considered as foreign.

        """
        if self._has_foreign_annotations:
            return

        if self.consider_as_foreign_keys:
            self._annotate_from_fk_list()
        else:
            self._annotate_present_fks()

    def _annotate_from_fk_list(self):
        def check_fk(col):
            if col in self.consider_as_foreign_keys:
                return col._annotate({"foreign": True})
        self.primaryjoin = visitors.replacement_traverse(
            self.primaryjoin,
            {},
            check_fk
        )
        if self.secondaryjoin is not None:
            self.secondaryjoin = visitors.replacement_traverse(
                self.secondaryjoin,
                {},
                check_fk
            )

    def _annotate_present_fks(self):
        if self.secondary is not None:
            secondarycols = util.column_set(self.secondary.c)
        else:
            secondarycols = set()

        def is_foreign(a, b):
            if isinstance(a, schema.Column) and \
                        isinstance(b, schema.Column):
                if a.references(b):
                    return a
                elif b.references(a):
                    return b

            if secondarycols:
                if a in secondarycols and b not in secondarycols:
                    return a
                elif b in secondarycols and a not in secondarycols:
                    return b

        def visit_binary(binary):
            if not isinstance(binary.left, sql.ColumnElement) or \
                        not isinstance(binary.right, sql.ColumnElement):
                return

            if "foreign" not in binary.left._annotations and \
                    "foreign" not in binary.right._annotations:
                col = is_foreign(binary.left, binary.right)
                if col is not None:
                    if col.compare(binary.left):
                        binary.left = binary.left._annotate(
                                            {"foreign": True})
                    elif col.compare(binary.right):
                        binary.right = binary.right._annotate(
                                            {"foreign": True})

        self.primaryjoin = visitors.cloned_traverse(
            self.primaryjoin,
            {},
            {"binary": visit_binary}
        )
        if self.secondaryjoin is not None:
            self.secondaryjoin = visitors.cloned_traverse(
                self.secondaryjoin,
                {},
                {"binary": visit_binary}
            )

    def _refers_to_parent_table(self):
        """Return True if the join condition contains column
        comparisons where both columns are in both tables.

        """
        pt = self.parent_selectable
        mt = self.child_selectable
        result = [False]

        def visit_binary(binary):
            c, f = binary.left, binary.right
            if (
                isinstance(c, expression.ColumnClause) and \
                isinstance(f, expression.ColumnClause) and \
                pt.is_derived_from(c.table) and \
                pt.is_derived_from(f.table) and \
                mt.is_derived_from(c.table) and \
                mt.is_derived_from(f.table)
            ):
                result[0] = True
        visitors.traverse(
                    self.primaryjoin,
                    {},
                    {"binary": visit_binary}
                )
        return result[0]

    def _tables_overlap(self):
        """Return True if parent/child tables have some overlap."""

        return selectables_overlap(self.parent_selectable, self.child_selectable)

    def _annotate_remote(self):
        """Annotate the primaryjoin and secondaryjoin
        structures with 'remote' annotations marking columns
        considered as part of the 'remote' side.

        """
        if self._has_remote_annotations:
            return

        if self.secondary is not None:
            self._annotate_remote_secondary()
        elif self._local_remote_pairs or self._remote_side:
            self._annotate_remote_from_args()
        elif self._refers_to_parent_table():
            self._annotate_selfref(lambda col: "foreign" in col._annotations)
        elif self._tables_overlap():
            self._annotate_remote_with_overlap()
        else:
            self._annotate_remote_distinct_selectables()

    def _annotate_remote_secondary(self):
        """annotate 'remote' in primaryjoin, secondaryjoin
        when 'secondary' is present.

        """
        def repl(element):
            if self.secondary.c.contains_column(element):
                return element._annotate({"remote": True})
        self.primaryjoin = visitors.replacement_traverse(
                                    self.primaryjoin, {},  repl)
        self.secondaryjoin = visitors.replacement_traverse(
                                    self.secondaryjoin, {}, repl)

    def _annotate_selfref(self, fn):
        """annotate 'remote' in primaryjoin, secondaryjoin
        when the relationship is detected as self-referential.

        """
        def visit_binary(binary):
            equated = binary.left.compare(binary.right)
            if isinstance(binary.left, expression.ColumnClause) and \
                    isinstance(binary.right, expression.ColumnClause):
                # assume one to many - FKs are "remote"
                if fn(binary.left):
                    binary.left = binary.left._annotate({"remote": True})
                if fn(binary.right) and not equated:
                    binary.right = binary.right._annotate(
                                        {"remote": True})
            else:
                self._warn_non_column_elements()

        self.primaryjoin = visitors.cloned_traverse(
                                self.primaryjoin, {},
                                {"binary": visit_binary})

    def _annotate_remote_from_args(self):
        """annotate 'remote' in primaryjoin, secondaryjoin
        when the 'remote_side' or '_local_remote_pairs'
        arguments are used.

        """
        if self._local_remote_pairs:
            if self._remote_side:
                raise sa_exc.ArgumentError(
                        "remote_side argument is redundant "
                        "against more detailed _local_remote_side "
                        "argument.")

            remote_side = [r for (l, r) in self._local_remote_pairs]
        else:
            remote_side = self._remote_side

        if self._refers_to_parent_table():
            self._annotate_selfref(lambda col: col in remote_side)
        else:
            def repl(element):
                if element in remote_side:
                    return element._annotate({"remote": True})
            self.primaryjoin = visitors.replacement_traverse(
                                        self.primaryjoin, {},  repl)

    def _annotate_remote_with_overlap(self):
        """annotate 'remote' in primaryjoin, secondaryjoin
        when the parent/child tables have some set of
        tables in common, though is not a fully self-referential
        relationship.

        """
        def visit_binary(binary):
            binary.left, binary.right = proc_left_right(binary.left,
                                            binary.right)
            binary.right, binary.left = proc_left_right(binary.right,
                                            binary.left)

        def proc_left_right(left, right):
            if isinstance(left, expression.ColumnClause) and \
                    isinstance(right, expression.ColumnClause):
                if self.child_selectable.c.contains_column(right) and \
                        self.parent_selectable.c.contains_column(left):
                    right = right._annotate({"remote": True})
            else:
                self._warn_non_column_elements()

            return left, right

        self.primaryjoin = visitors.cloned_traverse(
                                    self.primaryjoin, {},
                                    {"binary": visit_binary})

    def _annotate_remote_distinct_selectables(self):
        """annotate 'remote' in primaryjoin, secondaryjoin
        when the parent/child tables are entirely
        separate.

        """
        def repl(element):
            if self.child_selectable.c.contains_column(element) and \
                (
                    not self.parent_local_selectable.c.\
                            contains_column(element)
                    or self.child_local_selectable.c.\
                            contains_column(element)):
                return element._annotate({"remote": True})
        self.primaryjoin = visitors.replacement_traverse(
                                    self.primaryjoin, {},  repl)

    def _warn_non_column_elements(self):
        util.warn(
            "Non-simple column elements in primary "
            "join condition for property %s - consider using "
            "remote() annotations to mark the remote side."
            % self.prop
        )

    def _annotate_local(self):
        """Annotate the primaryjoin and secondaryjoin
        structures with 'local' annotations.

        This annotates all column elements found
        simultaneously in the parent table
        and the join condition that don't have a
        'remote' annotation set up from
        _annotate_remote() or user-defined.

        """
        if self._has_annotation(self.primaryjoin, "local"):
            return

        if self._local_remote_pairs:
            local_side = util.column_set([l for (l, r)
                                in self._local_remote_pairs])
        else:
            local_side = util.column_set(self.parent_selectable.c)

        def locals_(elem):
            if "remote" not in elem._annotations and \
                    elem in local_side:
                return elem._annotate({"local": True})
        self.primaryjoin = visitors.replacement_traverse(
                self.primaryjoin, {}, locals_
            )

    def _check_remote_side(self):
        if not self.local_remote_pairs:
            raise sa_exc.ArgumentError('Relationship %s could '
                    'not determine any unambiguous local/remote column '
                    'pairs based on join condition and remote_side '
                    'arguments.  '
                    'Consider using the remote() annotation to '
                    'accurately mark those elements of the join '
                    'condition that are on the remote side of '
                    'the relationship.'
                    % (self.prop, ))

    def _check_foreign_cols(self, join_condition, primary):
        """Check the foreign key columns collected and emit error
        messages."""

        can_sync = False

        foreign_cols = self._gather_columns_with_annotation(
                                join_condition, "foreign")

        has_foreign = bool(foreign_cols)

        if primary:
            can_sync = bool(self.synchronize_pairs)
        else:
            can_sync = bool(self.secondary_synchronize_pairs)

        if self.support_sync and can_sync or \
                (not self.support_sync and has_foreign):
            return

        # from here below is just determining the best error message
        # to report.  Check for a join condition using any operator
        # (not just ==), perhaps they need to turn on "viewonly=True".
        if self.support_sync and has_foreign and not can_sync:
            err = "Could not locate any simple equality expressions "\
                    "involving locally mapped foreign key columns for "\
                    "%s join condition "\
                    "'%s' on relationship %s." % (
                        primary and 'primary' or 'secondary',
                        join_condition,
                        self.prop
                    )
            err += \
                "  Ensure that referencing columns are associated "\
                "with a ForeignKey or ForeignKeyConstraint, or are "\
                "annotated in the join condition with the foreign() "\
                "annotation. To allow comparison operators other than "\
                "'==', the relationship can be marked as viewonly=True."

            raise sa_exc.ArgumentError(err)
        else:
            err = "Could not locate any relevant foreign key columns "\
                    "for %s join condition '%s' on relationship %s." % (
                        primary and 'primary' or 'secondary',
                        join_condition,
                        self.prop
                    )
            err += \
                '  Ensure that referencing columns are associated '\
                'with a ForeignKey or ForeignKeyConstraint, or are '\
                'annotated in the join condition with the foreign() '\
                'annotation.'
            raise sa_exc.ArgumentError(err)

    def _determine_direction(self):
        """Determine if this relationship is one to many, many to one,
        many to many.

        """
        if self.secondaryjoin is not None:
            self.direction = MANYTOMANY
        else:
            parentcols = util.column_set(self.parent_selectable.c)
            targetcols = util.column_set(self.child_selectable.c)

            # fk collection which suggests ONETOMANY.
            onetomany_fk = targetcols.intersection(
                            self.foreign_key_columns)

            # fk collection which suggests MANYTOONE.

            manytoone_fk = parentcols.intersection(
                            self.foreign_key_columns)

            if onetomany_fk and manytoone_fk:
                # fks on both sides.  test for overlap of local/remote
                # with foreign key
                self_equated = self.remote_columns.intersection(
                                        self.local_columns
                                    )
                onetomany_local = self.remote_columns.\
                                    intersection(self.foreign_key_columns).\
                                    difference(self_equated)
                manytoone_local = self.local_columns.\
                                    intersection(self.foreign_key_columns).\
                                    difference(self_equated)
                if onetomany_local and not manytoone_local:
                    self.direction = ONETOMANY
                elif manytoone_local and not onetomany_local:
                    self.direction = MANYTOONE
                else:
                    raise sa_exc.ArgumentError(
                        "Can't determine relationship"
                        " direction for relationship '%s' - foreign "
                        "key columns within the join condition are present "
                        "in both the parent and the child's mapped tables.  "
                        "Ensure that only those columns referring "
                        "to a parent column are marked as foreign, "
                        "either via the foreign() annotation or "
                        "via the foreign_keys argument." % self.prop)
            elif onetomany_fk:
                self.direction = ONETOMANY
            elif manytoone_fk:
                self.direction = MANYTOONE
            else:
                raise sa_exc.ArgumentError("Can't determine relationship "
                        "direction for relationship '%s' - foreign "
                        "key columns are present in neither the parent "
                        "nor the child's mapped tables" % self.prop)

    def _deannotate_pairs(self, collection):
        """provide deannotation for the various lists of
        pairs, so that using them in hashes doesn't incur
        high-overhead __eq__() comparisons against
        original columns mapped.

        """
        return [(x._deannotate(), y._deannotate())
            for x, y in collection]

    def _setup_pairs(self):
        sync_pairs = []
        lrp = util.OrderedSet([])
        secondary_sync_pairs = []

        def go(joincond, collection):
            def visit_binary(binary, left, right):
                if "remote" in right._annotations and \
                    "remote" not in left._annotations and \
                        self.can_be_synced_fn(left):
                    lrp.add((left, right))
                elif "remote" in left._annotations and \
                    "remote" not in right._annotations and \
                        self.can_be_synced_fn(right):
                    lrp.add((right, left))
                if binary.operator is operators.eq and \
                        self.can_be_synced_fn(left, right):
                    if "foreign" in right._annotations:
                        collection.append((left, right))
                    elif "foreign" in left._annotations:
                        collection.append((right, left))
            visit_binary_product(visit_binary, joincond)

        for joincond, collection in [
            (self.primaryjoin, sync_pairs),
            (self.secondaryjoin, secondary_sync_pairs)
        ]:
            if joincond is None:
                continue
            go(joincond, collection)

        self.local_remote_pairs = self._deannotate_pairs(lrp)
        self.synchronize_pairs = self._deannotate_pairs(sync_pairs)
        self.secondary_synchronize_pairs = \
            self._deannotate_pairs(secondary_sync_pairs)

    @util.memoized_property
    def remote_columns(self):
        return self._gather_join_annotations("remote")

    @util.memoized_property
    def local_columns(self):
        return self._gather_join_annotations("local")

    @util.memoized_property
    def foreign_key_columns(self):
        return self._gather_join_annotations("foreign")

    @util.memoized_property
    def deannotated_primaryjoin(self):
        return _deep_deannotate(self.primaryjoin)

    @util.memoized_property
    def deannotated_secondaryjoin(self):
        if self.secondaryjoin is not None:
            return _deep_deannotate(self.secondaryjoin)
        else:
            return None

    def _gather_join_annotations(self, annotation):
        s = set(
            self._gather_columns_with_annotation(
                        self.primaryjoin, annotation)
        )
        if self.secondaryjoin is not None:
            s.update(
                self._gather_columns_with_annotation(
                        self.secondaryjoin, annotation)
            )
        return set([x._deannotate() for x in s])

    def _gather_columns_with_annotation(self, clause, *annotation):
        annotation = set(annotation)
        return set([
            col for col in visitors.iterate(clause, {})
            if annotation.issubset(col._annotations)
        ])

    def join_targets(self, source_selectable,
                            dest_selectable,
                            aliased,
                            single_crit=None):
        """Given a source and destination selectable, create a
        join between them.

        This takes into account aliasing the join clause
        to reference the appropriate corresponding columns
        in the target objects, as well as the extra child
        criterion, equivalent column sets, etc.

        """

        # place a barrier on the destination such that
        # replacement traversals won't ever dig into it.
        # its internal structure remains fixed
        # regardless of context.
        dest_selectable = _shallow_annotate(
                                dest_selectable,
                                {'no_replacement_traverse': True})

        primaryjoin, secondaryjoin, secondary = self.primaryjoin, \
            self.secondaryjoin, self.secondary

        # adjust the join condition for single table inheritance,
        # in the case that the join is to a subclass
        # this is analogous to the
        # "_adjust_for_single_table_inheritance()" method in Query.

        if single_crit is not None:
            if secondaryjoin is not None:
                secondaryjoin = secondaryjoin & single_crit
            else:
                primaryjoin = primaryjoin & single_crit

        if aliased:
            if secondary is not None:
                secondary = secondary.alias(flat=True)
                primary_aliasizer = ClauseAdapter(secondary)
                secondary_aliasizer = \
                    ClauseAdapter(dest_selectable,
                        equivalents=self.child_equivalents).\
                        chain(primary_aliasizer)
                if source_selectable is not None:
                    primary_aliasizer = \
                        ClauseAdapter(secondary).\
                            chain(ClauseAdapter(source_selectable,
                            equivalents=self.parent_equivalents))
                secondaryjoin = \
                    secondary_aliasizer.traverse(secondaryjoin)
            else:
                primary_aliasizer = ClauseAdapter(dest_selectable,
                        exclude_fn=_ColInAnnotations("local"),
                        equivalents=self.child_equivalents)
                if source_selectable is not None:
                    primary_aliasizer.chain(
                        ClauseAdapter(source_selectable,
                            exclude_fn=_ColInAnnotations("remote"),
                            equivalents=self.parent_equivalents))
                secondary_aliasizer = None

            primaryjoin = primary_aliasizer.traverse(primaryjoin)
            target_adapter = secondary_aliasizer or primary_aliasizer
            target_adapter.exclude_fn = None
        else:
            target_adapter = None
        return primaryjoin, secondaryjoin, secondary, \
                        target_adapter, dest_selectable

    def create_lazy_clause(self, reverse_direction=False):
        binds = util.column_dict()
        lookup = util.column_dict()
        equated_columns = util.column_dict()
        being_replaced = set()

        if reverse_direction and self.secondaryjoin is None:
            for l, r in self.local_remote_pairs:
                _list = lookup.setdefault(r, [])
                _list.append((r, l))
                equated_columns[l] = r
        else:
            # replace all "local side" columns, which is
            # anything that isn't marked "remote"
            being_replaced.update(self.local_columns)
            for l, r in self.local_remote_pairs:
                _list = lookup.setdefault(l, [])
                _list.append((l, r))
                equated_columns[r] = l

        def col_to_bind(col):
            if col in being_replaced or col in lookup:
                if col in lookup:
                    for tobind, equated in lookup[col]:
                        if equated in binds:
                            return None
                else:
                    assert not reverse_direction
                if col not in binds:
                    binds[col] = sql.bindparam(
                        None, None, type_=col.type, unique=True)
                return binds[col]
            return None

        lazywhere = self.deannotated_primaryjoin

        if self.deannotated_secondaryjoin is None or not reverse_direction:
            lazywhere = visitors.replacement_traverse(
                                            lazywhere, {}, col_to_bind)

        if self.deannotated_secondaryjoin is not None:
            secondaryjoin = self.deannotated_secondaryjoin
            if reverse_direction:
                secondaryjoin = visitors.replacement_traverse(
                                            secondaryjoin, {}, col_to_bind)
            lazywhere = sql.and_(lazywhere, secondaryjoin)

        bind_to_col = dict((binds[col].key, col) for col in binds)

        return lazywhere, bind_to_col, equated_columns

class _ColInAnnotations(object):
    """Seralizable equivalent to:

        lambda c: "name" in c._annotations
    """
    def __init__(self, name):
        self.name = name

    def __call__(self, c):
        return self.name in c._annotations
