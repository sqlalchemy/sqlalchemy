# orm/_orm_constructors.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

import typing
from typing import Any
from typing import Callable
from typing import Collection
from typing import Optional
from typing import overload
from typing import Type
from typing import Union

from . import mapper as mapperlib
from .base import Mapped
from .descriptor_props import CompositeProperty
from .descriptor_props import SynonymProperty
from .properties import ColumnProperty
from .query import AliasOption
from .relationships import RelationshipProperty
from .session import Session
from .util import LoaderCriteriaOption
from .. import sql
from .. import util
from ..exc import InvalidRequestError
from ..sql.schema import Column
from ..sql.schema import SchemaEventTarget
from ..sql.type_api import TypeEngine
from ..util.typing import Literal


_RC = typing.TypeVar("_RC")
_T = typing.TypeVar("_T")


@util.deprecated(
    "1.4",
    "The :class:`.AliasOption` object is not necessary "
    "for entities to be matched up to a query that is established "
    "via :meth:`.Query.from_statement` and now does nothing.",
)
def contains_alias(alias) -> "AliasOption":
    r"""Return a :class:`.MapperOption` that will indicate to the
    :class:`_query.Query`
    that the main table has been aliased.

    """
    return AliasOption(alias)


@overload
def mapped_column(
    *args: SchemaEventTarget,
    nullable: bool = ...,
    primary_key: bool = ...,
    **kw: Any,
) -> "Mapped":
    ...


@overload
def mapped_column(
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Union[Literal[None], Literal[True]] = ...,
    primary_key: Union[Literal[None], Literal[False]] = ...,
    **kw: Any,
) -> "Mapped[Optional[_T]]":
    ...


@overload
def mapped_column(
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Union[Literal[None], Literal[True]] = ...,
    primary_key: Union[Literal[None], Literal[False]] = ...,
    **kw: Any,
) -> "Mapped[Optional[_T]]":
    ...


@overload
def mapped_column(
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Union[Literal[None], Literal[False]] = ...,
    primary_key: Literal[True] = True,
    **kw: Any,
) -> "Mapped[_T]":
    ...


@overload
def mapped_column(
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Literal[False] = ...,
    primary_key: bool = ...,
    **kw: Any,
) -> "Mapped[_T]":
    ...


@overload
def mapped_column(
    __name: str,
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Union[Literal[None], Literal[True]] = ...,
    primary_key: Union[Literal[None], Literal[False]] = ...,
    **kw: Any,
) -> "Mapped[Optional[_T]]":
    ...


@overload
def mapped_column(
    __name: str,
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Union[Literal[None], Literal[True]] = ...,
    primary_key: Union[Literal[None], Literal[False]] = ...,
    **kw: Any,
) -> "Mapped[Optional[_T]]":
    ...


@overload
def mapped_column(
    __name: str,
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Union[Literal[None], Literal[False]] = ...,
    primary_key: Literal[True] = True,
    **kw: Any,
) -> "Mapped[_T]":
    ...


@overload
def mapped_column(
    __name: str,
    __type: Union[Type["TypeEngine[_T]"], "TypeEngine[_T]"],
    *args: SchemaEventTarget,
    nullable: Literal[False] = ...,
    primary_key: bool = ...,
    **kw: Any,
) -> "Mapped[_T]":
    ...


def mapped_column(*args, **kw) -> "Mapped":
    """construct a new ORM-mapped :class:`_schema.Column` construct.

    The :func:`_orm.mapped_column` function is shorthand for the construction
    of a Core :class:`_schema.Column` object delivered within a
    :func:`_orm.column_property` construct, which provides for consistent
    typing information to be delivered to the class so that it works under
    static type checkers such as mypy and delivers useful information in
    IDE related type checkers such as pylance.   The function can be used
    in declarative mappings anywhere that :class:`_schema.Column` is normally
    used::

        from sqlalchemy.orm import mapped_column

        class User(Base):
            __tablename__ = 'user'

            id = mapped_column(Integer)
            name = mapped_column(String)


    .. versionadded:: 2.0

    """
    return column_property(Column(*args, **kw))


def column_property(
    column: sql.ColumnElement[_T], *additional_columns, **kwargs
) -> "Mapped[_T]":
    r"""Provide a column-level property for use with a mapping.

    Column-based properties can normally be applied to the mapper's
    ``properties`` dictionary using the :class:`_schema.Column`
    element directly.
    Use this function when the given column is not directly present within
    the mapper's selectable; examples include SQL expressions, functions,
    and scalar SELECT queries.

    The :func:`_orm.column_property` function returns an instance of
    :class:`.ColumnProperty`.

    Columns that aren't present in the mapper's selectable won't be
    persisted by the mapper and are effectively "read-only" attributes.

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

    :param comparator_factory: a class which extends
       :class:`.ColumnProperty.Comparator` which provides custom SQL
       clause generation for comparison operations.

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

    :param info: Optional data dictionary which will be populated into the
        :attr:`.MapperProperty.info` attribute of this object.

    :param raiseload: if True, indicates the column should raise an error
        when undeferred, rather than loading the value.  This can be
        altered at query time by using the :func:`.deferred` option with
        raiseload=False.

        .. versionadded:: 1.4

        .. seealso::

            :ref:`deferred_raiseload`

    .. seealso::

        :ref:`column_property_options` - to map columns while including
        mapping options

        :ref:`mapper_column_property_sql_expressions` - to map SQL
        expressions

    """
    return ColumnProperty(column, *additional_columns, **kwargs)


def composite(class_: Type[_T], *attrs, **kwargs) -> "Mapped[_T]":
    r"""Return a composite column-based property for use with a Mapper.

    See the mapping documentation section :ref:`mapper_composite` for a
    full usage example.

    The :class:`.MapperProperty` returned by :func:`.composite`
    is the :class:`.CompositeProperty`.

    :param class\_:
      The "composite type" class, or any classmethod or callable which
      will produce a new instance of the composite object given the
      column values in order.

    :param \*cols:
      List of Column objects to be mapped.

    :param active_history=False:
      When ``True``, indicates that the "previous" value for a
      scalar attribute should be loaded when replaced, if not
      already loaded.  See the same flag on :func:`.column_property`.

    :param group:
      A group name for this property when marked as deferred.

    :param deferred:
      When True, the column property is "deferred", meaning that it does
      not load immediately, and is instead loaded when the attribute is
      first accessed on an instance.  See also
      :func:`~sqlalchemy.orm.deferred`.

    :param comparator_factory:  a class which extends
      :class:`.CompositeProperty.Comparator` which provides custom SQL
      clause generation for comparison operations.

    :param doc:
      optional string that will be applied as the doc on the
      class-bound descriptor.

    :param info: Optional data dictionary which will be populated into the
        :attr:`.MapperProperty.info` attribute of this object.

    """
    return CompositeProperty(class_, *attrs, **kwargs)


def with_loader_criteria(
    entity_or_base,
    where_criteria,
    loader_only=False,
    include_aliases=False,
    propagate_to_loaders=True,
    track_closure_variables=True,
) -> "LoaderCriteriaOption":
    """Add additional WHERE criteria to the load for all occurrences of
    a particular entity.

    .. versionadded:: 1.4

    The :func:`_orm.with_loader_criteria` option is intended to add
    limiting criteria to a particular kind of entity in a query,
    **globally**, meaning it will apply to the entity as it appears
    in the SELECT query as well as within any subqueries, join
    conditions, and relationship loads, including both eager and lazy
    loaders, without the need for it to be specified in any particular
    part of the query.    The rendering logic uses the same system used by
    single table inheritance to ensure a certain discriminator is applied
    to a table.

    E.g., using :term:`2.0-style` queries, we can limit the way the
    ``User.addresses`` collection is loaded, regardless of the kind
    of loading used::

        from sqlalchemy.orm import with_loader_criteria

        stmt = select(User).options(
            selectinload(User.addresses),
            with_loader_criteria(Address, Address.email_address != 'foo'))
        )

    Above, the "selectinload" for ``User.addresses`` will apply the
    given filtering criteria to the WHERE clause.

    Another example, where the filtering will be applied to the
    ON clause of the join, in this example using :term:`1.x style`
    queries::

        q = session.query(User).outerjoin(User.addresses).options(
            with_loader_criteria(Address, Address.email_address != 'foo'))
        )

    The primary purpose of :func:`_orm.with_loader_criteria` is to use
    it in the :meth:`_orm.SessionEvents.do_orm_execute` event handler
    to ensure that all occurrences of a particular entity are filtered
    in a certain way, such as filtering for access control roles.    It
    also can be used to apply criteria to relationship loads.  In the
    example below, we can apply a certain set of rules to all queries
    emitted by a particular :class:`_orm.Session`::

        session = Session(bind=engine)

        @event.listens_for("do_orm_execute", session)
        def _add_filtering_criteria(execute_state):

            if (
                execute_state.is_select
                and not execute_state.is_column_load
                and not execute_state.is_relationship_load
            ):
                execute_state.statement = execute_state.statement.options(
                    with_loader_criteria(
                        SecurityRole,
                        lambda cls: cls.role.in_(['some_role']),
                        include_aliases=True
                    )
                )

    In the above example, the :meth:`_orm.SessionEvents.do_orm_execute`
    event will intercept all queries emitted using the
    :class:`_orm.Session`. For those queries which are SELECT statements
    and are not attribute or relationship loads a custom
    :func:`_orm.with_loader_criteria` option is added to the query.    The
    :func:`_orm.with_loader_criteria` option will be used in the given
    statement and will also be automatically propagated to all relationship
    loads that descend from this query.

    The criteria argument given is a ``lambda`` that accepts a ``cls``
    argument.  The given class will expand to include all mapped subclass
    and need not itself be a mapped class.

    .. tip::

       When using :func:`_orm.with_loader_criteria` option in
       conjunction with the :func:`_orm.contains_eager` loader option,
       it's important to note that :func:`_orm.with_loader_criteria` only
       affects the part of the query that determines what SQL is rendered
       in terms of the WHERE and FROM clauses. The
       :func:`_orm.contains_eager` option does not affect the rendering of
       the SELECT statement outside of the columns clause, so does not have
       any interaction with the :func:`_orm.with_loader_criteria` option.
       However, the way things "work" is that :func:`_orm.contains_eager`
       is meant to be used with a query that is already selecting from the
       additional entities in some way, where
       :func:`_orm.with_loader_criteria` can apply it's additional
       criteria.

       In the example below, assuming a mapping relationship as
       ``A -> A.bs -> B``, the given :func:`_orm.with_loader_criteria`
       option will affect the way in which the JOIN is rendered::

            stmt = select(A).join(A.bs).options(
                contains_eager(A.bs),
                with_loader_criteria(B, B.flag == 1)
            )

       Above, the given :func:`_orm.with_loader_criteria` option will
       affect the ON clause of the JOIN that is specified by
       ``.join(A.bs)``, so is applied as expected. The
       :func:`_orm.contains_eager` option has the effect that columns from
       ``B`` are added to the columns clause::

            SELECT
                b.id, b.a_id, b.data, b.flag,
                a.id AS id_1,
                a.data AS data_1
            FROM a JOIN b ON a.id = b.a_id AND b.flag = :flag_1


       The use of the :func:`_orm.contains_eager` option within the above
       statement has no effect on the behavior of the
       :func:`_orm.with_loader_criteria` option. If the
       :func:`_orm.contains_eager` option were omitted, the SQL would be
       the same as regards the FROM and WHERE clauses, where
       :func:`_orm.with_loader_criteria` continues to add its criteria to
       the ON clause of the JOIN. The addition of
       :func:`_orm.contains_eager` only affects the columns clause, in that
       additional columns against ``b`` are added which are then consumed
       by the ORM to produce ``B`` instances.

    .. warning:: The use of a lambda inside of the call to
      :func:`_orm.with_loader_criteria` is only invoked **once per unique
      class**. Custom functions should not be invoked within this lambda.
      See :ref:`engine_lambda_caching` for an overview of the "lambda SQL"
      feature, which is for advanced use only.

    :param entity_or_base: a mapped class, or a class that is a super
     class of a particular set of mapped classes, to which the rule
     will apply.

    :param where_criteria: a Core SQL expression that applies limiting
     criteria.   This may also be a "lambda:" or Python function that
     accepts a target class as an argument, when the given class is
     a base with many different mapped subclasses.

    :param include_aliases: if True, apply the rule to :func:`_orm.aliased`
     constructs as well.

    :param propagate_to_loaders: defaults to True, apply to relationship
     loaders such as lazy loaders.


     .. seealso::

        :ref:`examples_session_orm_events` - includes examples of using
        :func:`_orm.with_loader_criteria`.

        :ref:`do_orm_execute_global_criteria` - basic example on how to
        combine :func:`_orm.with_loader_criteria` with the
        :meth:`_orm.SessionEvents.do_orm_execute` event.

    :param track_closure_variables: when False, closure variables inside
     of a lambda expression will not be used as part of
     any cache key.    This allows more complex expressions to be used
     inside of a lambda expression but requires that the lambda ensures
     it returns the identical SQL every time given a particular class.

     .. versionadded:: 1.4.0b2

    """
    return LoaderCriteriaOption(
        entity_or_base,
        where_criteria,
        loader_only,
        include_aliases,
        propagate_to_loaders,
        track_closure_variables,
    )


@overload
def relationship(
    argument: Union[str, Type[_RC], Callable[[], Type[_RC]]],
    secondary=None,
    *,
    uselist: Literal[True] = None,
    primaryjoin=None,
    secondaryjoin=None,
    foreign_keys=None,
    order_by=False,
    backref=None,
    back_populates=None,
    overlaps=None,
    post_update=False,
    cascade=False,
    viewonly=False,
    lazy="select",
    collection_class=None,
    passive_deletes=RelationshipProperty._persistence_only["passive_deletes"],
    passive_updates=RelationshipProperty._persistence_only["passive_updates"],
    remote_side=None,
    enable_typechecks=RelationshipProperty._persistence_only[
        "enable_typechecks"
    ],
    join_depth=None,
    comparator_factory=None,
    single_parent=False,
    innerjoin=False,
    distinct_target_key=None,
    doc=None,
    active_history=RelationshipProperty._persistence_only["active_history"],
    cascade_backrefs=RelationshipProperty._persistence_only[
        "cascade_backrefs"
    ],
    load_on_pending=False,
    bake_queries=True,
    _local_remote_pairs=None,
    query_class=None,
    info=None,
    omit_join=None,
    sync_backref=None,
    _legacy_inactive_history_style=False,
) -> Mapped[Collection[_RC]]:
    ...


@overload
def relationship(
    argument: Union[str, Type[_RC], Callable[[], Type[_RC]]],
    secondary=None,
    *,
    uselist: Optional[bool] = None,
    primaryjoin=None,
    secondaryjoin=None,
    foreign_keys=None,
    order_by=False,
    backref=None,
    back_populates=None,
    overlaps=None,
    post_update=False,
    cascade=False,
    viewonly=False,
    lazy="select",
    collection_class=None,
    passive_deletes=RelationshipProperty._persistence_only["passive_deletes"],
    passive_updates=RelationshipProperty._persistence_only["passive_updates"],
    remote_side=None,
    enable_typechecks=RelationshipProperty._persistence_only[
        "enable_typechecks"
    ],
    join_depth=None,
    comparator_factory=None,
    single_parent=False,
    innerjoin=False,
    distinct_target_key=None,
    doc=None,
    active_history=RelationshipProperty._persistence_only["active_history"],
    cascade_backrefs=RelationshipProperty._persistence_only[
        "cascade_backrefs"
    ],
    load_on_pending=False,
    bake_queries=True,
    _local_remote_pairs=None,
    query_class=None,
    info=None,
    omit_join=None,
    sync_backref=None,
    _legacy_inactive_history_style=False,
) -> Mapped[_RC]:
    ...


def relationship(
    argument: Union[str, Type[_RC], Callable[[], Type[_RC]]],
    secondary=None,
    *,
    primaryjoin=None,
    secondaryjoin=None,
    foreign_keys=None,
    uselist: Optional[bool] = None,
    order_by=False,
    backref=None,
    back_populates=None,
    overlaps=None,
    post_update=False,
    cascade=False,
    viewonly=False,
    lazy="select",
    collection_class=None,
    passive_deletes=RelationshipProperty._persistence_only["passive_deletes"],
    passive_updates=RelationshipProperty._persistence_only["passive_updates"],
    remote_side=None,
    enable_typechecks=RelationshipProperty._persistence_only[
        "enable_typechecks"
    ],
    join_depth=None,
    comparator_factory=None,
    single_parent=False,
    innerjoin=False,
    distinct_target_key=None,
    doc=None,
    active_history=RelationshipProperty._persistence_only["active_history"],
    cascade_backrefs=RelationshipProperty._persistence_only[
        "cascade_backrefs"
    ],
    load_on_pending=False,
    bake_queries=True,
    _local_remote_pairs=None,
    query_class=None,
    info=None,
    omit_join=None,
    sync_backref=None,
    _legacy_inactive_history_style=False,
) -> Mapped[_RC]:
    """Provide a relationship between two mapped classes.

    This corresponds to a parent-child or associative table relationship.
    The constructed class is an instance of
    :class:`.RelationshipProperty`.

    A typical :func:`_orm.relationship`, used in a classical mapping::

       mapper(Parent, properties={
         'children': relationship(Child)
       })

    Some arguments accepted by :func:`_orm.relationship`
    optionally accept a
    callable function, which when called produces the desired value.
    The callable is invoked by the parent :class:`_orm.Mapper` at "mapper
    initialization" time, which happens only when mappers are first used,
    and is assumed to be after all mappings have been constructed.  This
    can be used to resolve order-of-declaration and other dependency
    issues, such as if ``Child`` is declared below ``Parent`` in the same
    file::

        mapper(Parent, properties={
            "children":relationship(lambda: Child,
                                order_by=lambda: Child.id)
        })

    When using the :ref:`declarative_toplevel` extension, the Declarative
    initializer allows string arguments to be passed to
    :func:`_orm.relationship`.  These string arguments are converted into
    callables that evaluate the string as Python code, using the
    Declarative class-registry as a namespace.  This allows the lookup of
    related classes to be automatic via their string name, and removes the
    need for related classes to be imported into the local module space
    before the dependent classes have been declared.  It is still required
    that the modules in which these related classes appear are imported
    anywhere in the application at some point before the related mappings
    are actually used, else a lookup error will be raised when the
    :func:`_orm.relationship`
    attempts to resolve the string reference to the
    related class.    An example of a string- resolved class is as
    follows::

        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        class Parent(Base):
            __tablename__ = 'parent'
            id = Column(Integer, primary_key=True)
            children = relationship("Child", order_by="Child.id")

    .. seealso::

      :ref:`relationship_config_toplevel` - Full introductory and
      reference documentation for :func:`_orm.relationship`.

      :ref:`orm_tutorial_relationship` - ORM tutorial introduction.

    :param argument:
      A mapped class, or actual :class:`_orm.Mapper` instance,
      representing
      the target of the relationship.

      :paramref:`_orm.relationship.argument`
      may also be passed as a callable
      function which is evaluated at mapper initialization time, and may
      be passed as a string name when using Declarative.

      .. warning:: Prior to SQLAlchemy 1.3.16, this value is interpreted
         using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

      .. versionchanged 1.3.16::

         The string evaluation of the main "argument" no longer accepts an
         open ended Python expression, instead only accepting a string
         class name or dotted package-qualified name.

      .. seealso::

        :ref:`declarative_configuring_relationships` - further detail
        on relationship configuration when using Declarative.

    :param secondary:
      For a many-to-many relationship, specifies the intermediary
      table, and is typically an instance of :class:`_schema.Table`.
      In less common circumstances, the argument may also be specified
      as an :class:`_expression.Alias` construct, or even a
      :class:`_expression.Join` construct.

      :paramref:`_orm.relationship.secondary` may
      also be passed as a callable function which is evaluated at
      mapper initialization time.  When using Declarative, it may also
      be a string argument noting the name of a :class:`_schema.Table`
      that is
      present in the :class:`_schema.MetaData`
      collection associated with the
      parent-mapped :class:`_schema.Table`.

      .. warning:: When passed as a Python-evaluable string, the
         argument is interpreted using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

      The :paramref:`_orm.relationship.secondary` keyword argument is
      typically applied in the case where the intermediary
      :class:`_schema.Table`
      is not otherwise expressed in any direct class mapping. If the
      "secondary" table is also explicitly mapped elsewhere (e.g. as in
      :ref:`association_pattern`), one should consider applying the
      :paramref:`_orm.relationship.viewonly` flag so that this
      :func:`_orm.relationship`
      is not used for persistence operations which
      may conflict with those of the association object pattern.

      .. seealso::

          :ref:`relationships_many_to_many` - Reference example of "many
          to many".

          :ref:`orm_tutorial_many_to_many` - ORM tutorial introduction to
          many-to-many relationships.

          :ref:`self_referential_many_to_many` - Specifics on using
          many-to-many in a self-referential case.

          :ref:`declarative_many_to_many` - Additional options when using
          Declarative.

          :ref:`association_pattern` - an alternative to
          :paramref:`_orm.relationship.secondary`
          when composing association
          table relationships, allowing additional attributes to be
          specified on the association table.

          :ref:`composite_secondary_join` - a lesser-used pattern which
          in some cases can enable complex :func:`_orm.relationship` SQL
          conditions to be used.

      .. versionadded:: 0.9.2 :paramref:`_orm.relationship.secondary`
         works
         more effectively when referring to a :class:`_expression.Join`
         instance.

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
      Indicates the string name of a property to be placed on the related
      mapper's class that will handle this relationship in the other
      direction. The other property will be created automatically
      when the mappers are configured.  Can also be passed as a
      :func:`.backref` object to control the configuration of the
      new relationship.

      .. seealso::

        :ref:`relationships_backref` - Introductory documentation and
        examples.

        :paramref:`_orm.relationship.back_populates` - alternative form
        of backref specification.

        :func:`.backref` - allows control over :func:`_orm.relationship`
        configuration when using :paramref:`_orm.relationship.backref`.


    :param back_populates:
      Takes a string name and has the same meaning as
      :paramref:`_orm.relationship.backref`, except the complementing
      property is **not** created automatically, and instead must be
      configured explicitly on the other mapper.  The complementing
      property should also indicate
      :paramref:`_orm.relationship.back_populates` to this relationship to
      ensure proper functioning.

      .. seealso::

        :ref:`relationships_backref` - Introductory documentation and
        examples.

        :paramref:`_orm.relationship.backref` - alternative form
        of backref specification.

    :param overlaps:
       A string name or comma-delimited set of names of other relationships
       on either this mapper, a descendant mapper, or a target mapper with
       which this relationship may write to the same foreign keys upon
       persistence.   The only effect this has is to eliminate the
       warning that this relationship will conflict with another upon
       persistence.   This is used for such relationships that are truly
       capable of conflicting with each other on write, but the application
       will ensure that no such conflicts occur.

       .. versionadded:: 1.4

       .. seealso::

            :ref:`error_qzyx` - usage example

    :param bake_queries=True:
      Enable :ref:`lambda caching <engine_lambda_caching>` for loader
      strategies, if applicable, which adds a performance gain to the
      construction of SQL constructs used by loader strategies, in addition
      to the usual SQL statement caching used throughout SQLAlchemy. This
      parameter currently applies only to the "lazy" and "selectin" loader
      strategies. There is generally no reason to set this parameter to
      False.

      .. versionchanged:: 1.4  Relationship loaders no longer use the
         previous "baked query" system of query caching.   The "lazy"
         and "selectin" loaders make use of the "lambda cache" system
         for the construction of SQL constructs,
         as well as the usual SQL caching system that is throughout
         SQLAlchemy as of the 1.4 series.

    :param cascade:
      A comma-separated list of cascade rules which determines how
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

    :param cascade_backrefs=False:
      Legacy; this flag is always False.

      .. versionchanged:: 2.0 "cascade_backrefs" functionality has been
         removed.

    :param collection_class:
      A class or callable that returns a new list-holding object. will
      be used in place of a plain list for storing elements.

      .. seealso::

        :ref:`custom_collections` - Introductory documentation and
        examples.

    :param comparator_factory:
      A class which extends :class:`.RelationshipProperty.Comparator`
      which provides custom SQL clause generation for comparison
      operations.

      .. seealso::

        :class:`.PropComparator` - some detail on redefining comparators
        at this level.

        :ref:`custom_comparators` - Brief intro to this feature.


    :param distinct_target_key=None:
      Indicate if a "subquery" eager load should apply the DISTINCT
      keyword to the innermost SELECT statement.  When left as ``None``,
      the DISTINCT keyword will be applied in those cases when the target
      columns do not comprise the full primary key of the target table.
      When set to ``True``, the DISTINCT keyword is applied to the
      innermost SELECT unconditionally.

      It may be desirable to set this flag to False when the DISTINCT is
      reducing performance of the innermost subquery beyond that of what
      duplicate innermost rows may be causing.

      .. versionchanged:: 0.9.0 -
         :paramref:`_orm.relationship.distinct_target_key` now defaults to
         ``None``, so that the feature enables itself automatically for
         those cases where the innermost query targets a non-unique
         key.

      .. seealso::

        :ref:`loading_toplevel` - includes an introduction to subquery
        eager loading.

    :param doc:
      Docstring which will be applied to the resulting descriptor.

    :param foreign_keys:

      A list of columns which are to be used as "foreign key"
      columns, or columns which refer to the value in a remote
      column, within the context of this :func:`_orm.relationship`
      object's :paramref:`_orm.relationship.primaryjoin` condition.
      That is, if the :paramref:`_orm.relationship.primaryjoin`
      condition of this :func:`_orm.relationship` is ``a.id ==
      b.a_id``, and the values in ``b.a_id`` are required to be
      present in ``a.id``, then the "foreign key" column of this
      :func:`_orm.relationship` is ``b.a_id``.

      In normal cases, the :paramref:`_orm.relationship.foreign_keys`
      parameter is **not required.** :func:`_orm.relationship` will
      automatically determine which columns in the
      :paramref:`_orm.relationship.primaryjoin` condition are to be
      considered "foreign key" columns based on those
      :class:`_schema.Column` objects that specify
      :class:`_schema.ForeignKey`,
      or are otherwise listed as referencing columns in a
      :class:`_schema.ForeignKeyConstraint` construct.
      :paramref:`_orm.relationship.foreign_keys` is only needed when:

        1. There is more than one way to construct a join from the local
           table to the remote table, as there are multiple foreign key
           references present.  Setting ``foreign_keys`` will limit the
           :func:`_orm.relationship`
           to consider just those columns specified
           here as "foreign".

        2. The :class:`_schema.Table` being mapped does not actually have
           :class:`_schema.ForeignKey` or
           :class:`_schema.ForeignKeyConstraint`
           constructs present, often because the table
           was reflected from a database that does not support foreign key
           reflection (MySQL MyISAM).

        3. The :paramref:`_orm.relationship.primaryjoin`
           argument is used to
           construct a non-standard join condition, which makes use of
           columns or expressions that do not normally refer to their
           "parent" column, such as a join condition expressed by a
           complex comparison using a SQL function.

      The :func:`_orm.relationship` construct will raise informative
      error messages that suggest the use of the
      :paramref:`_orm.relationship.foreign_keys` parameter when
      presented with an ambiguous condition.   In typical cases,
      if :func:`_orm.relationship` doesn't raise any exceptions, the
      :paramref:`_orm.relationship.foreign_keys` parameter is usually
      not needed.

      :paramref:`_orm.relationship.foreign_keys` may also be passed as a
      callable function which is evaluated at mapper initialization time,
      and may be passed as a Python-evaluable string when using
      Declarative.

      .. warning:: When passed as a Python-evaluable string, the
         argument is interpreted using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

      .. seealso::

        :ref:`relationship_foreign_keys`

        :ref:`relationship_custom_foreign`

        :func:`.foreign` - allows direct annotation of the "foreign"
        columns within a :paramref:`_orm.relationship.primaryjoin`
        condition.

    :param info: Optional data dictionary which will be populated into the
        :attr:`.MapperProperty.info` attribute of this object.

    :param innerjoin=False:
      When ``True``, joined eager loads will use an inner join to join
      against related tables instead of an outer join.  The purpose
      of this option is generally one of performance, as inner joins
      generally perform better than outer joins.

      This flag can be set to ``True`` when the relationship references an
      object via many-to-one using local foreign keys that are not
      nullable, or when the reference is one-to-one or a collection that
      is guaranteed to have one or at least one entry.

      The option supports the same "nested" and "unnested" options as
      that of :paramref:`_orm.joinedload.innerjoin`.  See that flag
      for details on nested / unnested behaviors.

      .. seealso::

        :paramref:`_orm.joinedload.innerjoin` - the option as specified by
        loader option, including detail on nesting behavior.

        :ref:`what_kind_of_loading` - Discussion of some details of
        various loader options.


    :param join_depth:
      When non-``None``, an integer value indicating how many levels
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
      How the related items should be loaded.  Default value is
      ``select``.  Values include:

      * ``select`` - items should be loaded lazily when the property is
        first accessed, using a separate SELECT statement, or identity map
        fetch for simple many-to-one references.

      * ``immediate`` - items should be loaded as the parents are loaded,
        using a separate SELECT statement, or identity map fetch for
        simple many-to-one references.

      * ``joined`` - items should be loaded "eagerly" in the same query as
        that of the parent, using a JOIN or LEFT OUTER JOIN.  Whether
        the join is "outer" or not is determined by the
        :paramref:`_orm.relationship.innerjoin` parameter.

      * ``subquery`` - items should be loaded "eagerly" as the parents are
        loaded, using one additional SQL statement, which issues a JOIN to
        a subquery of the original statement, for each collection
        requested.

      * ``selectin`` - items should be loaded "eagerly" as the parents
        are loaded, using one or more additional SQL statements, which
        issues a JOIN to the immediate parent object, specifying primary
        key identifiers using an IN clause.

        .. versionadded:: 1.2

      * ``noload`` - no loading should occur at any time.  This is to
        support "write-only" attributes, or attributes which are
        populated in some manner specific to the application.

      * ``raise`` - lazy loading is disallowed; accessing
        the attribute, if its value were not already loaded via eager
        loading, will raise an :exc:`~sqlalchemy.exc.InvalidRequestError`.
        This strategy can be used when objects are to be detached from
        their attached :class:`.Session` after they are loaded.

        .. versionadded:: 1.1

      * ``raise_on_sql`` - lazy loading that emits SQL is disallowed;
        accessing the attribute, if its value were not already loaded via
        eager loading, will raise an
        :exc:`~sqlalchemy.exc.InvalidRequestError`, **if the lazy load
        needs to emit SQL**.  If the lazy load can pull the related value
        from the identity map or determine that it should be None, the
        value is loaded.  This strategy can be used when objects will
        remain associated with the attached :class:`.Session`, however
        additional SELECT statements should be blocked.

        .. versionadded:: 1.1

      * ``dynamic`` - the attribute will return a pre-configured
        :class:`_query.Query` object for all read
        operations, onto which further filtering operations can be
        applied before iterating the results.  See
        the section :ref:`dynamic_relationship` for more details.

      * True - a synonym for 'select'

      * False - a synonym for 'joined'

      * None - a synonym for 'noload'

      .. seealso::

        :doc:`/orm/loading_relationships` - Full documentation on
        relationship loader configuration.

        :ref:`dynamic_relationship` - detail on the ``dynamic`` option.

        :ref:`collections_noload_raiseload` - notes on "noload" and "raise"

    :param load_on_pending=False:
      Indicates loading behavior for transient or pending parent objects.

      When set to ``True``, causes the lazy-loader to
      issue a query for a parent object that is not persistent, meaning it
      has never been flushed.  This may take effect for a pending object
      when autoflush is disabled, or for a transient object that has been
      "attached" to a :class:`.Session` but is not part of its pending
      collection.

      The :paramref:`_orm.relationship.load_on_pending`
      flag does not improve
      behavior when the ORM is used normally - object references should be
      constructed at the object level, not at the foreign key level, so
      that they are present in an ordinary way before a flush proceeds.
      This flag is not not intended for general use.

      .. seealso::

          :meth:`.Session.enable_relationship_loading` - this method
          establishes "load on pending" behavior for the whole object, and
          also allows loading on objects that remain transient or
          detached.

    :param order_by:
      Indicates the ordering that should be applied when loading these
      items.  :paramref:`_orm.relationship.order_by`
      is expected to refer to
      one of the :class:`_schema.Column`
      objects to which the target class is
      mapped, or the attribute itself bound to the target class which
      refers to the column.

      :paramref:`_orm.relationship.order_by`
      may also be passed as a callable
      function which is evaluated at mapper initialization time, and may
      be passed as a Python-evaluable string when using Declarative.

      .. warning:: When passed as a Python-evaluable string, the
         argument is interpreted using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

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
       disable the "nulling out" of the child foreign keys, when the parent
       object is deleted and there is no delete or delete-orphan cascade
       enabled.  This is typically used when a triggering or error raise
       scenario is in place on the database side.  Note that the foreign
       key attributes on in-session child objects will not be changed after
       a flush occurs so this is a very special use-case setting.
       Additionally, the "nulling out" will still occur if the child
       object is de-associated with the parent.

       .. seealso::

            :ref:`passive_deletes` - Introductory documentation
            and examples.

    :param passive_updates=True:
      Indicates the persistence behavior to take when a referenced
      primary key value changes in place, indicating that the referencing
      foreign key columns will also need their value changed.

      When True, it is assumed that ``ON UPDATE CASCADE`` is configured on
      the foreign key in the database, and that the database will
      handle propagation of an UPDATE from a source column to
      dependent rows.  When False, the SQLAlchemy
      :func:`_orm.relationship`
      construct will attempt to emit its own UPDATE statements to
      modify related targets.  However note that SQLAlchemy **cannot**
      emit an UPDATE for more than one level of cascade.  Also,
      setting this flag to False is not compatible in the case where
      the database is in fact enforcing referential integrity, unless
      those constraints are explicitly "deferred", if the target backend
      supports it.

      It is highly advised that an application which is employing
      mutable primary keys keeps ``passive_updates`` set to True,
      and instead uses the referential integrity features of the database
      itself in order to handle the change efficiently and fully.

      .. seealso::

          :ref:`passive_updates` - Introductory documentation and
          examples.

          :paramref:`.mapper.passive_updates` - a similar flag which
          takes effect for joined-table inheritance mappings.

    :param post_update:
      This indicates that the relationship should be handled by a
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
      use :paramref:`_orm.relationship.post_update` to "break" the cycle.

      .. seealso::

          :ref:`post_update` - Introductory documentation and examples.

    :param primaryjoin:
      A SQL expression that will be used as the primary
      join of the child object against the parent object, or in a
      many-to-many relationship the join of the parent object to the
      association table. By default, this value is computed based on the
      foreign key relationships of the parent and child tables (or
      association table).

      :paramref:`_orm.relationship.primaryjoin` may also be passed as a
      callable function which is evaluated at mapper initialization time,
      and may be passed as a Python-evaluable string when using
      Declarative.

      .. warning:: When passed as a Python-evaluable string, the
         argument is interpreted using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

      .. seealso::

          :ref:`relationship_primaryjoin`

    :param remote_side:
      Used for self-referential relationships, indicates the column or
      list of columns that form the "remote side" of the relationship.

      :paramref:`_orm.relationship.remote_side` may also be passed as a
      callable function which is evaluated at mapper initialization time,
      and may be passed as a Python-evaluable string when using
      Declarative.

      .. warning:: When passed as a Python-evaluable string, the
         argument is interpreted using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

      .. seealso::

        :ref:`self_referential` - in-depth explanation of how
        :paramref:`_orm.relationship.remote_side`
        is used to configure self-referential relationships.

        :func:`.remote` - an annotation function that accomplishes the
        same purpose as :paramref:`_orm.relationship.remote_side`,
        typically
        when a custom :paramref:`_orm.relationship.primaryjoin` condition
        is used.

    :param query_class:
      A :class:`_query.Query`
      subclass that will be used internally by the
      ``AppenderQuery`` returned by a "dynamic" relationship, that
      is, a relationship that specifies ``lazy="dynamic"`` or was
      otherwise constructed using the :func:`_orm.dynamic_loader`
      function.

      .. seealso::

        :ref:`dynamic_relationship` - Introduction to "dynamic"
        relationship loaders.

    :param secondaryjoin:
      A SQL expression that will be used as the join of
      an association table to the child object. By default, this value is
      computed based on the foreign key relationships of the association
      and child tables.

      :paramref:`_orm.relationship.secondaryjoin` may also be passed as a
      callable function which is evaluated at mapper initialization time,
      and may be passed as a Python-evaluable string when using
      Declarative.

      .. warning:: When passed as a Python-evaluable string, the
         argument is interpreted using Python's ``eval()`` function.
         **DO NOT PASS UNTRUSTED INPUT TO THIS STRING**.
         See :ref:`declarative_relationship_eval` for details on
         declarative evaluation of :func:`_orm.relationship` arguments.

      .. seealso::

          :ref:`relationship_primaryjoin`

    :param single_parent:
      When True, installs a validator which will prevent objects
      from being associated with more than one parent at a time.
      This is used for many-to-one or many-to-many relationships that
      should be treated either as one-to-one or one-to-many.  Its usage
      is optional, except for :func:`_orm.relationship` constructs which
      are many-to-one or many-to-many and also
      specify the ``delete-orphan`` cascade option.  The
      :func:`_orm.relationship` construct itself will raise an error
      instructing when this option is required.

      .. seealso::

        :ref:`unitofwork_cascades` - includes detail on when the
        :paramref:`_orm.relationship.single_parent`
        flag may be appropriate.

    :param uselist:
      A boolean that indicates if this property should be loaded as a
      list or a scalar. In most cases, this value is determined
      automatically by :func:`_orm.relationship` at mapper configuration
      time, based on the type and direction
      of the relationship - one to many forms a list, many to one
      forms a scalar, many to many is a list. If a scalar is desired
      where normally a list would be present, such as a bi-directional
      one-to-one relationship, set :paramref:`_orm.relationship.uselist`
      to
      False.

      The :paramref:`_orm.relationship.uselist`
      flag is also available on an
      existing :func:`_orm.relationship`
      construct as a read-only attribute,
      which can be used to determine if this :func:`_orm.relationship`
      deals
      with collections or scalar attributes::

          >>> User.addresses.property.uselist
          True

      .. seealso::

          :ref:`relationships_one_to_one` - Introduction to the "one to
          one" relationship pattern, which is typically when the
          :paramref:`_orm.relationship.uselist` flag is needed.

    :param viewonly=False:
      When set to ``True``, the relationship is used only for loading
      objects, and not for any persistence operation.  A
      :func:`_orm.relationship` which specifies
      :paramref:`_orm.relationship.viewonly` can work
      with a wider range of SQL operations within the
      :paramref:`_orm.relationship.primaryjoin` condition, including
      operations that feature the use of a variety of comparison operators
      as well as SQL functions such as :func:`_expression.cast`.  The
      :paramref:`_orm.relationship.viewonly`
      flag is also of general use when defining any kind of
      :func:`_orm.relationship` that doesn't represent
      the full set of related objects, to prevent modifications of the
      collection from resulting in persistence operations.

      When using the :paramref:`_orm.relationship.viewonly` flag in
      conjunction with backrefs, the originating relationship for a
      particular state change will not produce state changes within the
      viewonly relationship.   This is the behavior implied by
      :paramref:`_orm.relationship.sync_backref` being set to False.

      .. versionchanged:: 1.3.17 - the
         :paramref:`_orm.relationship.sync_backref` flag is set to False
             when using viewonly in conjunction with backrefs.

      .. seealso::

        :paramref:`_orm.relationship.sync_backref`

    :param sync_backref:
      A boolean that enables the events used to synchronize the in-Python
      attributes when this relationship is target of either
      :paramref:`_orm.relationship.backref` or
      :paramref:`_orm.relationship.back_populates`.

      Defaults to ``None``, which indicates that an automatic value should
      be selected based on the value of the
      :paramref:`_orm.relationship.viewonly` flag.  When left at its
      default, changes in state will be back-populated only if neither
      sides of a relationship is viewonly.

      .. versionadded:: 1.3.17

      .. versionchanged:: 1.4 - A relationship that specifies
         :paramref:`_orm.relationship.viewonly` automatically implies
         that :paramref:`_orm.relationship.sync_backref` is ``False``.

      .. seealso::

        :paramref:`_orm.relationship.viewonly`

    :param omit_join:
      Allows manual control over the "selectin" automatic join
      optimization.  Set to ``False`` to disable the "omit join" feature
      added in SQLAlchemy 1.3; or leave as ``None`` to leave automatic
      optimization in place.

      .. note:: This flag may only be set to ``False``.   It is not
         necessary to set it to ``True`` as the "omit_join" optimization is
         automatically detected; if it is not detected, then the
         optimization is not supported.

         .. versionchanged:: 1.3.11  setting ``omit_join`` to True will now
            emit a warning as this was not the intended use of this flag.

      .. versionadded:: 1.3


    """
    return RelationshipProperty(
        argument,
        secondary,
        primaryjoin,
        secondaryjoin,
        foreign_keys,
        uselist,
        order_by,
        backref,
        back_populates,
        overlaps,
        post_update,
        cascade,
        viewonly,
        lazy,
        collection_class,
        passive_deletes,
        passive_updates,
        remote_side,
        enable_typechecks,
        join_depth,
        comparator_factory,
        single_parent,
        innerjoin,
        distinct_target_key,
        doc,
        active_history,
        cascade_backrefs,
        load_on_pending,
        bake_queries,
        _local_remote_pairs,
        query_class,
        info,
        omit_join,
        sync_backref,
        _legacy_inactive_history_style,
    )


def synonym(
    name,
    map_column=None,
    descriptor=None,
    comparator_factory=None,
    doc=None,
    info=None,
) -> "Mapped":
    """Denote an attribute name as a synonym to a mapped property,
    in that the attribute will mirror the value and expression behavior
    of another attribute.

    e.g.::

        class MyClass(Base):
            __tablename__ = 'my_table'

            id = Column(Integer, primary_key=True)
            job_status = Column(String(50))

            status = synonym("job_status")


    :param name: the name of the existing mapped property.  This
      can refer to the string name ORM-mapped attribute
      configured on the class, including column-bound attributes
      and relationships.

    :param descriptor: a Python :term:`descriptor` that will be used
      as a getter (and potentially a setter) when this attribute is
      accessed at the instance level.

    :param map_column: **For classical mappings and mappings against
      an existing Table object only**.  if ``True``, the :func:`.synonym`
      construct will locate the :class:`_schema.Column`
      object upon the mapped
      table that would normally be associated with the attribute name of
      this synonym, and produce a new :class:`.ColumnProperty` that instead
      maps this :class:`_schema.Column`
      to the alternate name given as the "name"
      argument of the synonym; in this way, the usual step of redefining
      the mapping of the :class:`_schema.Column`
      to be under a different name is
      unnecessary. This is usually intended to be used when a
      :class:`_schema.Column`
      is to be replaced with an attribute that also uses a
      descriptor, that is, in conjunction with the
      :paramref:`.synonym.descriptor` parameter::

        my_table = Table(
            "my_table", metadata,
            Column('id', Integer, primary_key=True),
            Column('job_status', String(50))
        )

        class MyClass:
            @property
            def _job_status_descriptor(self):
                return "Status: %s" % self._job_status


        mapper(
            MyClass, my_table, properties={
                "job_status": synonym(
                    "_job_status", map_column=True,
                    descriptor=MyClass._job_status_descriptor)
            }
        )

      Above, the attribute named ``_job_status`` is automatically
      mapped to the ``job_status`` column::

        >>> j1 = MyClass()
        >>> j1._job_status = "employed"
        >>> j1.job_status
        Status: employed

      When using Declarative, in order to provide a descriptor in
      conjunction with a synonym, use the
      :func:`sqlalchemy.ext.declarative.synonym_for` helper.  However,
      note that the :ref:`hybrid properties <mapper_hybrids>` feature
      should usually be preferred, particularly when redefining attribute
      behavior.

    :param info: Optional data dictionary which will be populated into the
        :attr:`.InspectionAttr.info` attribute of this object.

        .. versionadded:: 1.0.0

    :param comparator_factory: A subclass of :class:`.PropComparator`
      that will provide custom comparison behavior at the SQL expression
      level.

      .. note::

        For the use case of providing an attribute which redefines both
        Python-level and SQL-expression level behavior of an attribute,
        please refer to the Hybrid attribute introduced at
        :ref:`mapper_hybrids` for a more effective technique.

    .. seealso::

        :ref:`synonyms` - Overview of synonyms

        :func:`.synonym_for` - a helper oriented towards Declarative

        :ref:`mapper_hybrids` - The Hybrid Attribute extension provides an
        updated approach to augmenting attribute behavior more flexibly
        than can be achieved with synonyms.

    """
    return SynonymProperty(
        name, map_column, descriptor, comparator_factory, doc, info
    )


def create_session(bind=None, **kwargs):
    r"""Create a new :class:`.Session`
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
    False.

    Usage::

      >>> from sqlalchemy.orm import create_session
      >>> session = create_session()

    It is recommended to use :func:`sessionmaker` instead of
    create_session().

    """

    kwargs.setdefault("autoflush", False)
    kwargs.setdefault("expire_on_commit", False)
    return Session(bind=bind, **kwargs)


def _mapper_fn(*arg, **kw):
    """Placeholder for the now-removed ``mapper()`` function.

    Classical mappings should be performed using the
    :meth:`_orm.registry.map_imperatively` method.

    This symbol remains in SQLAlchemy 2.0 to suit the deprecated use case
    of using the ``mapper()`` function as a target for ORM event listeners,
    which failed to be marked as deprecated in the 1.4 series.

    Global ORM mapper listeners should instead use the :class:`_orm.Mapper`
    class as the target.

    .. versionchanged:: 2.0  The ``mapper()`` function was removed; the
       symbol remains temporarily as a placeholder for the event listening
       use case.

    """
    raise InvalidRequestError(
        "The 'sqlalchemy.orm.mapper()' function is removed as of "
        "SQLAlchemy 2.0.  Use the "
        "'sqlalchemy.orm.registry.map_imperatively()` "
        "method of the ``sqlalchemy.orm.registry`` class to perform "
        "classical mapping."
    )


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
    kw["lazy"] = "dynamic"
    return relationship(argument, **kw)


def backref(name, **kwargs):
    """Create a back reference with explicit keyword arguments, which are the
    same arguments one can send to :func:`relationship`.

    Used with the ``backref`` keyword argument to :func:`relationship` in
    place of a string argument, e.g.::

        'items':relationship(
            SomeItem, backref=backref('parent', lazy='subquery'))

    .. seealso::

        :ref:`relationships_backref`

    """

    return (name, kwargs)


def deferred(*columns, **kw):
    r"""Indicate a column-based mapped attribute that by default will
    not load unless accessed.

    :param \*columns: columns to be mapped.  This is typically a single
     :class:`_schema.Column` object,
     however a collection is supported in order
     to support multiple columns mapped under the same attribute.

    :param raiseload: boolean, if True, indicates an exception should be raised
     if the load operation is to take place.

     .. versionadded:: 1.4

     .. seealso::

        :ref:`deferred_raiseload`

    :param \**kw: additional keyword arguments passed to
     :class:`.ColumnProperty`.

    .. seealso::

        :ref:`deferred`

    """
    return ColumnProperty(deferred=True, *columns, **kw)


def query_expression(default_expr=sql.null()):
    """Indicate an attribute that populates from a query-time SQL expression.

    :param default_expr: Optional SQL expression object that will be used in
        all cases if not assigned later with :func:`_orm.with_expression`.
        E.g.::

            from sqlalchemy.sql import literal

            class C(Base):
                #...
                my_expr = query_expression(literal(1))

        .. versionadded:: 1.3.18


    .. versionadded:: 1.2

    .. seealso::

        :ref:`mapper_querytime_expression`

    """
    prop = ColumnProperty(default_expr)
    prop.strategy_key = (("query_expression", True),)
    return prop


def clear_mappers():
    """Remove all mappers from all classes.

    .. versionchanged:: 1.4  This function now locates all
       :class:`_orm.registry` objects and calls upon the
       :meth:`_orm.registry.dispose` method of each.

    This function removes all instrumentation from classes and disposes
    of their associated mappers.  Once called, the classes are unmapped
    and can be later re-mapped with new mappers.

    :func:`.clear_mappers` is *not* for normal use, as there is literally no
    valid usage for it outside of very specific testing scenarios. Normally,
    mappers are permanent structural components of user-defined classes, and
    are never discarded independently of their class.  If a mapped class
    itself is garbage collected, its mapper is automatically disposed of as
    well. As such, :func:`.clear_mappers` is only for usage in test suites
    that re-use the same classes with different mappings, which is itself an
    extremely rare use case - the only such use case is in fact SQLAlchemy's
    own test suite, and possibly the test suites of other ORM extension
    libraries which intend to test various combinations of mapper construction
    upon a fixed set of classes.

    """

    mapperlib._dispose_registries(mapperlib._all_registries(), False)
