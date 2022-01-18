# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""

"""

import typing
from typing import Any
from typing import cast
from typing import Mapping
from typing import NoReturn
from typing import Optional
from typing import Tuple
from typing import Union

from . import util as orm_util
from .base import InspectionAttr
from .interfaces import LoaderOption
from .path_registry import _DEFAULT_TOKEN
from .path_registry import _WILDCARD_TOKEN
from .path_registry import PathRegistry
from .path_registry import TokenRegistry
from .util import _orm_full_deannotate
from .util import AliasedInsp
from .. import exc as sa_exc
from .. import inspect
from .. import util
from ..sql import and_
from ..sql import cache_key
from ..sql import coercions
from ..sql import roles
from ..sql import traversals
from ..sql import visitors
from ..sql.base import _generative

_RELATIONSHIP_TOKEN = "relationship"
_COLUMN_TOKEN = "column"

if typing.TYPE_CHECKING:
    from .mapper import Mapper

Self_AbstractLoad = typing.TypeVar("Self_AbstractLoad", bound="_AbstractLoad")


class _AbstractLoad(traversals.GenerativeOnTraversal, LoaderOption):
    __slots__ = ("propagate_to_loaders",)

    _is_strategy_option = True
    propagate_to_loaders: bool

    def contains_eager(self, attr, alias=None, _is_chain=False):
        r"""Indicate that the given attribute should be eagerly loaded from
        columns stated manually in the query.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        The option is used in conjunction with an explicit join that loads
        the desired rows, i.e.::

            sess.query(Order).\
                    join(Order.user).\
                    options(contains_eager(Order.user))

        The above query would join from the ``Order`` entity to its related
        ``User`` entity, and the returned ``Order`` objects would have the
        ``Order.user`` attribute pre-populated.

        It may also be used for customizing the entries in an eagerly loaded
        collection; queries will normally want to use the
        :ref:`orm_queryguide_populate_existing` execution option assuming the
        primary collection of parent objects may already have been loaded::

            sess.query(User).\
                join(User.addresses).\
                filter(Address.email_address.like('%@aol.com')).\
                options(contains_eager(User.addresses)).\
                populate_existing()

        See the section :ref:`contains_eager` for complete usage details.

        .. seealso::

            :ref:`loading_toplevel`

            :ref:`contains_eager`

        """
        if alias is not None:
            if not isinstance(alias, str):
                info = inspect(alias)
                alias = info.selectable

            else:
                util.warn_deprecated(
                    "Passing a string name for the 'alias' argument to "
                    "'contains_eager()` is deprecated, and will not work in a "
                    "future release.  Please use a sqlalchemy.alias() or "
                    "sqlalchemy.orm.aliased() construct.",
                    version="1.4",
                )

        elif getattr(attr, "_of_type", None):
            ot = inspect(attr._of_type)
            alias = ot.selectable

        cloned = self._set_relationship_strategy(
            attr,
            {"lazy": "joined"},
            propagate_to_loaders=False,
            opts={"eager_from_alias": alias},
            _reconcile_to_other=True if _is_chain else None,
        )
        return cloned

    def load_only(self, *attrs):
        """Indicate that for a particular entity, only the given list
        of column-based attribute names should be loaded; all others will be
        deferred.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        Example - given a class ``User``, load only the ``name`` and
        ``fullname`` attributes::

            session.query(User).options(load_only(User.name, User.fullname))

        Example - given a relationship ``User.addresses -> Address``, specify
        subquery loading for the ``User.addresses`` collection, but on each
        ``Address`` object load only the ``email_address`` attribute::

            session.query(User).options(
                subqueryload(User.addresses).load_only(Address.email_address)
            )

        For a statement that has multiple entities,
        the lead entity can be
        specifically referred to using the :class:`_orm.Load` constructor::

            stmt = select(User, Address).join(User.addresses).options(
                        Load(User).load_only(User.name, User.fullname),
                        Load(Address).load_only(Address.email_address)
                    )

        .. note:: This method will still load a :class:`_schema.Column` even
            if the column property is defined with ``deferred=True``
            for the :func:`.column_property` function.

        """
        cloned = self._set_column_strategy(
            attrs,
            {"deferred": False, "instrument": True},
        )
        cloned = cloned._set_column_strategy(
            "*", {"deferred": True, "instrument": True}, {"undefer_pks": True}
        )
        return cloned

    def joinedload(self, attr, innerjoin=None):
        """Indicate that the given attribute should be loaded using joined
        eager loading.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        examples::

            # joined-load the "orders" collection on "User"
            query(User).options(joinedload(User.orders))

            # joined-load Order.items and then Item.keywords
            query(Order).options(
                joinedload(Order.items).joinedload(Item.keywords))

            # lazily load Order.items, but when Items are loaded,
            # joined-load the keywords collection
            query(Order).options(
                lazyload(Order.items).joinedload(Item.keywords))

        :param innerjoin: if ``True``, indicates that the joined eager load
         should use an inner join instead of the default of left outer join::

            query(Order).options(joinedload(Order.user, innerjoin=True))

        In order to chain multiple eager joins together where some may be
        OUTER and others INNER, right-nested joins are used to link them::

            query(A).options(
                joinedload(A.bs, innerjoin=False).
                    joinedload(B.cs, innerjoin=True)
            )

        The above query, linking A.bs via "outer" join and B.cs via "inner"
        join would render the joins as "a LEFT OUTER JOIN (b JOIN c)". When
        using older versions of SQLite (< 3.7.16), this form of JOIN is
        translated to use full subqueries as this syntax is otherwise not
        directly supported.

        The ``innerjoin`` flag can also be stated with the term ``"unnested"``.
        This indicates that an INNER JOIN should be used, *unless* the join
        is linked to a LEFT OUTER JOIN to the left, in which case it
        will render as LEFT OUTER JOIN.  For example, supposing ``A.bs``
        is an outerjoin::

            query(A).options(
                joinedload(A.bs).
                    joinedload(B.cs, innerjoin="unnested")
            )

        The above join will render as "a LEFT OUTER JOIN b LEFT OUTER JOIN c",
        rather than as "a LEFT OUTER JOIN (b JOIN c)".

        .. note:: The "unnested" flag does **not** affect the JOIN rendered
            from a many-to-many association table, e.g. a table configured as
            :paramref:`_orm.relationship.secondary`, to the target table; for
            correctness of results, these joins are always INNER and are
            therefore right-nested if linked to an OUTER join.

        .. versionchanged:: 1.0.0 ``innerjoin=True`` now implies
            ``innerjoin="nested"``, whereas in 0.9 it implied
            ``innerjoin="unnested"``. In order to achieve the pre-1.0
            "unnested" inner join behavior, use the value
            ``innerjoin="unnested"``. See :ref:`migration_3008`.

        .. note::

            The joins produced by :func:`_orm.joinedload` are **anonymously
            aliased**. The criteria by which the join proceeds cannot be
            modified, nor can the ORM-enabled :class:`_sql.Select` or legacy
            :class:`_query.Query` refer to these joins in any way, including
            ordering. See :ref:`zen_of_eager_loading` for further detail.

            To produce a specific SQL JOIN which is explicitly available, use
            :meth:`_sql.Select.join` and :meth:`_query.Query.join`. To combine
            explicit JOINs with eager loading of collections, use
            :func:`_orm.contains_eager`; see :ref:`contains_eager`.

        .. seealso::

            :ref:`loading_toplevel`

            :ref:`joined_eager_loading`

        """
        loader = self._set_relationship_strategy(
            attr,
            {"lazy": "joined"},
            opts={"innerjoin": innerjoin}
            if innerjoin is not None
            else util.EMPTY_DICT,
        )
        return loader

    def subqueryload(self, attr):
        """Indicate that the given attribute should be loaded using
        subquery eager loading.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        examples::

            # subquery-load the "orders" collection on "User"
            query(User).options(subqueryload(User.orders))

            # subquery-load Order.items and then Item.keywords
            query(Order).options(
                subqueryload(Order.items).subqueryload(Item.keywords))

            # lazily load Order.items, but when Items are loaded,
            # subquery-load the keywords collection
            query(Order).options(
                lazyload(Order.items).subqueryload(Item.keywords))


        .. seealso::

            :ref:`loading_toplevel`

            :ref:`subquery_eager_loading`

        """
        return self._set_relationship_strategy(attr, {"lazy": "subquery"})

    def selectinload(self, attr):
        """Indicate that the given attribute should be loaded using
        SELECT IN eager loading.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        examples::

            # selectin-load the "orders" collection on "User"
            query(User).options(selectinload(User.orders))

            # selectin-load Order.items and then Item.keywords
            query(Order).options(
                selectinload(Order.items).selectinload(Item.keywords))

            # lazily load Order.items, but when Items are loaded,
            # selectin-load the keywords collection
            query(Order).options(
                lazyload(Order.items).selectinload(Item.keywords))

        .. versionadded:: 1.2

        .. seealso::

            :ref:`loading_toplevel`

            :ref:`selectin_eager_loading`

        """
        return self._set_relationship_strategy(attr, {"lazy": "selectin"})

    def lazyload(self, attr):
        """Indicate that the given attribute should be loaded using "lazy"
        loading.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        .. seealso::

            :ref:`loading_toplevel`

            :ref:`lazy_loading`

        """
        return self._set_relationship_strategy(attr, {"lazy": "select"})

    def immediateload(self, attr):
        """Indicate that the given attribute should be loaded using
        an immediate load with a per-attribute SELECT statement.

        The load is achieved using the "lazyloader" strategy and does not
        fire off any additional eager loaders.

        The :func:`.immediateload` option is superseded in general
        by the :func:`.selectinload` option, which performs the same task
        more efficiently by emitting a SELECT for all loaded objects.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        .. seealso::

            :ref:`loading_toplevel`

            :ref:`selectin_eager_loading`

        """
        loader = self._set_relationship_strategy(attr, {"lazy": "immediate"})
        return loader

    def noload(self, attr):
        """Indicate that the given relationship attribute should remain
        unloaded.

        The relationship attribute will return ``None`` when accessed without
        producing any loading effect.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        :func:`_orm.noload` applies to :func:`_orm.relationship` attributes
        only.

        .. note:: Setting this loading strategy as the default strategy
            for a relationship using the :paramref:`.orm.relationship.lazy`
            parameter may cause issues with flushes, such if a delete operation
            needs to load related objects and instead ``None`` was returned.

        .. seealso::

            :ref:`loading_toplevel`

        """

        return self._set_relationship_strategy(attr, {"lazy": "noload"})

    def raiseload(self, attr, sql_only=False):
        """Indicate that the given attribute should raise an error if accessed.

        A relationship attribute configured with :func:`_orm.raiseload` will
        raise an :exc:`~sqlalchemy.exc.InvalidRequestError` upon access. The
        typical way this is useful is when an application is attempting to
        ensure that all relationship attributes that are accessed in a
        particular context would have been already loaded via eager loading.
        Instead of having to read through SQL logs to ensure lazy loads aren't
        occurring, this strategy will cause them to raise immediately.

        :func:`_orm.raiseload` applies to :func:`_orm.relationship` attributes
        only. In order to apply raise-on-SQL behavior to a column-based
        attribute, use the :paramref:`.orm.defer.raiseload` parameter on the
        :func:`.defer` loader option.

        :param sql_only: if True, raise only if the lazy load would emit SQL,
         but not if it is only checking the identity map, or determining that
         the related value should just be None due to missing keys. When False,
         the strategy will raise for all varieties of relationship loading.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.


        .. versionadded:: 1.1

        .. seealso::

            :ref:`loading_toplevel`

            :ref:`prevent_lazy_with_raiseload`

            :ref:`deferred_raiseload`

        """

        return self._set_relationship_strategy(
            attr, {"lazy": "raise_on_sql" if sql_only else "raise"}
        )

    def defaultload(self, attr):
        """Indicate an attribute should load using its default loader style.

        This method is used to link to other loader options further into
        a chain of attributes without altering the loader style of the links
        along the chain.  For example, to set joined eager loading for an
        element of an element::

            session.query(MyClass).options(
                defaultload(MyClass.someattribute).
                joinedload(MyOtherClass.someotherattribute)
            )

        :func:`.defaultload` is also useful for setting column-level options on
        a related class, namely that of :func:`.defer` and :func:`.undefer`::

            session.query(MyClass).options(
                defaultload(MyClass.someattribute).
                defer("some_column").
                undefer("some_other_column")
            )

        .. seealso::

            :meth:`_orm.Load.options` - allows for complex hierarchical
            loader option structures with less verbosity than with individual
            :func:`.defaultload` directives.

            :ref:`relationship_loader_options`

            :ref:`deferred_loading_w_multiple`

        """
        return self._set_relationship_strategy(attr, None)

    def defer(self, key, raiseload=False):
        r"""Indicate that the given column-oriented attribute should be
        deferred, e.g. not loaded until accessed.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        e.g.::

            from sqlalchemy.orm import defer

            session.query(MyClass).options(
                defer(MyClass.attribute_one),
                defer(MyClass.attribute_two)
            )

        To specify a deferred load of an attribute on a related class,
        the path can be specified one token at a time, specifying the loading
        style for each link along the chain.  To leave the loading style
        for a link unchanged, use :func:`_orm.defaultload`::

            session.query(MyClass).options(
                defaultload(MyClass.someattr).defer(RelatedClass.some_column)
            )

        Multiple deferral options related to a relationship can be bundled
        at once using :meth:`_orm.Load.options`::


            session.query(MyClass).options(
                defaultload(MyClass.someattr).options(
                    defer(RelatedClass.some_column),
                    defer(RelatedClass.some_other_column),
                    defer(RelatedClass.another_column)
                )
            )

        :param key: Attribute to be deferred.

        :param raiseload: raise :class:`.InvalidRequestError` if the column
         value is to be loaded from emitting SQL.   Used to prevent unwanted
         SQL from being emitted.

        .. versionadded:: 1.4

        .. seealso::

            :ref:`deferred_raiseload`

        .. seealso::

            :ref:`deferred`

            :func:`_orm.undefer`

        """
        strategy = {"deferred": True, "instrument": True}
        if raiseload:
            strategy["raiseload"] = True
        return self._set_column_strategy((key,), strategy)

    def undefer(self, key):
        r"""Indicate that the given column-oriented attribute should be
        undeferred, e.g. specified within the SELECT statement of the entity
        as a whole.

        The column being undeferred is typically set up on the mapping as a
        :func:`.deferred` attribute.

        This function is part of the :class:`_orm.Load` interface and supports
        both method-chained and standalone operation.

        Examples::

            # undefer two columns
            session.query(MyClass).options(undefer("col1"), undefer("col2"))

            # undefer all columns specific to a single class using Load + *
            session.query(MyClass, MyOtherClass).options(
                Load(MyClass).undefer("*"))

            # undefer a column on a related object
            session.query(MyClass).options(
                defaultload(MyClass.items).undefer('text'))

        :param key: Attribute to be undeferred.

        .. seealso::

            :ref:`deferred`

            :func:`_orm.defer`

            :func:`_orm.undefer_group`

        """
        return self._set_column_strategy(
            (key,), {"deferred": False, "instrument": True}
        )

    def undefer_group(self, name):
        """Indicate that columns within the given deferred group name should be
        undeferred.

        The columns being undeferred are set up on the mapping as
        :func:`.deferred` attributes and include a "group" name.

        E.g::

            session.query(MyClass).options(undefer_group("large_attrs"))

        To undefer a group of attributes on a related entity, the path can be
        spelled out using relationship loader options, such as
        :func:`_orm.defaultload`::

            session.query(MyClass).options(
                defaultload("someattr").undefer_group("large_attrs"))

        .. seealso::

            :ref:`deferred`

            :func:`_orm.defer`

            :func:`_orm.undefer`

        """
        return self._set_column_strategy(
            _WILDCARD_TOKEN, None, {f"undefer_group_{name}": True}
        )

    def with_expression(self, key, expression):
        r"""Apply an ad-hoc SQL expression to a "deferred expression"
        attribute.

        This option is used in conjunction with the
        :func:`_orm.query_expression` mapper-level construct that indicates an
        attribute which should be the target of an ad-hoc SQL expression.

        E.g.::

            sess.query(SomeClass).options(
                with_expression(SomeClass.x_y_expr, SomeClass.x + SomeClass.y)
            )

        .. versionadded:: 1.2

        :param key: Attribute to be undeferred.

        :param expr: SQL expression to be applied to the attribute.

        .. note:: the target attribute is populated only if the target object
           is **not currently loaded** in the current :class:`_orm.Session`
           unless the :ref:`orm_queryguide_populate_existing` execution option
           is used. Please refer to :ref:`mapper_querytime_expression` for
           complete usage details.

        .. seealso::

            :ref:`mapper_querytime_expression`

        """

        expression = coercions.expect(
            roles.LabeledColumnExprRole, _orm_full_deannotate(expression)
        )

        return self._set_column_strategy(
            (key,), {"query_expression": True}, opts={"expression": expression}
        )

    def selectin_polymorphic(self, classes):
        """Indicate an eager load should take place for all attributes
        specific to a subclass.

        This uses an additional SELECT with IN against all matched primary
        key values, and is the per-query analogue to the ``"selectin"``
        setting on the :paramref:`.mapper.polymorphic_load` parameter.

        .. versionadded:: 1.2

        .. seealso::

            :ref:`polymorphic_selectin`

        """
        self = self._set_class_strategy(
            {"selectinload_polymorphic": True},
            opts={
                "entities": tuple(
                    sorted((inspect(cls) for cls in classes), key=id)
                )
            },
        )
        return self

    def _coerce_strat(self, strategy):
        if strategy is not None:
            strategy = tuple(sorted(strategy.items()))
        return strategy

    @_generative
    def _set_relationship_strategy(
        self: Self_AbstractLoad,
        attr,
        strategy,
        propagate_to_loaders=True,
        opts=None,
        _reconcile_to_other=None,
    ) -> Self_AbstractLoad:
        strategy = self._coerce_strat(strategy)

        self._clone_for_bind_strategy(
            (attr,),
            strategy,
            _RELATIONSHIP_TOKEN,
            opts=opts,
            propagate_to_loaders=propagate_to_loaders,
            reconcile_to_other=_reconcile_to_other,
        )
        return self

    @_generative
    def _set_column_strategy(
        self: Self_AbstractLoad, attrs, strategy, opts=None
    ) -> Self_AbstractLoad:
        strategy = self._coerce_strat(strategy)

        self._clone_for_bind_strategy(
            attrs,
            strategy,
            _COLUMN_TOKEN,
            opts=opts,
            attr_group=attrs,
        )
        return self

    @_generative
    def _set_generic_strategy(
        self: Self_AbstractLoad, attrs, strategy, _reconcile_to_other=None
    ) -> Self_AbstractLoad:
        strategy = self._coerce_strat(strategy)
        self._clone_for_bind_strategy(
            attrs,
            strategy,
            None,
            propagate_to_loaders=True,
            reconcile_to_other=_reconcile_to_other,
        )
        return self

    @_generative
    def _set_class_strategy(
        self: Self_AbstractLoad, strategy, opts
    ) -> Self_AbstractLoad:
        strategy = self._coerce_strat(strategy)

        self._clone_for_bind_strategy(None, strategy, None, opts=opts)
        return self

    def _apply_to_parent(self, parent):
        """apply this :class:`_orm._AbstractLoad` object as a sub-option o
        a :class:`_orm.Load` object.

        Implementation is provided by subclasses.

        """
        raise NotImplementedError()

    def options(self: Self_AbstractLoad, *opts) -> NoReturn:
        r"""Apply a series of options as sub-options to this
        :class:`_orm._AbstractLoad` object.

        Implementation is provided by subclasses.

        """
        raise NotImplementedError()

    def _clone_for_bind_strategy(
        self,
        attrs,
        strategy,
        wildcard_key,
        opts=None,
        attr_group=None,
        propagate_to_loaders=True,
        reconcile_to_other=None,
    ):
        raise NotImplementedError()

    def process_compile_state_replaced_entities(
        self, compile_state, mapper_entities
    ):
        if not compile_state.compile_options._enable_eagerloads:
            return

        # process is being run here so that the options given are validated
        # against what the lead entities were, as well as to accommodate
        # for the entities having been replaced with equivalents
        self._process(
            compile_state,
            mapper_entities,
            not bool(compile_state.current_path),
        )

    def process_compile_state(self, compile_state):
        if not compile_state.compile_options._enable_eagerloads:
            return

        self._process(
            compile_state,
            compile_state._lead_mapper_entities,
            not bool(compile_state.current_path)
            and not compile_state.compile_options._for_refresh_state,
        )

    def _process(self, compile_state, mapper_entities, raiseerr):
        """implemented by subclasses"""
        raise NotImplementedError()

    @classmethod
    def _chop_path(cls, to_chop, path, debug=False):
        i = -1

        for i, (c_token, p_token) in enumerate(zip(to_chop, path.path)):
            if isinstance(c_token, str):
                if i == 0 and c_token.endswith(f":{_DEFAULT_TOKEN}"):
                    return to_chop
                elif (
                    c_token != f"{_RELATIONSHIP_TOKEN}:{_WILDCARD_TOKEN}"
                    and c_token != p_token.key
                ):
                    return None

            if c_token is p_token:
                continue
            elif (
                isinstance(c_token, InspectionAttr)
                and c_token.is_mapper
                and (
                    (p_token.is_mapper and c_token.isa(p_token))
                    or (
                        # a too-liberal check here to allow a path like
                        # A->A.bs->B->B.cs->C->C.ds, natural path, to chop
                        # against current path
                        # A->A.bs->B(B, B2)->B(B, B2)->cs, in an of_type()
                        # scenario which should only be occurring in a loader
                        # that is against a non-aliased lead element with
                        # single path.  otherwise the
                        # "B" won't match into the B(B, B2).
                        #
                        # i>=2 prevents this check from proceeding for
                        # the first path element.
                        #
                        # if we could do away with the "natural_path"
                        # concept, we would not need guessy checks like this
                        #
                        # two conflicting tests for this comparison are:
                        # test_eager_relations.py->
                        #       test_lazyload_aliased_abs_bcs_two
                        # and
                        # test_of_type.py->test_all_subq_query
                        #
                        i >= 2
                        and p_token.is_aliased_class
                        and p_token._is_with_polymorphic
                        and c_token in p_token.with_polymorphic_mappers
                        # and (breakpoint() or True)
                    )
                )
            ):
                continue

            else:
                return None
        return to_chop[i + 1 :]


SelfLoad = typing.TypeVar("SelfLoad", bound="Load")


class Load(_AbstractLoad):
    """Represents loader options which modify the state of a
    ORM-enabled :class:`_sql.Select` or a legacy :class:`_query.Query` in
    order to affect how various mapped attributes are loaded.

    The :class:`_orm.Load` object is in most cases used implicitly behind the
    scenes when one makes use of a query option like :func:`_orm.joinedload`,
    :func:`.defer`, or similar.   However, the :class:`_orm.Load` object
    can also be used directly, and in some cases can be useful.

    To use :class:`_orm.Load` directly, instantiate it with the target mapped
    class as the argument.   This style of usage is
    useful when dealing with a statement
    that has multiple entities::

        myopt = Load(MyClass).joinedload("widgets")

    The above ``myopt`` can now be used with :meth:`_sql.Select.options` or
    :meth:`_query.Query.options` where it
    will only take effect for the ``MyClass`` entity::

        stmt = select(MyClass, MyOtherClass).options(myopt)

    One case where :class:`_orm.Load`
    is useful as public API is when specifying
    "wildcard" options that only take effect for a certain class::

        stmt = select(Order).options(Load(Order).lazyload('*'))

    Above, all relationships on ``Order`` will be lazy-loaded, but other
    attributes on those descendant objects will load using their normal
    loader strategy.

    .. seealso::

        :ref:`deferred_options`

        :ref:`deferred_loading_w_multiple`

        :ref:`relationship_loader_options`

    """

    __slots__ = (
        "path",
        "context",
    )

    _traverse_internals = [
        ("path", visitors.ExtendedInternalTraversal.dp_has_cache_key),
        (
            "context",
            visitors.InternalTraversal.dp_has_cache_key_list,
        ),
        ("propagate_to_loaders", visitors.InternalTraversal.dp_boolean),
    ]
    _cache_key_traversal = None

    path: PathRegistry
    context: Tuple["_LoadElement", ...]

    def __init__(self, entity):
        insp = cast(Union["Mapper", AliasedInsp], inspect(entity))
        insp._post_inspect

        self.path = insp._path_registry
        self.context = ()
        self.propagate_to_loaders = False

    def __str__(self):
        return f"Load({self.path[0]})"

    @classmethod
    def _construct_for_existing_path(cls, path):
        load = cls.__new__(cls)
        load.path = path
        load.context = ()
        load.propagate_to_loaders = False
        return load

    def _adjust_for_extra_criteria(self, context):
        """Apply the current bound parameters in a QueryContext to all
        occurrences "extra_criteria" stored within this ``Load`` object,
        returning a new instance of this ``Load`` object.

        """
        orig_query = context.compile_state.select_statement

        orig_cache_key = None
        replacement_cache_key = None

        def process(opt):
            if not opt._extra_criteria:
                return opt

            nonlocal orig_cache_key, replacement_cache_key

            # avoid generating cache keys for the queries if we don't
            # actually have any extra_criteria options, which is the
            # common case
            if orig_cache_key is None or replacement_cache_key is None:
                orig_cache_key = orig_query._generate_cache_key()
                replacement_cache_key = context.query._generate_cache_key()

            opt._extra_criteria = tuple(
                replacement_cache_key._apply_params_to_element(
                    orig_cache_key, crit
                )
                for crit in opt._extra_criteria
            )
            return opt

        cloned = self._generate()

        if self.context:
            cloned.context = tuple(
                process(value._clone()) for value in self.context
            )

        return cloned

    def _reconcile_query_entities_with_us(self, mapper_entities, raiseerr):
        """called at process time to allow adjustment of the root
        entity inside of _LoadElement objects.

        """
        path = self.path

        ezero = None
        for ent in mapper_entities:
            ezero = ent.entity_zero
            if ezero and orm_util._entity_corresponds_to(ezero, path[0]):
                return ezero

        return None

    def _process(self, compile_state, mapper_entities, raiseerr):

        reconciled_lead_entity = self._reconcile_query_entities_with_us(
            mapper_entities, raiseerr
        )

        for loader in self.context:
            loader.process_compile_state(
                self,
                compile_state,
                mapper_entities,
                reconciled_lead_entity,
                raiseerr,
            )

    def _apply_to_parent(self, parent):
        """apply this :class:`_orm.Load` object as a sub-option of another
        :class:`_orm.Load` object.

        This method is used by the :meth:`_orm.Load.options` method.

        """
        cloned = self._generate()

        assert cloned.propagate_to_loaders == self.propagate_to_loaders

        if not orm_util._entity_corresponds_to_use_path_impl(
            parent.path[-1], cloned.path[0]
        ):
            raise sa_exc.ArgumentError(
                f'Attribute "{cloned.path[1]}" does not link '
                f'from element "{parent.path[-1]}".'
            )

        cloned.path = PathRegistry.coerce(parent.path[0:-1] + cloned.path[:])

        if self.context:
            cloned.context = tuple(
                value._prepend_path_from(parent) for value in self.context
            )

        if cloned.context:
            parent.context += cloned.context

    @_generative
    def options(self: SelfLoad, *opts) -> SelfLoad:
        r"""Apply a series of options as sub-options to this
        :class:`_orm.Load`
        object.

        E.g.::

            query = session.query(Author)
            query = query.options(
                        joinedload(Author.book).options(
                            load_only(Book.summary, Book.excerpt),
                            joinedload(Book.citations).options(
                                joinedload(Citation.author)
                            )
                        )
                    )

        :param \*opts: A series of loader option objects (ultimately
         :class:`_orm.Load` objects) which should be applied to the path
         specified by this :class:`_orm.Load` object.

        .. versionadded:: 1.3.6

        .. seealso::

            :func:`.defaultload`

            :ref:`relationship_loader_options`

            :ref:`deferred_loading_w_multiple`

        """
        for opt in opts:
            opt._apply_to_parent(self)
        return self

    def _clone_for_bind_strategy(
        self,
        attrs,
        strategy,
        wildcard_key,
        opts=None,
        attr_group=None,
        propagate_to_loaders=True,
        reconcile_to_other=None,
    ) -> None:
        # for individual strategy that needs to propagate, set the whole
        # Load container to also propagate, so that it shows up in
        # InstanceState.load_options
        if propagate_to_loaders:
            self.propagate_to_loaders = True

        if not self.path.has_entity:
            if self.path.is_token:
                raise sa_exc.ArgumentError(
                    "Wildcard token cannot be followed by another entity"
                )
            else:
                # re-use the lookup which will raise a nicely formatted
                # LoaderStrategyException
                if strategy:
                    self.path.prop._strategy_lookup(
                        self.path.prop, strategy[0]
                    )
                else:
                    raise sa_exc.ArgumentError(
                        f"Mapped attribute '{self.path.prop}' does not "
                        "refer to a mapped entity"
                    )

        if attrs is None:
            load_element = _ClassStrategyLoad.create(
                self.path,
                None,
                strategy,
                wildcard_key,
                opts,
                propagate_to_loaders,
                attr_group=attr_group,
                reconcile_to_other=reconcile_to_other,
            )
            if load_element:
                self.context += (load_element,)

        else:
            for attr in attrs:
                if isinstance(attr, str):
                    load_element = _TokenStrategyLoad.create(
                        self.path,
                        attr,
                        strategy,
                        wildcard_key,
                        opts,
                        propagate_to_loaders,
                        attr_group=attr_group,
                        reconcile_to_other=reconcile_to_other,
                    )
                else:
                    load_element = _AttributeStrategyLoad.create(
                        self.path,
                        attr,
                        strategy,
                        wildcard_key,
                        opts,
                        propagate_to_loaders,
                        attr_group=attr_group,
                        reconcile_to_other=reconcile_to_other,
                    )

                if load_element:
                    # for relationship options, update self.path on this Load
                    # object with the latest path.
                    if wildcard_key is _RELATIONSHIP_TOKEN:
                        self.path = load_element.path
                    self.context += (load_element,)

    def __getstate__(self):
        d = self._shallow_to_dict()
        d["path"] = self.path.serialize()
        return d

    def __setstate__(self, state):
        state["path"] = PathRegistry.deserialize(state["path"])
        self._shallow_from_dict(state)


SelfWildcardLoad = typing.TypeVar("SelfWildcardLoad", bound="_WildcardLoad")


class _WildcardLoad(_AbstractLoad):
    """represent a standalone '*' load operation"""

    __slots__ = ("strategy", "path", "local_opts")

    _traverse_internals = [
        ("strategy", visitors.ExtendedInternalTraversal.dp_plain_obj),
        ("path", visitors.ExtendedInternalTraversal.dp_plain_obj),
        (
            "local_opts",
            visitors.ExtendedInternalTraversal.dp_string_multi_dict,
        ),
    ]
    cache_key_traversal = None

    strategy: Optional[Tuple[Any, ...]]
    local_opts: Mapping[str, Any]
    path: Tuple[str, ...]
    propagate_to_loaders = False

    def __init__(self):
        self.path = ()
        self.strategy = None
        self.local_opts = util.EMPTY_DICT

    def _clone_for_bind_strategy(
        self,
        attrs,
        strategy,
        wildcard_key,
        opts=None,
        attr_group=None,
        propagate_to_loaders=True,
        reconcile_to_other=None,
    ):
        attr = attrs[0]
        assert (
            wildcard_key
            and isinstance(attr, str)
            and attr in (_WILDCARD_TOKEN, _DEFAULT_TOKEN)
        )

        attr = f"{wildcard_key}:{attr}"

        self.strategy = strategy
        self.path = (attr,)
        if opts:
            self.local_opts = util.immutabledict(opts)

    def options(self: SelfWildcardLoad, *opts) -> SelfWildcardLoad:
        raise NotImplementedError("Star option does not support sub-options")

    def _apply_to_parent(self, parent):
        """apply this :class:`_orm._WildcardLoad` object as a sub-option of
        a :class:`_orm.Load` object.

        This method is used by the :meth:`_orm.Load.options` method.   Note
        that :class:`_orm.WildcardLoad` itself can't have sub-options, but
        it may be used as the sub-option of a :class:`_orm.Load` object.

        """

        attr = self.path[0]
        if attr.endswith(_DEFAULT_TOKEN):
            attr = f"{attr.split(':')[0]}:{_WILDCARD_TOKEN}"

        effective_path = parent.path.token(attr)

        assert effective_path.is_token

        loader = _TokenStrategyLoad.create(
            effective_path,
            None,
            self.strategy,
            None,
            self.local_opts,
            self.propagate_to_loaders,
        )

        parent.context += (loader,)

    def _process(self, compile_state, mapper_entities, raiseerr):
        is_refresh = compile_state.compile_options._for_refresh_state

        if is_refresh and not self.propagate_to_loaders:
            return

        entities = [ent.entity_zero for ent in mapper_entities]
        current_path = compile_state.current_path

        start_path = self.path

        # TODO: chop_path already occurs in loader.process_compile_state()
        # so we will seek to simplify this
        if current_path:
            start_path = self._chop_path(start_path, current_path)
            if not start_path:
                return

        # start_path is a single-token tuple
        assert start_path and len(start_path) == 1

        token = start_path[0]

        entity = self._find_entity_basestring(entities, token, raiseerr)

        if not entity:
            return

        path_element = entity

        # transfer our entity-less state into a Load() object
        # with a real entity path.  Start with the lead entity
        # we just located, then go through the rest of our path
        # tokens and populate into the Load().

        loader = _TokenStrategyLoad.create(
            path_element._path_registry,
            token,
            self.strategy,
            None,
            self.local_opts,
            self.propagate_to_loaders,
            raiseerr=raiseerr,
        )
        if not loader:
            return

        assert loader.path.is_token

        # don't pass a reconciled lead entity here
        loader.process_compile_state(
            self, compile_state, mapper_entities, None, raiseerr
        )

        return loader

    def _find_entity_basestring(self, entities, token, raiseerr):
        if token.endswith(f":{_WILDCARD_TOKEN}"):
            if len(list(entities)) != 1:
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Can't apply wildcard ('*') or load_only() "
                        f"loader option to multiple entities "
                        f"{', '.join(str(ent) for ent in entities)}. Specify "
                        "loader options for each entity individually, such as "
                        f"""{
                            ", ".join(
                                f"Load({ent}).some_option('*')"
                                for ent in entities
                            )
                        }."""
                    )
        elif token.endswith(_DEFAULT_TOKEN):
            raiseerr = False

        for ent in entities:
            # return only the first _MapperEntity when searching
            # based on string prop name.   Ideally object
            # attributes are used to specify more exactly.
            return ent
        else:
            if raiseerr:
                raise sa_exc.ArgumentError(
                    "Query has only expression-based entities - "
                    f'can\'t find property named "{token}".'
                )
            else:
                return None

    def __getstate__(self):
        d = self._shallow_to_dict()
        return d

    def __setstate__(self, state):
        self._shallow_from_dict(state)


class _LoadElement(
    cache_key.HasCacheKey, traversals.HasShallowCopy, visitors.Traversible
):
    """represents strategy information to select for a LoaderStrategy
    and pass options to it.

    :class:`._LoadElement` objects provide the inner datastructure
    stored by a :class:`_orm.Load` object and are also the object passed
    to methods like :meth:`.LoaderStrategy.setup_query`.

    .. versionadded:: 2.0

    """

    __slots__ = (
        "path",
        "strategy",
        "propagate_to_loaders",
        "local_opts",
        "_extra_criteria",
        "_reconcile_to_other",
    )
    __visit_name__ = "load_element"

    _traverse_internals = [
        ("path", visitors.ExtendedInternalTraversal.dp_has_cache_key),
        ("strategy", visitors.ExtendedInternalTraversal.dp_plain_obj),
        (
            "local_opts",
            visitors.ExtendedInternalTraversal.dp_string_multi_dict,
        ),
        ("_extra_criteria", visitors.InternalTraversal.dp_clauseelement_list),
        ("propagate_to_loaders", visitors.InternalTraversal.dp_plain_obj),
        ("_reconcile_to_other", visitors.InternalTraversal.dp_plain_obj),
    ]
    _cache_key_traversal = None

    _extra_criteria: Tuple[Any, ...]

    _reconcile_to_other: Optional[bool]
    strategy: Tuple[Any, ...]
    path: PathRegistry
    propagate_to_loaders: bool

    local_opts: Mapping[str, Any]

    is_token_strategy: bool
    is_class_strategy: bool

    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return traversals.compare(self, other)

    @property
    def is_opts_only(self):
        return bool(self.local_opts and self.strategy is None)

    def _clone(self):
        cls = self.__class__
        s = cls.__new__(cls)

        self._shallow_copy_to(s)
        return s

    def __getstate__(self):
        d = self._shallow_to_dict()
        d["path"] = self.path.serialize()
        return d

    def __setstate__(self, state):
        state["path"] = PathRegistry.deserialize(state["path"])
        self._shallow_from_dict(state)

    def _raise_for_no_match(self, parent_loader, mapper_entities):
        path = parent_loader.path

        found_entities = False
        for ent in mapper_entities:
            ezero = ent.entity_zero
            if ezero:
                found_entities = True
                break

        if not found_entities:
            raise sa_exc.ArgumentError(
                "Query has only expression-based entities; "
                f"attribute loader options for {path[0]} can't "
                "be applied here."
            )
        else:
            raise sa_exc.ArgumentError(
                f"Mapped class {path[0]} does not apply to any of the "
                f"root entities in this query, e.g. "
                f"""{
                    ", ".join(str(x.entity_zero)
                    for x in mapper_entities if x.entity_zero
                )}. Please """
                "specify the full path "
                "from one of the root entities to the target "
                "attribute. "
            )

    def _adjust_effective_path_for_current_path(
        self, effective_path, current_path
    ):
        """receives the 'current_path' entry from an :class:`.ORMCompileState`
        instance, which is set during lazy loads and secondary loader strategy
        loads, and adjusts the given path to be relative to the
        current_path.

        E.g. given a loader path and current path::

            lp: User -> orders -> Order -> items -> Item -> keywords -> Keyword

            cp: User -> orders -> Order -> items

        The adjusted path would be::

            Item -> keywords -> Keyword


        """
        chopped_start_path = Load._chop_path(effective_path, current_path)
        if not chopped_start_path:
            return None

        tokens_removed_from_start_path = len(effective_path) - len(
            chopped_start_path
        )

        loader_lead_path_element = self.path[tokens_removed_from_start_path]

        effective_path = PathRegistry.coerce(
            (loader_lead_path_element,) + chopped_start_path[1:]
        )

        return effective_path

    def _init_path(self, path, attr, wildcard_key, attr_group, raiseerr):
        """Apply ORM attributes and/or wildcard to an existing path, producing
        a new path.

        This method is used within the :meth:`.create` method to initialize
        a :class:`._LoadElement` object.

        """
        raise NotImplementedError()

    def _prepare_for_compile_state(
        self,
        parent_loader,
        compile_state,
        mapper_entities,
        reconciled_lead_entity,
        raiseerr,
    ):
        """implemented by subclasses."""
        raise NotImplementedError()

    def process_compile_state(
        self,
        parent_loader,
        compile_state,
        mapper_entities,
        reconciled_lead_entity,
        raiseerr,
    ):
        """populate ORMCompileState.attributes with loader state for this
        _LoadElement.

        """
        keys = self._prepare_for_compile_state(
            parent_loader,
            compile_state,
            mapper_entities,
            reconciled_lead_entity,
            raiseerr,
        )
        for key in keys:
            if key in compile_state.attributes:
                compile_state.attributes[key] = _LoadElement._reconcile(
                    self, compile_state.attributes[key]
                )
            else:
                compile_state.attributes[key] = self

    @classmethod
    def create(
        cls,
        path,
        attr,
        strategy,
        wildcard_key,
        local_opts,
        propagate_to_loaders,
        raiseerr=True,
        attr_group=None,
        reconcile_to_other=None,
    ):
        """Create a new :class:`._LoadElement` object."""

        opt = cls.__new__(cls)
        opt.path = path
        opt.strategy = strategy
        opt.propagate_to_loaders = propagate_to_loaders
        opt.local_opts = (
            util.immutabledict(local_opts) if local_opts else util.EMPTY_DICT
        )
        opt._extra_criteria = ()

        if reconcile_to_other is not None:
            opt._reconcile_to_other = reconcile_to_other
        elif strategy is None and not local_opts:
            opt._reconcile_to_other = True
        else:
            opt._reconcile_to_other = None

        path = opt._init_path(path, attr, wildcard_key, attr_group, raiseerr)

        if not path:
            return None

        assert opt.is_token_strategy == path.is_token

        opt.path = path
        return opt

    def __init__(self, path, strategy, local_opts, propagate_to_loaders):
        raise NotImplementedError()

    def _prepend_path_from(self, parent):
        """adjust the path of this :class:`._LoadElement` to be
        a subpath of that of the given parent :class:`_orm.Load` object's
        path.

        This is used by the :meth:`_orm.Load._apply_to_parent` method,
        which is in turn part of the :meth:`_orm.Load.options` method.

        """
        cloned = self._clone()

        assert cloned.strategy == self.strategy
        assert cloned.local_opts == self.local_opts
        assert cloned.is_class_strategy == self.is_class_strategy

        if not orm_util._entity_corresponds_to_use_path_impl(
            parent.path[-1], cloned.path[0]
        ):
            raise sa_exc.ArgumentError(
                f'Attribute "{cloned.path[1]}" does not link '
                f'from element "{parent.path[-1]}".'
            )

        cloned.path = PathRegistry.coerce(parent.path[0:-1] + cloned.path[:])

        return cloned

    @staticmethod
    def _reconcile(replacement, existing):
        """define behavior for when two Load objects are to be put into
        the context.attributes under the same key.

        :param replacement: ``_LoadElement`` that seeks to replace the
         existing one

        :param existing: ``_LoadElement`` that is already present.

        """
        # mapper inheritance loading requires fine-grained "block other
        # options" / "allow these options to be overridden" behaviors
        # see test_poly_loading.py

        if replacement._reconcile_to_other:
            return existing
        elif replacement._reconcile_to_other is False:
            return replacement
        elif existing._reconcile_to_other:
            return replacement
        elif existing._reconcile_to_other is False:
            return existing

        if existing is replacement:
            return replacement
        elif (
            existing.strategy == replacement.strategy
            and existing.local_opts == replacement.local_opts
        ):
            return replacement
        elif replacement.is_opts_only:
            existing = existing._clone()
            existing.local_opts = existing.local_opts.union(
                replacement.local_opts
            )
            existing._extra_criteria += replacement._extra_criteria
            return existing
        elif existing.is_opts_only:
            replacement = replacement._clone()
            replacement.local_opts = replacement.local_opts.union(
                existing.local_opts
            )
            replacement._extra_criteria += replacement._extra_criteria
            return replacement
        elif replacement.path.is_token:
            # use 'last one wins' logic for wildcard options.  this is also
            # kind of inconsistent vs. options that are specific paths which
            # will raise as below
            return replacement

        raise sa_exc.InvalidRequestError(
            f"Loader strategies for {replacement.path} conflict"
        )


class _AttributeStrategyLoad(_LoadElement):
    """Loader strategies against specific relationship or column paths.

    e.g.::

        joinedload(User.addresses)
        defer(Order.name)
        selectinload(User.orders).lazyload(Order.items)

    """

    __slots__ = ("_of_type", "_path_with_polymorphic_path")

    __visit_name__ = "attribute_strategy_load_element"

    _traverse_internals = _LoadElement._traverse_internals + [
        ("_of_type", visitors.ExtendedInternalTraversal.dp_multi),
        (
            "_path_with_polymorphic_path",
            visitors.ExtendedInternalTraversal.dp_has_cache_key,
        ),
    ]

    _of_type: Union["Mapper", AliasedInsp, None]
    _path_with_polymorphic_path: Optional[PathRegistry]

    is_class_strategy = False
    is_token_strategy = False

    def _init_path(self, path, attr, wildcard_key, attr_group, raiseerr):
        assert attr is not None
        self._of_type = None
        self._path_with_polymorphic_path = None
        insp, _, prop = _parse_attr_argument(attr)

        if insp.is_property:
            # direct property can be sent from internal strategy logic
            # that sets up specific loaders, such as
            # emit_lazyload->_lazyload_reverse
            # prop = found_property = attr
            prop = attr
            path = path[prop]

            if path.has_entity:
                path = path.entity_path
            return path

        elif not insp.is_attribute:
            # should not reach here;
            assert False

        # here we assume we have user-passed InstrumentedAttribute
        if not orm_util._entity_corresponds_to_use_path_impl(
            path[-1], attr.parent
        ):
            if raiseerr:
                if attr_group and attr is not attr_group[0]:
                    raise sa_exc.ArgumentError(
                        "Can't apply wildcard ('*') or load_only() "
                        "loader option to multiple entities in the "
                        "same option. Use separate options per entity."
                    )
                elif len(path) > 1:
                    path_is_of_type = (
                        path[-1].entity is not path[-2].mapper.class_
                    )
                    raise sa_exc.ArgumentError(
                        f'ORM mapped attribute "{attr}" does not '
                        f'link from relationship "{path[-2]}%s".%s'
                        % (
                            f".of_type({path[-1]})" if path_is_of_type else "",
                            (
                                "  Did you mean to use "
                                f'"{path[-2]}'
                                f'.of_type({attr.class_.__name__})"?'
                                if not path_is_of_type
                                and not path[-1].is_aliased_class
                                and orm_util._entity_corresponds_to(
                                    path.entity, attr.parent.mapper
                                )
                                else ""
                            ),
                        )
                    )
                else:
                    raise sa_exc.ArgumentError(
                        f'ORM mapped attribute "{attr}" does not '
                        f'link mapped class "{path[-1]}"'
                    )
            else:
                return None

        # note the essential logic of this attribute was very different in
        # 1.4, where there were caching failures in e.g.
        # test_relationship_criteria.py::RelationshipCriteriaTest::
        # test_selectinload_nested_criteria[True] if an existing
        # "_extra_criteria" on a Load object were replaced with that coming
        # from an attribute.   This appears to have been an artifact of how
        # _UnboundLoad / Load interacted together, which was opaque and
        # poorly defined.
        self._extra_criteria = attr._extra_criteria

        if getattr(attr, "_of_type", None):
            ac = attr._of_type
            ext_info = inspect(ac)
            self._of_type = ext_info

            self._path_with_polymorphic_path = path.entity_path[prop]

            path = path[prop][ext_info]

        else:
            path = path[prop]

        if path.has_entity:
            path = path.entity_path

        return path

    def _generate_extra_criteria(self, context):
        """Apply the current bound parameters in a QueryContext to the
        immediate "extra_criteria" stored with this Load object.

        Load objects are typically pulled from the cached version of
        the statement from a QueryContext.  The statement currently being
        executed will have new values (and keys) for bound parameters in the
        extra criteria which need to be applied by loader strategies when
        they handle this criteria for a result set.

        """

        assert (
            self._extra_criteria
        ), "this should only be called if _extra_criteria is present"

        orig_query = context.compile_state.select_statement
        current_query = context.query

        # NOTE: while it seems like we should not do the "apply" operation
        # here if orig_query is current_query, skipping it in the "optimized"
        # case causes the query to be different from a cache key perspective,
        # because we are creating a copy of the criteria which is no longer
        # the same identity of the _extra_criteria in the loader option
        # itself.  cache key logic produces a different key for
        # (A, copy_of_A) vs. (A, A), because in the latter case it shortens
        # the second part of the key to just indicate on identity.

        # if orig_query is current_query:
        # not cached yet.   just do the and_()
        #    return and_(*self._extra_criteria)

        k1 = orig_query._generate_cache_key()
        k2 = current_query._generate_cache_key()

        return k2._apply_params_to_element(k1, and_(*self._extra_criteria))

    def _set_of_type_info(self, context, current_path):
        assert self._path_with_polymorphic_path

        pwpi = self._of_type
        assert pwpi
        if not pwpi.is_aliased_class:
            pwpi = inspect(
                orm_util.with_polymorphic(
                    pwpi.mapper.base_mapper,
                    pwpi.mapper,
                    aliased=True,
                    _use_mapper_path=True,
                )
            )
        start_path = self._path_with_polymorphic_path
        if current_path:

            start_path = self._adjust_effective_path_for_current_path(
                start_path, current_path
            )
            if start_path is None:
                return

        key = ("path_with_polymorphic", start_path.natural_path)
        if key in context:
            existing_aliased_insp = context[key]
            this_aliased_insp = pwpi
            new_aliased_insp = existing_aliased_insp._merge_with(
                this_aliased_insp
            )
            context[key] = new_aliased_insp
        else:
            context[key] = pwpi

    def _prepare_for_compile_state(
        self,
        parent_loader,
        compile_state,
        mapper_entities,
        reconciled_lead_entity,
        raiseerr,
    ):
        # _AttributeStrategyLoad

        current_path = compile_state.current_path
        is_refresh = compile_state.compile_options._for_refresh_state
        assert not self.path.is_token

        if is_refresh and not self.propagate_to_loaders:
            return []

        if self._of_type:
            # apply additional with_polymorphic alias that may have been
            # generated.  this has to happen even if this is a defaultload
            self._set_of_type_info(compile_state.attributes, current_path)

        # omit setting loader attributes for a "defaultload" type of option
        if not self.strategy and not self.local_opts:
            return []

        if raiseerr and not reconciled_lead_entity:
            self._raise_for_no_match(parent_loader, mapper_entities)

        if self.path.has_entity:
            effective_path = self.path.parent
        else:
            effective_path = self.path

        if current_path:
            effective_path = self._adjust_effective_path_for_current_path(
                effective_path, current_path
            )
            if effective_path is None:
                return []

        return [("loader", cast(PathRegistry, effective_path).natural_path)]

    def __getstate__(self):
        d = super().__getstate__()

        # can't pickle this.  See
        # test_pickled.py -> test_lazyload_extra_criteria_not_supported
        # where we should be emitting a warning for the usual case where this
        # would be non-None
        d["_extra_criteria"] = ()

        if self._path_with_polymorphic_path:
            d[
                "_path_with_polymorphic_path"
            ] = self._path_with_polymorphic_path.serialize()

        if self._of_type:
            if self._of_type.is_aliased_class:
                d["_of_type"] = None
            elif self._of_type.is_mapper:
                d["_of_type"] = self._of_type.class_
            else:
                assert False, "unexpected object for _of_type"

        return d

    def __setstate__(self, state):
        super().__setstate__(state)

        if state.get("_path_with_polymorphic_path", None):
            self._path_with_polymorphic_path = PathRegistry.deserialize(
                state["_path_with_polymorphic_path"]
            )
        else:
            self._path_with_polymorphic_path = None

        if state.get("_of_type", None):
            self._of_type = inspect(state["_of_type"])
        else:
            self._of_type = None


class _TokenStrategyLoad(_LoadElement):
    """Loader strategies against wildcard attributes

    e.g.::

        raiseload('*')
        Load(User).lazyload('*')
        defer('*')
        load_only(User.name, User.email)  # will create a defer('*')
        joinedload(User.addresses).raiseload('*')

    """

    __visit_name__ = "token_strategy_load_element"

    inherit_cache = True
    is_class_strategy = False
    is_token_strategy = True

    def _init_path(self, path, attr, wildcard_key, attr_group, raiseerr):
        # assert isinstance(attr, str) or attr is None
        if attr is not None:
            default_token = attr.endswith(_DEFAULT_TOKEN)
            if attr.endswith(_WILDCARD_TOKEN) or default_token:
                if wildcard_key:
                    attr = f"{wildcard_key}:{attr}"

                path = path.token(attr)
                return path
            else:
                raise sa_exc.ArgumentError(
                    "Strings are not accepted for attribute names in loader "
                    "options; please use class-bound attributes directly."
                )
        return path

    def _prepare_for_compile_state(
        self,
        parent_loader,
        compile_state,
        mapper_entities,
        reconciled_lead_entity,
        raiseerr,
    ):
        # _TokenStrategyLoad

        current_path = compile_state.current_path
        is_refresh = compile_state.compile_options._for_refresh_state

        assert self.path.is_token

        if is_refresh and not self.propagate_to_loaders:
            return []

        # omit setting attributes for a "defaultload" type of option
        if not self.strategy and not self.local_opts:
            return []

        effective_path = self.path
        if reconciled_lead_entity:
            effective_path = PathRegistry.coerce(
                (reconciled_lead_entity,) + effective_path.path[1:]
            )

        if current_path:
            effective_path = self._adjust_effective_path_for_current_path(
                effective_path, current_path
            )
            if effective_path is None:
                return []

        # for a wildcard token, expand out the path we set
        # to encompass everything from the query entity on
        # forward.  not clear if this is necessary when current_path
        # is set.

        return [
            ("loader", _path.natural_path)
            for _path in cast(
                TokenRegistry, effective_path
            ).generate_for_superclasses()
        ]


class _ClassStrategyLoad(_LoadElement):
    """Loader strategies that deals with a class as a target, not
    an attribute path

    e.g.::

        q = s.query(Person).options(
            selectin_polymorphic(Person, [Engineer, Manager])
        )

    """

    inherit_cache = True
    is_class_strategy = True
    is_token_strategy = False

    __visit_name__ = "class_strategy_load_element"

    def _init_path(self, path, attr, wildcard_key, attr_group, raiseerr):
        return path

    def _prepare_for_compile_state(
        self,
        parent_loader,
        compile_state,
        mapper_entities,
        reconciled_lead_entity,
        raiseerr,
    ):
        # _ClassStrategyLoad

        current_path = compile_state.current_path
        is_refresh = compile_state.compile_options._for_refresh_state

        if is_refresh and not self.propagate_to_loaders:
            return []

        # omit setting attributes for a "defaultload" type of option
        if not self.strategy and not self.local_opts:
            return []

        effective_path = self.path

        if current_path:
            effective_path = self._adjust_effective_path_for_current_path(
                effective_path, current_path
            )
            if effective_path is None:
                return []

        return [("loader", cast(PathRegistry, effective_path).natural_path)]


def _generate_from_keys(meth, keys, chained, kw) -> _AbstractLoad:

    lead_element = None

    for is_default, _keys in (True, keys[0:-1]), (False, keys[-1:]):
        for attr in _keys:
            if isinstance(attr, str):
                if attr.startswith("." + _WILDCARD_TOKEN):
                    util.warn_deprecated(
                        "The undocumented `.{WILDCARD}` format is "
                        "deprecated "
                        "and will be removed in a future version as "
                        "it is "
                        "believed to be unused. "
                        "If you have been using this functionality, "
                        "please "
                        "comment on Issue #4390 on the SQLAlchemy project "
                        "tracker.",
                        version="1.4",
                    )
                    attr = attr[1:]

                if attr == _WILDCARD_TOKEN:
                    if is_default:
                        raise sa_exc.ArgumentError(
                            "Wildcard token cannot be followed by "
                            "another entity",
                        )

                    if lead_element is None:
                        lead_element = _WildcardLoad()

                    lead_element = meth(lead_element, _DEFAULT_TOKEN, **kw)

                else:
                    raise sa_exc.ArgumentError(
                        "Strings are not accepted for attribute names in "
                        "loader options; please use class-bound "
                        "attributes directly.",
                    )
            else:
                if lead_element is None:
                    _, lead_entity, _ = _parse_attr_argument(attr)
                    lead_element = Load(lead_entity)

                if is_default:
                    if not chained:
                        lead_element = lead_element.defaultload(attr)
                    else:
                        lead_element = meth(
                            lead_element, attr, _is_chain=True, **kw
                        )
                else:
                    lead_element = meth(lead_element, attr, **kw)

    assert lead_element
    return lead_element


def _parse_attr_argument(attr):
    """parse an attribute or wildcard argument to produce an
    :class:`._AbstractLoad` instance.

    This is used by the standalone loader strategy functions like
    ``joinedload()``, ``defer()``, etc. to produce :class:`_orm.Load` or
    :class:`._WildcardLoad` objects.

    """
    try:
        insp = inspect(attr)
    except sa_exc.NoInspectionAvailable as err:
        raise sa_exc.ArgumentError(
            "expected ORM mapped attribute for loader strategy argument"
        ) from err

    if insp.is_property:
        lead_entity = insp.parent
        prop = insp
    elif insp.is_attribute:
        lead_entity = insp.parent
        prop = insp.prop
    else:
        raise sa_exc.ArgumentError(
            "expected ORM mapped attribute for loader strategy argument"
        )

    return insp, lead_entity, prop


def loader_unbound_fn(fn):
    """decorator that applies docstrings between standalone loader functions
    and the loader methods on :class:`._AbstractLoad`.

    """
    bound_fn = getattr(_AbstractLoad, fn.__name__)
    fn_doc = bound_fn.__doc__
    bound_fn.__doc__ = f"""Produce a new :class:`_orm.Load` object with the
:func:`_orm.{fn.__name__}` option applied.

See :func:`_orm.{fn.__name__}` for usage examples.

"""

    fn.__doc__ = fn_doc
    return fn


# standalone functions follow.  docstrings are filled in
# by the ``@loader_unbound_fn`` decorator.


@loader_unbound_fn
def contains_eager(*keys, **kw) -> _AbstractLoad:
    return _generate_from_keys(Load.contains_eager, keys, True, kw)


@loader_unbound_fn
def load_only(*attrs) -> _AbstractLoad:
    # TODO: attrs against different classes.  we likely have to
    # add some extra state to Load of some kind
    _, lead_element, _ = _parse_attr_argument(attrs[0])
    return Load(lead_element).load_only(*attrs)


@loader_unbound_fn
def joinedload(*keys, **kw) -> _AbstractLoad:
    return _generate_from_keys(Load.joinedload, keys, False, kw)


@loader_unbound_fn
def subqueryload(*keys) -> _AbstractLoad:
    return _generate_from_keys(Load.subqueryload, keys, False, {})


@loader_unbound_fn
def selectinload(*keys) -> _AbstractLoad:
    return _generate_from_keys(Load.selectinload, keys, False, {})


@loader_unbound_fn
def lazyload(*keys) -> _AbstractLoad:
    return _generate_from_keys(Load.lazyload, keys, False, {})


@loader_unbound_fn
def immediateload(*keys) -> _AbstractLoad:
    return _generate_from_keys(Load.immediateload, keys, False, {})


@loader_unbound_fn
def noload(*keys) -> _AbstractLoad:
    return _generate_from_keys(Load.noload, keys, False, {})


@loader_unbound_fn
def raiseload(*keys, **kw) -> _AbstractLoad:
    return _generate_from_keys(Load.raiseload, keys, False, kw)


@loader_unbound_fn
def defaultload(*keys) -> _AbstractLoad:
    return _generate_from_keys(Load.defaultload, keys, False, {})


@loader_unbound_fn
def defer(key, *addl_attrs, **kw) -> _AbstractLoad:
    if addl_attrs:
        util.warn_deprecated(
            "The *addl_attrs on orm.defer is deprecated.  Please use "
            "method chaining in conjunction with defaultload() to "
            "indicate a path.",
            version="1.3",
        )
    return _generate_from_keys(Load.defer, (key,) + addl_attrs, False, kw)


@loader_unbound_fn
def undefer(key, *addl_attrs) -> _AbstractLoad:
    if addl_attrs:
        util.warn_deprecated(
            "The *addl_attrs on orm.undefer is deprecated.  Please use "
            "method chaining in conjunction with defaultload() to "
            "indicate a path.",
            version="1.3",
        )
    return _generate_from_keys(Load.undefer, (key,) + addl_attrs, False, {})


@loader_unbound_fn
def undefer_group(name) -> _AbstractLoad:
    element = _WildcardLoad()
    return element.undefer_group(name)


@loader_unbound_fn
def with_expression(key, expression) -> _AbstractLoad:
    return _generate_from_keys(
        Load.with_expression, (key,), False, {"expression": expression}
    )


@loader_unbound_fn
def selectin_polymorphic(base_cls, classes) -> _AbstractLoad:
    ul = Load(base_cls)
    return ul.selectin_polymorphic(classes)
