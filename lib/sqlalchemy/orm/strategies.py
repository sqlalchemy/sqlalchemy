# orm/strategies.py
# Copyright (C) 2005-2016 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""sqlalchemy.orm.interfaces.LoaderStrategy
   implementations, and related MapperOptions."""

from .. import exc as sa_exc, inspect
from .. import util, log, event
from ..sql import util as sql_util, visitors
from .. import sql
from . import (
    attributes, interfaces, exc as orm_exc, loading,
    unitofwork, util as orm_util
)
from .state import InstanceState
from .util import _none_set
from . import properties
from .interfaces import (
    LoaderStrategy, StrategizedProperty
)
from .base import _SET_DEFERRED_EXPIRED, _DEFER_FOR_STATE
from .session import _state_session
import itertools


def _register_attribute(
    strategy, mapper, useobject,
    compare_function=None,
    typecallable=None,
    uselist=False,
    callable_=None,
    proxy_property=None,
    active_history=False,
    impl_class=None,
    **kw
):

    prop = strategy.parent_property

    attribute_ext = list(util.to_list(prop.extension, default=[]))

    listen_hooks = []

    if useobject and prop.single_parent:
        listen_hooks.append(single_parent_validator)

    if prop.key in prop.parent.validators:
        fn, opts = prop.parent.validators[prop.key]
        listen_hooks.append(
            lambda desc, prop: orm_util._validator_events(
                desc,
                prop.key, fn, **opts)
        )

    if useobject:
        listen_hooks.append(unitofwork.track_cascade_events)

    # need to assemble backref listeners
    # after the singleparentvalidator, mapper validator
    backref = kw.pop('backref', None)
    if backref:
        listen_hooks.append(
            lambda desc, prop: attributes.backref_listeners(
                desc,
                backref,
                uselist
            )
        )

    # a single MapperProperty is shared down a class inheritance
    # hierarchy, so we set up attribute instrumentation and backref event
    # for each mapper down the hierarchy.

    # typically, "mapper" is the same as prop.parent, due to the way
    # the configure_mappers() process runs, however this is not strongly
    # enforced, and in the case of a second configure_mappers() run the
    # mapper here might not be prop.parent; also, a subclass mapper may
    # be called here before a superclass mapper.  That is, can't depend
    # on mappers not already being set up so we have to check each one.

    for m in mapper.self_and_descendants:
        if prop is m._props.get(prop.key) and \
                not m.class_manager._attr_has_impl(prop.key):

            desc = attributes.register_attribute_impl(
                m.class_,
                prop.key,
                parent_token=prop,
                uselist=uselist,
                compare_function=compare_function,
                useobject=useobject,
                extension=attribute_ext,
                trackparent=useobject and (
                    prop.single_parent or
                    prop.direction is interfaces.ONETOMANY),
                typecallable=typecallable,
                callable_=callable_,
                active_history=active_history,
                impl_class=impl_class,
                send_modified_events=not useobject or not prop.viewonly,
                doc=prop.doc,
                **kw
            )

            for hook in listen_hooks:
                hook(desc, prop)


@properties.ColumnProperty.strategy_for(instrument=False, deferred=False)
class UninstrumentedColumnLoader(LoaderStrategy):
    """Represent a non-instrumented MapperProperty.

    The polymorphic_on argument of mapper() often results in this,
    if the argument is against the with_polymorphic selectable.

    """
    __slots__ = 'columns',

    def __init__(self, parent, strategy_key):
        super(UninstrumentedColumnLoader, self).__init__(parent, strategy_key)
        self.columns = self.parent_property.columns

    def setup_query(
            self, context, entity, path, loadopt, adapter,
            column_collection=None, **kwargs):
        for c in self.columns:
            if adapter:
                c = adapter.columns[c]
            column_collection.append(c)

    def create_row_processor(
            self, context, path, loadopt,
            mapper, result, adapter, populators):
        pass


@log.class_logger
@properties.ColumnProperty.strategy_for(instrument=True, deferred=False)
class ColumnLoader(LoaderStrategy):
    """Provide loading behavior for a :class:`.ColumnProperty`."""

    __slots__ = 'columns', 'is_composite'

    def __init__(self, parent, strategy_key):
        super(ColumnLoader, self).__init__(parent, strategy_key)
        self.columns = self.parent_property.columns
        self.is_composite = hasattr(self.parent_property, 'composite_class')

    def setup_query(
            self, context, entity, path, loadopt,
            adapter, column_collection, memoized_populators, **kwargs):

        for c in self.columns:
            if adapter:
                c = adapter.columns[c]
            column_collection.append(c)

        fetch = self.columns[0]
        if adapter:
            fetch = adapter.columns[fetch]
        memoized_populators[self.parent_property] = fetch

    def init_class_attribute(self, mapper):
        self.is_class_level = True
        coltype = self.columns[0].type
        # TODO: check all columns ?  check for foreign key as well?
        active_history = self.parent_property.active_history or \
            self.columns[0].primary_key or \
            mapper.version_id_col in set(self.columns)

        _register_attribute(
            self, mapper, useobject=False,
            compare_function=coltype.compare_values,
            active_history=active_history
        )

    def create_row_processor(
            self, context, path,
            loadopt, mapper, result, adapter, populators):
        # look through list of columns represented here
        # to see which, if any, is present in the row.
        for col in self.columns:
            if adapter:
                col = adapter.columns[col]
            getter = result._getter(col, False)
            if getter:
                populators["quick"].append((self.key, getter))
                break
        else:
            populators["expire"].append((self.key, True))


@log.class_logger
@properties.ColumnProperty.strategy_for(deferred=True, instrument=True)
class DeferredColumnLoader(LoaderStrategy):
    """Provide loading behavior for a deferred :class:`.ColumnProperty`."""

    __slots__ = 'columns', 'group'

    def __init__(self, parent, strategy_key):
        super(DeferredColumnLoader, self).__init__(parent, strategy_key)
        if hasattr(self.parent_property, 'composite_class'):
            raise NotImplementedError("Deferred loading for composite "
                                      "types not implemented yet")
        self.columns = self.parent_property.columns
        self.group = self.parent_property.group

    def create_row_processor(
            self, context, path, loadopt,
            mapper, result, adapter, populators):

        # this path currently does not check the result
        # for the column; this is because in most cases we are
        # working just with the setup_query() directive which does
        # not support this, and the behavior here should be consistent.
        if not self.is_class_level:
            set_deferred_for_local_state = \
                self.parent_property._deferred_column_loader
            populators["new"].append((self.key, set_deferred_for_local_state))
        else:
            populators["expire"].append((self.key, False))

    def init_class_attribute(self, mapper):
        self.is_class_level = True

        _register_attribute(
            self, mapper, useobject=False,
            compare_function=self.columns[0].type.compare_values,
            callable_=self._load_for_state,
            expire_missing=False
        )

    def setup_query(
            self, context, entity, path, loadopt,
            adapter, column_collection, memoized_populators,
            only_load_props=None, **kw):

        if (
            (
                loadopt and
                'undefer_pks' in loadopt.local_opts and
                set(self.columns).intersection(
                    self.parent._should_undefer_in_wildcard)
            )
            or
            (
                loadopt and
                self.group and
                loadopt.local_opts.get('undefer_group_%s' % self.group, False)
            )
            or
            (
                only_load_props and self.key in only_load_props
            )
        ):
            self.parent_property._get_strategy(
                (("deferred", False), ("instrument", True))
            ).setup_query(
                context, entity,
                path, loadopt, adapter,
                column_collection, memoized_populators, **kw)
        elif self.is_class_level:
            memoized_populators[self.parent_property] = _SET_DEFERRED_EXPIRED
        else:
            memoized_populators[self.parent_property] = _DEFER_FOR_STATE

    def _load_for_state(self, state, passive):
        if not state.key:
            return attributes.ATTR_EMPTY

        if not passive & attributes.SQL_OK:
            return attributes.PASSIVE_NO_RESULT

        localparent = state.manager.mapper

        if self.group:
            toload = [
                p.key for p in
                localparent.iterate_properties
                if isinstance(p, StrategizedProperty) and
                isinstance(p.strategy, DeferredColumnLoader) and
                p.group == self.group
            ]
        else:
            toload = [self.key]

        # narrow the keys down to just those which have no history
        group = [k for k in toload if k in state.unmodified]

        session = _state_session(state)
        if session is None:
            raise orm_exc.DetachedInstanceError(
                "Parent instance %s is not bound to a Session; "
                "deferred load operation of attribute '%s' cannot proceed" %
                (orm_util.state_str(state), self.key)
            )

        query = session.query(localparent)
        if loading.load_on_ident(
                query, state.key,
                only_load_props=group, refresh_state=state) is None:
            raise orm_exc.ObjectDeletedError(state)

        return attributes.ATTR_WAS_SET


class LoadDeferredColumns(object):
    """serializable loader object used by DeferredColumnLoader"""

    def __init__(self, key):
        self.key = key

    def __call__(self, state, passive=attributes.PASSIVE_OFF):
        key = self.key

        localparent = state.manager.mapper
        prop = localparent._props[key]
        strategy = prop._strategies[DeferredColumnLoader]
        return strategy._load_for_state(state, passive)


class AbstractRelationshipLoader(LoaderStrategy):
    """LoaderStratgies which deal with related objects."""

    __slots__ = 'mapper', 'target', 'uselist'

    def __init__(self, parent, strategy_key):
        super(AbstractRelationshipLoader, self).__init__(parent, strategy_key)
        self.mapper = self.parent_property.mapper
        self.target = self.parent_property.target
        self.uselist = self.parent_property.uselist


@log.class_logger
@properties.RelationshipProperty.strategy_for(lazy="noload")
@properties.RelationshipProperty.strategy_for(lazy=None)
class NoLoader(AbstractRelationshipLoader):
    """Provide loading behavior for a :class:`.RelationshipProperty`
    with "lazy=None".

    """

    __slots__ = ()

    def init_class_attribute(self, mapper):
        self.is_class_level = True

        _register_attribute(
            self, mapper,
            useobject=True,
            uselist=self.parent_property.uselist,
            typecallable=self.parent_property.collection_class,
        )

    def create_row_processor(
            self, context, path, loadopt, mapper,
            result, adapter, populators):
        def invoke_no_load(state, dict_, row):
            if self.uselist:
                state.manager.get_impl(self.key).initialize(state, dict_)
            else:
                dict_[self.key] = None
        populators["new"].append((self.key, invoke_no_load))


@log.class_logger
@properties.RelationshipProperty.strategy_for(lazy=True)
@properties.RelationshipProperty.strategy_for(lazy="select")
@properties.RelationshipProperty.strategy_for(lazy="raise")
@properties.RelationshipProperty.strategy_for(lazy="raise_on_sql")
class LazyLoader(AbstractRelationshipLoader, util.MemoizedSlots):
    """Provide loading behavior for a :class:`.RelationshipProperty`
    with "lazy=True", that is loads when first accessed.

    """

    __slots__ = (
        '_lazywhere', '_rev_lazywhere', 'use_get', '_bind_to_col',
        '_equated_columns', '_rev_bind_to_col', '_rev_equated_columns',
        '_simple_lazy_clause', '_raise_always', '_raise_on_sql')

    def __init__(self, parent, strategy_key):
        super(LazyLoader, self).__init__(parent, strategy_key)
        self._raise_always = self.strategy_opts["lazy"] == "raise"
        self._raise_on_sql = self.strategy_opts["lazy"] == "raise_on_sql"

        join_condition = self.parent_property._join_condition
        self._lazywhere, \
            self._bind_to_col, \
            self._equated_columns = join_condition.create_lazy_clause()

        self._rev_lazywhere, \
            self._rev_bind_to_col, \
            self._rev_equated_columns = join_condition.create_lazy_clause(
                reverse_direction=True)

        self.logger.info("%s lazy loading clause %s", self, self._lazywhere)

        # determine if our "lazywhere" clause is the same as the mapper's
        # get() clause.  then we can just use mapper.get()
        self.use_get = not self.uselist and \
            self.mapper._get_clause[0].compare(
                self._lazywhere,
                use_proxies=True,
                equivalents=self.mapper._equivalent_columns
            )

        if self.use_get:
            for col in list(self._equated_columns):
                if col in self.mapper._equivalent_columns:
                    for c in self.mapper._equivalent_columns[col]:
                        self._equated_columns[c] = self._equated_columns[col]

            self.logger.info("%s will use query.get() to "
                             "optimize instance loads", self)

    def init_class_attribute(self, mapper):
        self.is_class_level = True

        active_history = (
            self.parent_property.active_history or
            self.parent_property.direction is not interfaces.MANYTOONE or
            not self.use_get
        )

        # MANYTOONE currently only needs the
        # "old" value for delete-orphan
        # cascades.  the required _SingleParentValidator
        # will enable active_history
        # in that case.  otherwise we don't need the
        # "old" value during backref operations.
        _register_attribute(
            self,
            mapper,
            useobject=True,
            callable_=self._load_for_state,
            uselist=self.parent_property.uselist,
            backref=self.parent_property.back_populates,
            typecallable=self.parent_property.collection_class,
            active_history=active_history
        )

    def _memoized_attr__simple_lazy_clause(self):
        criterion, bind_to_col = (
            self._lazywhere,
            self._bind_to_col
        )

        params = []

        def visit_bindparam(bindparam):
            bindparam.unique = False
            if bindparam._identifying_key in bind_to_col:
                params.append((
                    bindparam.key, bind_to_col[bindparam._identifying_key],
                    None))
            elif bindparam.callable is None:
                params.append((bindparam.key, None, bindparam.value))

        criterion = visitors.cloned_traverse(
            criterion, {}, {'bindparam': visit_bindparam}
        )

        return criterion, params

    def _generate_lazy_clause(self, state, passive):
        criterion, param_keys = self._simple_lazy_clause

        if state is None:
            return sql_util.adapt_criterion_to_null(
                criterion, [key for key, ident, value in param_keys])

        mapper = self.parent_property.parent

        o = state.obj()  # strong ref
        dict_ = attributes.instance_dict(o)

        if passive & attributes.INIT_OK:
            passive ^= attributes.INIT_OK

        params = {}
        for key, ident, value in param_keys:
            if ident is not None:
                if passive and passive & attributes.LOAD_AGAINST_COMMITTED:
                    value = mapper._get_committed_state_attr_by_column(
                        state, dict_, ident, passive)
                else:
                    value = mapper._get_state_attr_by_column(
                        state, dict_, ident, passive)

            params[key] = value

        return criterion, params

    def _invoke_raise_load(self, state, passive, lazy):
        raise sa_exc.InvalidRequestError(
            "'%s' is not available due to lazy='%s'" % (self, lazy)
        )

    def _load_for_state(self, state, passive):

        if not state.key and (
                (
                    not self.parent_property.load_on_pending
                    and not state._load_pending
                )
                or not state.session_id
        ):
            return attributes.ATTR_EMPTY

        pending = not state.key
        ident_key = None

        if (
            (not passive & attributes.SQL_OK and not self.use_get)
            or
            (not passive & attributes.NON_PERSISTENT_OK and pending)
        ):
            return attributes.PASSIVE_NO_RESULT

        if self._raise_always:
            self._invoke_raise_load(state, passive, "raise")

        session = _state_session(state)
        if not session:
            raise orm_exc.DetachedInstanceError(
                "Parent instance %s is not bound to a Session; "
                "lazy load operation of attribute '%s' cannot proceed" %
                (orm_util.state_str(state), self.key)
            )

        # if we have a simple primary key load, check the
        # identity map without generating a Query at all
        if self.use_get:
            ident = self._get_ident_for_use_get(
                session,
                state,
                passive
            )
            if attributes.PASSIVE_NO_RESULT in ident:
                return attributes.PASSIVE_NO_RESULT
            elif attributes.NEVER_SET in ident:
                return attributes.NEVER_SET

            if _none_set.issuperset(ident):
                return None

            ident_key = self.mapper.identity_key_from_primary_key(ident)
            instance = loading.get_from_identity(session, ident_key, passive)
            if instance is not None:
                return instance
            elif not passive & attributes.SQL_OK or \
                    not passive & attributes.RELATED_OBJECT_OK:
                return attributes.PASSIVE_NO_RESULT

        return self._emit_lazyload(session, state, ident_key, passive)

    def _get_ident_for_use_get(self, session, state, passive):
        instance_mapper = state.manager.mapper

        if passive & attributes.LOAD_AGAINST_COMMITTED:
            get_attr = instance_mapper._get_committed_state_attr_by_column
        else:
            get_attr = instance_mapper._get_state_attr_by_column

        dict_ = state.dict

        return [
            get_attr(
                state,
                dict_,
                self._equated_columns[pk],
                passive=passive)
            for pk in self.mapper.primary_key
        ]

    @util.dependencies("sqlalchemy.orm.strategy_options")
    def _emit_lazyload(
            self, strategy_options, session, state, ident_key, passive):

        q = session.query(self.mapper)._adapt_all_clauses()
        if self.parent_property.secondary is not None:
            q = q.select_from(self.mapper, self.parent_property.secondary)

        q = q._with_invoke_all_eagers(False)

        pending = not state.key

        # don't autoflush on pending
        if pending or passive & attributes.NO_AUTOFLUSH:
            q = q.autoflush(False)

        if state.load_path:
            q = q._with_current_path(state.load_path[self.parent_property])

        if state.load_options:
            q = q._conditional_options(*state.load_options)

        if self.use_get:
            if self._raise_on_sql:
                self._invoke_raise_load(state, passive, "raise_on_sql")
            return loading.load_on_ident(q, ident_key)

        if self.parent_property.order_by:
            q = q.order_by(*util.to_list(self.parent_property.order_by))

        for rev in self.parent_property._reverse_property:
            # reverse props that are MANYTOONE are loading *this*
            # object from get(), so don't need to eager out to those.
            if rev.direction is interfaces.MANYTOONE and \
                rev._use_get and \
                    not isinstance(rev.strategy, LazyLoader):
                q = q.options(
                    strategy_options.Load.for_existing_path(
                        q._current_path[rev.parent]
                    ).lazyload(rev.key)
                )

        lazy_clause, params = self._generate_lazy_clause(
            state, passive=passive)

        if pending:
            if util.has_intersection(
                    orm_util._none_set, params.values()):
                return None
        elif util.has_intersection(orm_util._never_set, params.values()):
            return None

        if self._raise_on_sql:
            self._invoke_raise_load(state, passive, "raise_on_sql")

        q = q.filter(lazy_clause).params(params)

        result = q.all()
        if self.uselist:
            return result
        else:
            l = len(result)
            if l:
                if l > 1:
                    util.warn(
                        "Multiple rows returned with "
                        "uselist=False for lazily-loaded attribute '%s' "
                        % self.parent_property)

                return result[0]
            else:
                return None

    def create_row_processor(
            self, context, path, loadopt,
            mapper, result, adapter, populators):
        key = self.key
        if not self.is_class_level:
            # we are not the primary manager for this attribute
            # on this class - set up a
            # per-instance lazyloader, which will override the
            # class-level behavior.
            # this currently only happens when using a
            # "lazyload" option on a "no load"
            # attribute - "eager" attributes always have a
            # class-level lazyloader installed.
            set_lazy_callable = InstanceState._instance_level_callable_processor(
                mapper.class_manager,
                LoadLazyAttribute(key, self), key)

            populators["new"].append((self.key, set_lazy_callable))
        elif context.populate_existing or mapper.always_refresh:
            def reset_for_lazy_callable(state, dict_, row):
                # we are the primary manager for this attribute on
                # this class - reset its
                # per-instance attribute state, so that the class-level
                # lazy loader is
                # executed when next referenced on this instance.
                # this is needed in
                # populate_existing() types of scenarios to reset
                # any existing state.
                state._reset(dict_, key)

            populators["new"].append((self.key, reset_for_lazy_callable))


class LoadLazyAttribute(object):
    """serializable loader object used by LazyLoader"""

    def __init__(self, key, initiating_strategy):
        self.key = key
        self.strategy_key = initiating_strategy.strategy_key

    def __call__(self, state, passive=attributes.PASSIVE_OFF):
        key = self.key
        instance_mapper = state.manager.mapper
        prop = instance_mapper._props[key]
        strategy = prop._strategies[self.strategy_key]

        return strategy._load_for_state(state, passive)


@properties.RelationshipProperty.strategy_for(lazy="immediate")
class ImmediateLoader(AbstractRelationshipLoader):
    __slots__ = ()

    def init_class_attribute(self, mapper):
        self.parent_property.\
            _get_strategy((("lazy", "select"),)).\
            init_class_attribute(mapper)

    def setup_query(
            self, context, entity,
            path, loadopt, adapter, column_collection=None,
            parentmapper=None, **kwargs):
        pass

    def create_row_processor(
            self, context, path, loadopt,
            mapper, result, adapter, populators):
        def load_immediate(state, dict_, row):
            state.get_impl(self.key).get(state, dict_)

        populators["delayed"].append((self.key, load_immediate))


@log.class_logger
@properties.RelationshipProperty.strategy_for(lazy="subquery")
class SubqueryLoader(AbstractRelationshipLoader):
    __slots__ = 'join_depth',

    def __init__(self, parent, strategy_key):
        super(SubqueryLoader, self).__init__(parent, strategy_key)
        self.join_depth = self.parent_property.join_depth

    def init_class_attribute(self, mapper):
        self.parent_property.\
            _get_strategy((("lazy", "select"),)).\
            init_class_attribute(mapper)

    def setup_query(
            self, context, entity,
            path, loadopt, adapter,
            column_collection=None,
            parentmapper=None, **kwargs):

        if not context.query._enable_eagerloads:
            return
        elif context.query._yield_per:
            context.query._no_yield_per("subquery")

        path = path[self.parent_property]

        # build up a path indicating the path from the leftmost
        # entity to the thing we're subquery loading.
        with_poly_info = path.get(
            context.attributes,
            "path_with_polymorphic", None)
        if with_poly_info is not None:
            effective_entity = with_poly_info.entity
        else:
            effective_entity = self.mapper

        subq_path = context.attributes.get(
            ('subquery_path', None),
            orm_util.PathRegistry.root)

        subq_path = subq_path + path

        # if not via query option, check for
        # a cycle
        if not path.contains(context.attributes, "loader"):
            if self.join_depth:
                if path.length / 2 > self.join_depth:
                    return
            elif subq_path.contains_mapper(self.mapper):
                return

        leftmost_mapper, leftmost_attr, leftmost_relationship = \
            self._get_leftmost(subq_path)

        orig_query = context.attributes.get(
            ("orig_query", SubqueryLoader),
            context.query)

        # generate a new Query from the original, then
        # produce a subquery from it.
        left_alias = self._generate_from_original_query(
            orig_query, leftmost_mapper,
            leftmost_attr, leftmost_relationship,
            entity.entity_zero
        )

        # generate another Query that will join the
        # left alias to the target relationships.
        # basically doing a longhand
        # "from_self()".  (from_self() itself not quite industrial
        # strength enough for all contingencies...but very close)
        q = orig_query.session.query(effective_entity)
        q._attributes = {
            ("orig_query", SubqueryLoader): orig_query,
            ('subquery_path', None): subq_path
        }

        q = q._set_enable_single_crit(False)
        to_join, local_attr, parent_alias = \
            self._prep_for_joins(left_alias, subq_path)
        q = q.order_by(*local_attr)
        q = q.add_columns(*local_attr)
        q = self._apply_joins(
            q, to_join, left_alias,
            parent_alias, effective_entity)

        q = self._setup_options(q, subq_path, orig_query, effective_entity)
        q = self._setup_outermost_orderby(q)

        # add new query to attributes to be picked up
        # by create_row_processor
        path.set(context.attributes, "subquery", q)

    def _get_leftmost(self, subq_path):
        subq_path = subq_path.path
        subq_mapper = orm_util._class_to_mapper(subq_path[0])

        # determine attributes of the leftmost mapper
        if self.parent.isa(subq_mapper) and \
                self.parent_property is subq_path[1]:
            leftmost_mapper, leftmost_prop = \
                self.parent, self.parent_property
        else:
            leftmost_mapper, leftmost_prop = \
                subq_mapper, \
                subq_path[1]

        leftmost_cols = leftmost_prop.local_columns

        leftmost_attr = [
            getattr(
                subq_path[0].entity,
                leftmost_mapper._columntoproperty[c].key)
            for c in leftmost_cols
        ]

        return leftmost_mapper, leftmost_attr, leftmost_prop

    def _generate_from_original_query(
        self,
        orig_query, leftmost_mapper,
        leftmost_attr, leftmost_relationship, orig_entity
    ):
        # reformat the original query
        # to look only for significant columns
        q = orig_query._clone().correlate(None)

        # set a real "from" if not present, as this is more
        # accurate than just going off of the column expression
        if not q._from_obj and orig_entity.mapper.isa(leftmost_mapper):
            q._set_select_from([orig_entity], False)
        target_cols = q._adapt_col_list(leftmost_attr)

        # select from the identity columns of the outer
        q._set_entities(target_cols)

        distinct_target_key = leftmost_relationship.distinct_target_key

        if distinct_target_key is True:
            q._distinct = True
        elif distinct_target_key is None:
            # if target_cols refer to a non-primary key or only
            # part of a composite primary key, set the q as distinct
            for t in set(c.table for c in target_cols):
                if not set(target_cols).issuperset(t.primary_key):
                    q._distinct = True
                    break

        if q._order_by is False:
            q._order_by = leftmost_mapper.order_by

        # don't need ORDER BY if no limit/offset
        if q._limit is None and q._offset is None:
            q._order_by = None

        # the original query now becomes a subquery
        # which we'll join onto.

        embed_q = q.with_labels().subquery()
        left_alias = orm_util.AliasedClass(
            leftmost_mapper, embed_q,
            use_mapper_path=True)
        return left_alias

    def _prep_for_joins(self, left_alias, subq_path):
        # figure out what's being joined.  a.k.a. the fun part
        to_join = []
        pairs = list(subq_path.pairs())

        for i, (mapper, prop) in enumerate(pairs):
            if i > 0:
                # look at the previous mapper in the chain -
                # if it is as or more specific than this prop's
                # mapper, use that instead.
                # note we have an assumption here that
                # the non-first element is always going to be a mapper,
                # not an AliasedClass

                prev_mapper = pairs[i - 1][1].mapper
                to_append = prev_mapper if prev_mapper.isa(mapper) else mapper
            else:
                to_append = mapper

            to_join.append((to_append, prop.key))

        # determine the immediate parent class we are joining from,
        # which needs to be aliased.

        if len(to_join) < 2:
            # in the case of a one level eager load, this is the
            # leftmost "left_alias".
            parent_alias = left_alias
        else:
            info = inspect(to_join[-1][0])
            if info.is_aliased_class:
                parent_alias = info.entity
            else:
                # alias a plain mapper as we may be
                # joining multiple times
                parent_alias = orm_util.AliasedClass(
                    info.entity,
                    use_mapper_path=True)

        local_cols = self.parent_property.local_columns

        local_attr = [
            getattr(parent_alias, self.parent._columntoproperty[c].key)
            for c in local_cols
        ]
        return to_join, local_attr, parent_alias

    def _apply_joins(
            self, q, to_join, left_alias, parent_alias,
            effective_entity):

        ltj = len(to_join)
        if ltj == 1:
            to_join = [
                getattr(left_alias, to_join[0][1]).of_type(effective_entity)
            ]
        elif ltj == 2:
            to_join = [
                getattr(left_alias, to_join[0][1]).of_type(parent_alias),
                getattr(parent_alias, to_join[-1][1]).of_type(effective_entity)
            ]
        elif ltj > 2:
            middle = [
                (
                    orm_util.AliasedClass(item[0])
                    if not inspect(item[0]).is_aliased_class
                    else item[0].entity,
                    item[1]
                ) for item in to_join[1:-1]
            ]
            inner = []

            while middle:
                item = middle.pop(0)
                attr = getattr(item[0], item[1])
                if middle:
                    attr = attr.of_type(middle[0][0])
                else:
                    attr = attr.of_type(parent_alias)

                inner.append(attr)

            to_join = [
                getattr(left_alias, to_join[0][1]).of_type(inner[0].parent)
            ] + inner + [
                getattr(parent_alias, to_join[-1][1]).of_type(effective_entity)
            ]

        for attr in to_join:
            q = q.join(attr, from_joinpoint=True)
        return q

    def _setup_options(self, q, subq_path, orig_query, effective_entity):
        # propagate loader options etc. to the new query.
        # these will fire relative to subq_path.
        q = q._with_current_path(subq_path)
        q = q._conditional_options(*orig_query._with_options)
        if orig_query._populate_existing:
            q._populate_existing = orig_query._populate_existing

        return q

    def _setup_outermost_orderby(self, q):
        if self.parent_property.order_by:
            # if there's an ORDER BY, alias it the same
            # way joinedloader does, but we have to pull out
            # the "eagerjoin" from the query.
            # this really only picks up the "secondary" table
            # right now.
            eagerjoin = q._from_obj[0]
            eager_order_by = \
                eagerjoin._target_adapter.\
                copy_and_process(
                    util.to_list(
                        self.parent_property.order_by
                    )
                )
            q = q.order_by(*eager_order_by)
        return q

    class _SubqCollections(object):
        """Given a :class:`.Query` used to emit the "subquery load",
        provide a load interface that executes the query at the
        first moment a value is needed.

        """
        _data = None

        def __init__(self, subq):
            self.subq = subq

        def get(self, key, default):
            if self._data is None:
                self._load()
            return self._data.get(key, default)

        def _load(self):
            self._data = dict(
                (k, [vv[0] for vv in v])
                for k, v in itertools.groupby(
                    self.subq,
                    lambda x: x[1:]
                )
            )

        def loader(self, state, dict_, row):
            if self._data is None:
                self._load()

    def create_row_processor(
            self, context, path, loadopt,
            mapper, result, adapter, populators):
        if not self.parent.class_manager[self.key].impl.supports_population:
            raise sa_exc.InvalidRequestError(
                "'%s' does not support object "
                "population - eager loading cannot be applied." %
                self)

        path = path[self.parent_property]

        subq = path.get(context.attributes, 'subquery')

        if subq is None:
            return

        assert subq.session is context.session, (
            "Subquery session doesn't refer to that of "
            "our context.  Are there broken context caching "
            "schemes being used?"
        )

        local_cols = self.parent_property.local_columns

        # cache the loaded collections in the context
        # so that inheriting mappers don't re-load when they
        # call upon create_row_processor again
        collections = path.get(context.attributes, "collections")
        if collections is None:
            collections = self._SubqCollections(subq)
            path.set(context.attributes, 'collections', collections)

        if adapter:
            local_cols = [adapter.columns[c] for c in local_cols]

        if self.uselist:
            self._create_collection_loader(
                context, collections, local_cols, populators)
        else:
            self._create_scalar_loader(
                context, collections, local_cols, populators)

    def _create_collection_loader(
            self, context, collections, local_cols, populators):
        def load_collection_from_subq(state, dict_, row):
            collection = collections.get(
                tuple([row[col] for col in local_cols]),
                ()
            )
            state.get_impl(self.key).\
                set_committed_value(state, dict_, collection)

        populators["new"].append((self.key, load_collection_from_subq))
        if context.invoke_all_eagers:
            populators["eager"].append((self.key, collections.loader))

    def _create_scalar_loader(
            self, context, collections, local_cols, populators):
        def load_scalar_from_subq(state, dict_, row):
            collection = collections.get(
                tuple([row[col] for col in local_cols]),
                (None,)
            )
            if len(collection) > 1:
                util.warn(
                    "Multiple rows returned with "
                    "uselist=False for eagerly-loaded attribute '%s' "
                    % self)

            scalar = collection[0]
            state.get_impl(self.key).\
                set_committed_value(state, dict_, scalar)

        populators["new"].append((self.key, load_scalar_from_subq))
        if context.invoke_all_eagers:
            populators["eager"].append((self.key, collections.loader))


@log.class_logger
@properties.RelationshipProperty.strategy_for(lazy="joined")
@properties.RelationshipProperty.strategy_for(lazy=False)
class JoinedLoader(AbstractRelationshipLoader):
    """Provide loading behavior for a :class:`.RelationshipProperty`
    using joined eager loading.

    """

    __slots__ = 'join_depth',

    def __init__(self, parent, strategy_key):
        super(JoinedLoader, self).__init__(parent, strategy_key)
        self.join_depth = self.parent_property.join_depth

    def init_class_attribute(self, mapper):
        self.parent_property.\
            _get_strategy((("lazy", "select"),)).init_class_attribute(mapper)

    def setup_query(
            self, context, entity, path, loadopt, adapter,
            column_collection=None, parentmapper=None,
            chained_from_outerjoin=False,
            **kwargs):
        """Add a left outer join to the statement that's being constructed."""

        if not context.query._enable_eagerloads:
            return
        elif context.query._yield_per and self.uselist:
            context.query._no_yield_per("joined collection")

        path = path[self.parent_property]

        with_polymorphic = None

        user_defined_adapter = self._init_user_defined_eager_proc(
            loadopt, context) if loadopt else False

        if user_defined_adapter is not False:
            clauses, adapter, add_to_collection = \
                self._setup_query_on_user_defined_adapter(
                    context, entity, path, adapter,
                    user_defined_adapter
                )
        else:
            # if not via query option, check for
            # a cycle
            if not path.contains(context.attributes, "loader"):
                if self.join_depth:
                    if path.length / 2 > self.join_depth:
                        return
                elif path.contains_mapper(self.mapper):
                    return

            clauses, adapter, add_to_collection, chained_from_outerjoin = \
                self._generate_row_adapter(
                    context, entity, path, loadopt, adapter,
                    column_collection, parentmapper, chained_from_outerjoin
                )

        with_poly_info = path.get(
            context.attributes,
            "path_with_polymorphic",
            None
        )
        if with_poly_info is not None:
            with_polymorphic = with_poly_info.with_polymorphic_mappers
        else:
            with_polymorphic = None

        path = path[self.mapper]

        loading._setup_entity_query(
            context, self.mapper, entity,
            path, clauses, add_to_collection,
            with_polymorphic=with_polymorphic,
            parentmapper=self.mapper,
            chained_from_outerjoin=chained_from_outerjoin)

        if with_poly_info is not None and \
                None in set(context.secondary_columns):
            raise sa_exc.InvalidRequestError(
                "Detected unaliased columns when generating joined "
                "load.  Make sure to use aliased=True or flat=True "
                "when using joined loading with with_polymorphic()."
            )

    def _init_user_defined_eager_proc(self, loadopt, context):

        # check if the opt applies at all
        if "eager_from_alias" not in loadopt.local_opts:
            # nope
            return False

        path = loadopt.path.parent

        # the option applies.  check if the "user_defined_eager_row_processor"
        # has been built up.
        adapter = path.get(
            context.attributes,
            "user_defined_eager_row_processor", False)
        if adapter is not False:
            # just return it
            return adapter

        # otherwise figure it out.
        alias = loadopt.local_opts["eager_from_alias"]

        root_mapper, prop = path[-2:]

        #from .mapper import Mapper
        #from .interfaces import MapperProperty
        #assert isinstance(root_mapper, Mapper)
        #assert isinstance(prop, MapperProperty)

        if alias is not None:
            if isinstance(alias, str):
                alias = prop.target.alias(alias)
            adapter = sql_util.ColumnAdapter(
                alias,
                equivalents=prop.mapper._equivalent_columns)
        else:
            if path.contains(context.attributes, "path_with_polymorphic"):
                with_poly_info = path.get(
                    context.attributes,
                    "path_with_polymorphic")
                adapter = orm_util.ORMAdapter(
                    with_poly_info.entity,
                    equivalents=prop.mapper._equivalent_columns)
            else:
                adapter = context.query._polymorphic_adapters.get(
                    prop.mapper, None)
        path.set(
            context.attributes,
            "user_defined_eager_row_processor",
            adapter)

        return adapter

    def _setup_query_on_user_defined_adapter(
            self, context, entity,
            path, adapter, user_defined_adapter):

        # apply some more wrapping to the "user defined adapter"
        # if we are setting up the query for SQL render.
        adapter = entity._get_entity_clauses(context.query, context)

        if adapter and user_defined_adapter:
            user_defined_adapter = user_defined_adapter.wrap(adapter)
            path.set(
                context.attributes, "user_defined_eager_row_processor",
                user_defined_adapter)
        elif adapter:
            user_defined_adapter = adapter
            path.set(
                context.attributes, "user_defined_eager_row_processor",
                user_defined_adapter)

        add_to_collection = context.primary_columns
        return user_defined_adapter, adapter, add_to_collection

    def _generate_row_adapter(
            self,
            context, entity, path, loadopt, adapter,
            column_collection, parentmapper, chained_from_outerjoin):
        with_poly_info = path.get(
            context.attributes,
            "path_with_polymorphic",
            None
        )
        if with_poly_info:
            to_adapt = with_poly_info.entity
        else:
            to_adapt = orm_util.AliasedClass(
                self.mapper,
                flat=True,
                use_mapper_path=True)
        clauses = orm_util.ORMAdapter(
            to_adapt,
            equivalents=self.mapper._equivalent_columns,
            adapt_required=True, allow_label_resolve=False,
            anonymize_labels=True)
        assert clauses.aliased_class is not None

        if self.parent_property.uselist:
            context.multi_row_eager_loaders = True

        innerjoin = (
            loadopt.local_opts.get(
                'innerjoin', self.parent_property.innerjoin)
            if loadopt is not None
            else self.parent_property.innerjoin
        )

        if not innerjoin:
            # if this is an outer join, all non-nested eager joins from
            # this path must also be outer joins
            chained_from_outerjoin = True

        context.create_eager_joins.append(
            (
                self._create_eager_join, context,
                entity, path, adapter,
                parentmapper, clauses, innerjoin, chained_from_outerjoin
            )
        )

        add_to_collection = context.secondary_columns
        path.set(context.attributes, "eager_row_processor", clauses)

        return clauses, adapter, add_to_collection, chained_from_outerjoin

    def _create_eager_join(
            self, context, entity,
            path, adapter, parentmapper,
            clauses, innerjoin, chained_from_outerjoin):

        if parentmapper is None:
            localparent = entity.mapper
        else:
            localparent = parentmapper

        # whether or not the Query will wrap the selectable in a subquery,
        # and then attach eager load joins to that (i.e., in the case of
        # LIMIT/OFFSET etc.)
        should_nest_selectable = context.multi_row_eager_loaders and \
            context.query._should_nest_selectable

        entity_key = None

        if entity not in context.eager_joins and \
            not should_nest_selectable and \
                context.from_clause:
            index, clause = sql_util.find_join_source(
                context.from_clause, entity.selectable)
            if clause is not None:
                # join to an existing FROM clause on the query.
                # key it to its list index in the eager_joins dict.
                # Query._compile_context will adapt as needed and
                # append to the FROM clause of the select().
                entity_key, default_towrap = index, clause

        if entity_key is None:
            entity_key, default_towrap = entity, entity.selectable

        towrap = context.eager_joins.setdefault(entity_key, default_towrap)

        if adapter:
            if getattr(adapter, 'aliased_class', None):
                # joining from an adapted entity.  The adapted entity
                # might be a "with_polymorphic", so resolve that to our
                # specific mapper's entity before looking for our attribute
                # name on it.
                efm = inspect(adapter.aliased_class).\
                    _entity_for_mapper(
                        parentmapper
                        if parentmapper.isa(self.parent) else self.parent)

                # look for our attribute on the adapted entity, else fall back
                # to our straight property
                onclause = getattr(
                    efm.entity, self.key,
                    self.parent_property)
            else:
                onclause = getattr(
                    orm_util.AliasedClass(
                        self.parent,
                        adapter.selectable,
                        use_mapper_path=True
                    ),
                    self.key, self.parent_property
                )

        else:
            onclause = self.parent_property

        assert clauses.aliased_class is not None

        attach_on_outside = (
            not chained_from_outerjoin or
            not innerjoin or innerjoin == 'unnested')

        if attach_on_outside:
            # this is the "classic" eager join case.
            eagerjoin = orm_util._ORMJoin(
                towrap,
                clauses.aliased_class,
                onclause,
                isouter=not innerjoin or (
                    chained_from_outerjoin and isinstance(towrap, sql.Join)
                ), _left_memo=self.parent, _right_memo=self.mapper
            )
        else:
            # all other cases are innerjoin=='nested' approach
            eagerjoin = self._splice_nested_inner_join(
                path, towrap, clauses, onclause)

        context.eager_joins[entity_key] = eagerjoin

        # send a hint to the Query as to where it may "splice" this join
        eagerjoin.stop_on = entity.selectable

        if not parentmapper:
            # for parentclause that is the non-eager end of the join,
            # ensure all the parent cols in the primaryjoin are actually
            # in the
            # columns clause (i.e. are not deferred), so that aliasing applied
            # by the Query propagates those columns outward.
            # This has the effect
            # of "undefering" those columns.
            for col in sql_util._find_columns(
                    self.parent_property.primaryjoin):
                if localparent.mapped_table.c.contains_column(col):
                    if adapter:
                        col = adapter.columns[col]
                    context.primary_columns.append(col)

        if self.parent_property.order_by:
            context.eager_order_by += eagerjoin._target_adapter.\
                copy_and_process(
                    util.to_list(
                        self.parent_property.order_by
                    )
                )

    def _splice_nested_inner_join(
            self, path, join_obj, clauses, onclause, splicing=False):

        if splicing is False:
            # first call is always handed a join object
            # from the outside
            assert isinstance(join_obj, orm_util._ORMJoin)
        elif isinstance(join_obj, sql.selectable.FromGrouping):
            return self._splice_nested_inner_join(
                path, join_obj.element, clauses, onclause, splicing
            )
        elif not isinstance(join_obj, orm_util._ORMJoin):
            if path[-2] is splicing:
                return orm_util._ORMJoin(
                    join_obj, clauses.aliased_class,
                    onclause, isouter=False,
                    _left_memo=splicing,
                    _right_memo=path[-1].mapper
                )
            else:
                # only here if splicing == True
                return None

        target_join = self._splice_nested_inner_join(
            path, join_obj.right, clauses,
            onclause, join_obj._right_memo)
        if target_join is None:
            right_splice = False
            target_join = self._splice_nested_inner_join(
                path, join_obj.left, clauses,
                onclause, join_obj._left_memo)
            if target_join is None:
                # should only return None when recursively called,
                # e.g. splicing==True
                assert splicing is not False, \
                    "assertion failed attempting to produce joined eager loads"
                return None
        else:
            right_splice = True

        if right_splice:
            # for a right splice, attempt to flatten out
            # a JOIN b JOIN c JOIN .. to avoid needless
            # parenthesis nesting
            if not join_obj.isouter and not target_join.isouter:
                eagerjoin = join_obj._splice_into_center(target_join)
            else:
                eagerjoin = orm_util._ORMJoin(
                    join_obj.left, target_join,
                    join_obj.onclause, isouter=join_obj.isouter,
                    _left_memo=join_obj._left_memo)
        else:
            eagerjoin = orm_util._ORMJoin(
                target_join, join_obj.right,
                join_obj.onclause, isouter=join_obj.isouter,
                _right_memo=join_obj._right_memo)

        eagerjoin._target_adapter = target_join._target_adapter
        return eagerjoin

    def _create_eager_adapter(self, context, result, adapter, path, loadopt):
        user_defined_adapter = self._init_user_defined_eager_proc(
            loadopt, context) if loadopt else False

        if user_defined_adapter is not False:
            decorator = user_defined_adapter
            # user defined eagerloads are part of the "primary"
            # portion of the load.
            # the adapters applied to the Query should be honored.
            if context.adapter and decorator:
                decorator = decorator.wrap(context.adapter)
            elif context.adapter:
                decorator = context.adapter
        else:
            decorator = path.get(context.attributes, "eager_row_processor")
            if decorator is None:
                return False

        if self.mapper._result_has_identity_key(result, decorator):
            return decorator
        else:
            # no identity key - don't return a row
            # processor, will cause a degrade to lazy
            return False

    def create_row_processor(
            self, context, path, loadopt, mapper,
            result, adapter, populators):
        if not self.parent.class_manager[self.key].impl.supports_population:
            raise sa_exc.InvalidRequestError(
                "'%s' does not support object "
                "population - eager loading cannot be applied." %
                self
            )

        our_path = path[self.parent_property]

        eager_adapter = self._create_eager_adapter(
            context,
            result,
            adapter, our_path, loadopt)

        if eager_adapter is not False:
            key = self.key

            _instance = loading._instance_processor(
                self.mapper,
                context,
                result,
                our_path[self.mapper],
                eager_adapter)

            if not self.uselist:
                self._create_scalar_loader(context, key, _instance, populators)
            else:
                self._create_collection_loader(
                    context, key, _instance, populators)
        else:
            self.parent_property._get_strategy((("lazy", "select"),)).\
                create_row_processor(
                    context, path, loadopt,
                    mapper, result, adapter, populators)

    def _create_collection_loader(self, context, key, _instance, populators):
        def load_collection_from_joined_new_row(state, dict_, row):
            collection = attributes.init_state_collection(
                state, dict_, key)
            result_list = util.UniqueAppender(collection,
                                              'append_without_event')
            context.attributes[(state, key)] = result_list
            inst = _instance(row)
            if inst is not None:
                result_list.append(inst)

        def load_collection_from_joined_existing_row(state, dict_, row):
            if (state, key) in context.attributes:
                result_list = context.attributes[(state, key)]
            else:
                # appender_key can be absent from context.attributes
                # with isnew=False when self-referential eager loading
                # is used; the same instance may be present in two
                # distinct sets of result columns
                collection = attributes.init_state_collection(
                    state, dict_, key)
                result_list = util.UniqueAppender(
                    collection,
                    'append_without_event')
                context.attributes[(state, key)] = result_list
            inst = _instance(row)
            if inst is not None:
                result_list.append(inst)

        def load_collection_from_joined_exec(state, dict_, row):
            _instance(row)

        populators["new"].append((self.key, load_collection_from_joined_new_row))
        populators["existing"].append(
            (self.key, load_collection_from_joined_existing_row))
        if context.invoke_all_eagers:
            populators["eager"].append(
                (self.key, load_collection_from_joined_exec))

    def _create_scalar_loader(self, context, key, _instance, populators):
        def load_scalar_from_joined_new_row(state, dict_, row):
            # set a scalar object instance directly on the parent
            # object, bypassing InstrumentedAttribute event handlers.
            dict_[key] = _instance(row)

        def load_scalar_from_joined_existing_row(state, dict_, row):
            # call _instance on the row, even though the object has
            # been created, so that we further descend into properties
            existing = _instance(row)

            # conflicting value already loaded, this shouldn't happen
            if key in dict_:
                if existing is not dict_[key]:
                    util.warn(
                        "Multiple rows returned with "
                        "uselist=False for eagerly-loaded attribute '%s' "
                        % self)
            else:
                # this case is when one row has multiple loads of the
                # same entity (e.g. via aliasing), one has an attribute
                # that the other doesn't.
                dict_[key] = existing

        def load_scalar_from_joined_exec(state, dict_, row):
            _instance(row)

        populators["new"].append((self.key, load_scalar_from_joined_new_row))
        populators["existing"].append(
            (self.key, load_scalar_from_joined_existing_row))
        if context.invoke_all_eagers:
            populators["eager"].append((self.key, load_scalar_from_joined_exec))


def single_parent_validator(desc, prop):
    def _do_check(state, value, oldvalue, initiator):
        if value is not None and initiator.key == prop.key:
            hasparent = initiator.hasparent(attributes.instance_state(value))
            if hasparent and oldvalue is not value:
                raise sa_exc.InvalidRequestError(
                    "Instance %s is already associated with an instance "
                    "of %s via its %s attribute, and is only allowed a "
                    "single parent." %
                    (orm_util.instance_str(value), state.class_, prop)
                )
        return value

    def append(state, value, initiator):
        return _do_check(state, value, None, initiator)

    def set_(state, value, oldvalue, initiator):
        return _do_check(state, value, oldvalue, initiator)

    event.listen(
        desc, 'append', append, raw=True, retval=True,
        active_history=True)
    event.listen(
        desc, 'set', set_, raw=True, retval=True,
        active_history=True)
