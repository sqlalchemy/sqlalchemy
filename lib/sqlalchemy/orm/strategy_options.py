# Copyright (C) 2005-2017 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""

"""

from .interfaces import MapperOption, PropComparator, MapperProperty
from .attributes import QueryableAttribute
from .. import util
from ..sql.base import _generative, Generative
from .. import exc as sa_exc, inspect
from .base import _is_aliased_class, _class_to_mapper, _is_mapped_class
from . import util as orm_util
from .path_registry import PathRegistry, TokenRegistry, \
    _WILDCARD_TOKEN, _DEFAULT_TOKEN


class Load(Generative, MapperOption):
    """Represents loader options which modify the state of a
    :class:`.Query` in order to affect how various mapped attributes are
    loaded.

    The :class:`.Load` object is in most cases used implicitly behind the
    scenes when one makes use of a query option like :func:`.joinedload`,
    :func:`.defer`, or similar.   However, the :class:`.Load` object
    can also be used directly, and in some cases can be useful.

    To use :class:`.Load` directly, instantiate it with the target mapped
    class as the argument.   This style of usage is
    useful when dealing with a :class:`.Query` that has multiple entities::

        myopt = Load(MyClass).joinedload("widgets")

    The above ``myopt`` can now be used with :meth:`.Query.options`, where it
    will only take effect for the ``MyClass`` entity::

        session.query(MyClass, MyOtherClass).options(myopt)

    One case where :class:`.Load` is useful as public API is when specifying
    "wildcard" options that only take effect for a certain class::

        session.query(Order).options(Load(Order).lazyload('*'))

    Above, all relationships on ``Order`` will be lazy-loaded, but other
    attributes on those descendant objects will load using their normal
    loader strategy.

    .. seealso::

        :ref:`loading_toplevel`

    """

    def __init__(self, entity):
        insp = inspect(entity)
        self.path = insp._path_registry
        # note that this .context is shared among all descendant
        # Load objects
        self.context = util.OrderedDict()
        self.local_opts = {}
        self._of_type = None
        self.is_class_strategy = False

    @classmethod
    def for_existing_path(cls, path):
        load = cls.__new__(cls)
        load.path = path
        load.context = {}
        load.local_opts = {}
        load._of_type = None
        return load

    def _generate_cache_key(self, path):
        if path.path[0].is_aliased_class:
            return False

        serialized = []
        for (key, loader_path), obj in self.context.items():
            if key != "loader":
                continue

            endpoint = obj._of_type or obj.path.path[-1]
            chopped = self._chop_path(loader_path, path)
            if not chopped and not obj._of_type:
                continue

            serialized_path = []

            for token in chopped:
                if isinstance(token, util.string_types):
                    serialized_path.append(token)
                elif token.is_aliased_class:
                    return False
                elif token.is_property:
                    serialized_path.append(token.key)
                else:
                    assert token.is_mapper
                    serialized_path.append(token.class_)

            if not serialized_path or endpoint != serialized_path[-1]:
                if endpoint.is_mapper:
                    serialized_path.append(endpoint.class_)
                elif endpoint.is_aliased_class:
                    return False

            serialized.append(
                (
                    tuple(serialized_path) +
                    (obj.strategy or ()) +
                    (tuple([
                        (key, obj.local_opts[key])
                        for key in sorted(obj.local_opts)
                    ]) if obj.local_opts else ())
                )
            )
        if not serialized:
            return None
        else:
            return tuple(serialized)

    def _generate(self):
        cloned = super(Load, self)._generate()
        cloned.local_opts = {}
        return cloned

    is_opts_only = False
    is_class_strategy = False
    strategy = None
    propagate_to_loaders = False

    def process_query(self, query):
        self._process(query, True)

    def process_query_conditionally(self, query):
        self._process(query, False)

    def _process(self, query, raiseerr):
        current_path = query._current_path
        if current_path:
            for (token, start_path), loader in self.context.items():
                chopped_start_path = self._chop_path(start_path, current_path)
                if chopped_start_path is not None:
                    query._attributes[(token, chopped_start_path)] = loader
        else:
            query._attributes.update(self.context)

    def _generate_path(self, path, attr, wildcard_key, raiseerr=True):
        self._of_type = None

        if raiseerr and not path.has_entity:
            if isinstance(path, TokenRegistry):
                raise sa_exc.ArgumentError(
                    "Wildcard token cannot be followed by another entity")
            else:
                raise sa_exc.ArgumentError(
                    "Attribute '%s' of entity '%s' does not "
                    "refer to a mapped entity" %
                    (path.prop.key, path.parent.entity)
                )

        if isinstance(attr, util.string_types):
            default_token = attr.endswith(_DEFAULT_TOKEN)
            if attr.endswith(_WILDCARD_TOKEN) or default_token:
                if default_token:
                    self.propagate_to_loaders = False
                if wildcard_key:
                    attr = "%s:%s" % (wildcard_key, attr)
                path = path.token(attr)
                self.path = path
                return path

            try:
                # use getattr on the class to work around
                # synonyms, hybrids, etc.
                attr = getattr(path.entity.class_, attr)
            except AttributeError:
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Can't find property named '%s' on the "
                        "mapped entity %s in this Query. " % (
                            attr, path.entity)
                    )
                else:
                    return None
            else:
                attr = attr.property

            path = path[attr]
        elif _is_mapped_class(attr):
            if not attr.common_parent(path.mapper):
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Attribute '%s' does not "
                        "link from element '%s'" % (attr, path.entity))
                else:
                    return None
        else:
            prop = attr.property

            if not prop.parent.common_parent(path.mapper):
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Attribute '%s' does not "
                        "link from element '%s'" % (attr, path.entity))
                else:
                    return None

            if getattr(attr, '_of_type', None):
                ac = attr._of_type
                ext_info = inspect(ac)

                existing = path.entity_path[prop].get(
                    self.context, "path_with_polymorphic")
                if not ext_info.is_aliased_class:
                    ac = orm_util.with_polymorphic(
                        ext_info.mapper.base_mapper,
                        ext_info.mapper, aliased=True,
                        _use_mapper_path=True,
                        _existing_alias=existing)
                path.entity_path[prop].set(
                    self.context, "path_with_polymorphic", inspect(ac))

                # the path here will go into the context dictionary and
                # needs to match up to how the class graph is traversed.
                # so we can't put an AliasedInsp in the path here, needs
                # to be the base mapper.
                path = path[prop][ext_info.mapper]

                # but, we need to know what the original of_type()
                # argument is for cache key purposes.  so....store that too.
                # it might be better for "path" to really represent,
                # "the path", but trying to keep the impact of the cache
                # key feature localized for now
                self._of_type = ext_info
            else:
                path = path[prop]

        if path.has_entity:
            path = path.entity_path
        self.path = path
        return path

    def __str__(self):
        return "Load(strategy=%r)" % (self.strategy, )

    def _coerce_strat(self, strategy):
        if strategy is not None:
            strategy = tuple(sorted(strategy.items()))
        return strategy

    @_generative
    def set_relationship_strategy(
            self, attr, strategy, propagate_to_loaders=True):
        strategy = self._coerce_strat(strategy)

        self.is_class_strategy = False
        self.propagate_to_loaders = propagate_to_loaders
        # if the path is a wildcard, this will set propagate_to_loaders=False
        self._generate_path(self.path, attr, "relationship")
        self.strategy = strategy
        if strategy is not None:
            self._set_path_strategy()

    @_generative
    def set_column_strategy(self, attrs, strategy, opts=None, opts_only=False):
        strategy = self._coerce_strat(strategy)

        self.is_class_strategy = False
        for attr in attrs:
            cloned = self._generate()
            cloned.strategy = strategy
            cloned._generate_path(self.path, attr, "column")
            cloned.propagate_to_loaders = True
            if opts:
                cloned.local_opts.update(opts)
            if opts_only:
                cloned.is_opts_only = True
            cloned._set_path_strategy()
        self.is_class_strategy = False

    @_generative
    def set_generic_strategy(self, attrs, strategy):
        strategy = self._coerce_strat(strategy)

        for attr in attrs:
            path = self._generate_path(self.path, attr, None)
            cloned = self._generate()
            cloned.strategy = strategy
            cloned.path = path
            cloned.propagate_to_loaders = True
            cloned._set_path_strategy()

    @_generative
    def set_class_strategy(self, strategy, opts):
        strategy = self._coerce_strat(strategy)
        cloned = self._generate()
        cloned.is_class_strategy = True
        path = cloned._generate_path(self.path, None, None)
        cloned.strategy = strategy
        cloned.path = path
        cloned.propagate_to_loaders = True
        cloned._set_path_strategy()
        cloned.local_opts.update(opts)

    def _set_for_path(self, context, path, replace=True, merge_opts=False):
        if merge_opts or not replace:
            existing = path.get(self.context, "loader")

            if existing:
                if merge_opts:
                    existing.local_opts.update(self.local_opts)
            else:
                path.set(context, "loader", self)
        else:
            existing = path.get(self.context, "loader")
            path.set(context, "loader", self)
            if existing and existing.is_opts_only:
                self.local_opts.update(existing.local_opts)

    def _set_path_strategy(self):
        if not self.is_class_strategy and self.path.has_entity:
            effective_path = self.path.parent
        else:
            effective_path = self.path

        self._set_for_path(
            self.context, effective_path, replace=True,
            merge_opts=self.is_opts_only)

    def __getstate__(self):
        d = self.__dict__.copy()
        d["path"] = self.path.serialize()
        return d

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.path = PathRegistry.deserialize(self.path)

    def _chop_path(self, to_chop, path):
        i = -1

        for i, (c_token, p_token) in enumerate(zip(to_chop, path.path)):
            if isinstance(c_token, util.string_types):
                # TODO: this is approximated from the _UnboundLoad
                # version and probably has issues, not fully covered.

                if i == 0 and c_token.endswith(':' + _DEFAULT_TOKEN):
                    return to_chop
                elif c_token != 'relationship:%s' % (_WILDCARD_TOKEN,) and \
                        c_token != p_token.key:
                    return None

            if c_token is p_token:
                continue
            else:
                return None
        return to_chop[i + 1:]


class _UnboundLoad(Load):
    """Represent a loader option that isn't tied to a root entity.

    The loader option will produce an entity-linked :class:`.Load`
    object when it is passed :metfh:`.Query.options`.

    This provides compatibility with the traditional system
    of freestanding options, e.g. ``joinedload('x.y.z')``.

    """

    def __init__(self):
        self.path = ()
        self._to_bind = []
        self.local_opts = {}

    _is_chain_link = False

    def _generate_cache_key(self, path):
        serialized = ()
        for val in self._to_bind:
            opt = val._bind_loader(
                [path.path[0]],
                None, None, False)
            if opt:
                c_key = opt._generate_cache_key(path)
                if c_key is False:
                    return False
                elif c_key:
                    serialized += c_key
        if not serialized:
            return None
        else:
            return serialized

    def _set_path_strategy(self):
        self._to_bind.append(self)

    def _generate_path(self, path, attr, wildcard_key):
        if wildcard_key and isinstance(attr, util.string_types) and \
                attr in (_WILDCARD_TOKEN, _DEFAULT_TOKEN):
            if attr == _DEFAULT_TOKEN:
                self.propagate_to_loaders = False
            attr = "%s:%s" % (wildcard_key, attr)
        if path and _is_mapped_class(path[-1]) and not self.is_class_strategy:
            path = path[0:-1]
        if attr:
            path = path + (attr, )
        self.path = path
        return path

    def __getstate__(self):
        d = self.__dict__.copy()
        d['path'] = self._serialize_path(self.path)
        return d

    def __setstate__(self, state):
        ret = []
        for key in state['path']:
            if isinstance(key, tuple):
                if len(key) == 2:
                    # support legacy
                    cls, propkey = key
                else:
                    cls, propkey, of_type = key
                prop = getattr(cls, propkey)
                if of_type:
                    prop = prop.of_type(prop)
                ret.append(prop)
            else:
                ret.append(key)
        state['path'] = tuple(ret)
        self.__dict__ = state

    def _process(self, query, raiseerr):
        for val in self._to_bind:
            val._bind_loader(
                [ent.entity_zero for ent in query._mapper_entities],
                query._current_path, query._attributes, raiseerr)

    @classmethod
    def _from_keys(cls, meth, keys, chained, kw):
        opt = _UnboundLoad()

        def _split_key(key):
            if isinstance(key, util.string_types):
                # coerce fooload('*') into "default loader strategy"
                if key == _WILDCARD_TOKEN:
                    return (_DEFAULT_TOKEN, )
                # coerce fooload(".*") into "wildcard on default entity"
                elif key.startswith("." + _WILDCARD_TOKEN):
                    key = key[1:]
                return key.split(".")
            else:
                return (key,)
        all_tokens = [token for key in keys for token in _split_key(key)]

        for token in all_tokens[0:-1]:
            if chained:
                opt = meth(opt, token, **kw)
            else:
                opt = opt.defaultload(token)
            opt._is_chain_link = True

        opt = meth(opt, all_tokens[-1], **kw)
        opt._is_chain_link = False

        return opt

    def _chop_path(self, to_chop, path):
        i = -1
        for i, (c_token, (p_entity, p_prop)) in enumerate(
                zip(to_chop, path.pairs())):
            if isinstance(c_token, util.string_types):
                if i == 0 and c_token.endswith(':' + _DEFAULT_TOKEN):
                    return to_chop
                elif c_token != 'relationship:%s' % (
                        _WILDCARD_TOKEN,) and c_token != p_prop.key:
                    return None
            elif isinstance(c_token, PropComparator):
                if c_token.property is not p_prop or \
                        (
                            c_token._parententity is not p_entity and (
                                not c_token._parententity.is_mapper or
                                not c_token._parententity.isa(p_entity)
                            )
                        ):
                    return None
        else:
            i += 1

        return to_chop[i:]

    def _serialize_path(self, path, reject_aliased_class=False):
        ret = []
        for token in path:
            if isinstance(token, QueryableAttribute):
                if reject_aliased_class and (
                    (token._of_type and
                        inspect(token._of_type).is_aliased_class)
                    or
                    inspect(token.parent).is_aliased_class
                ):
                    return False
                ret.append(
                    (token._parentmapper.class_, token.key, token._of_type))
            elif isinstance(token, PropComparator):
                ret.append((token._parentmapper.class_, token.key, None))
            else:
                ret.append(token)
        return ret

    def _bind_loader(self, entities, current_path, context, raiseerr):
        """Convert from an _UnboundLoad() object into a Load() object.

        The _UnboundLoad() uses an informal "path" and does not necessarily
        refer to a lead entity as it may use string tokens.   The Load()
        OTOH refers to a complete path.   This method reconciles from a
        given Query into a Load.

        Example::


            query = session.query(User).options(
                joinedload("orders").joinedload("items"))

        The above options will be an _UnboundLoad object along the lines
        of (note this is not the exact API of _UnboundLoad)::

            _UnboundLoad(
                _to_bind=[
                    _UnboundLoad(["orders"], {"lazy": "joined"}),
                    _UnboundLoad(["orders", "items"], {"lazy": "joined"}),
                ]
            )

        After this method, we get something more like this (again this is
        not exact API)::

            Load(
                User,
                (User, User.orders.property))
            Load(
                User,
                (User, User.orders.property, Order, Order.items.property))

        """

        start_path = self.path

        if self.is_class_strategy and current_path:
            start_path += (entities[0], )

        # _current_path implies we're in a
        # secondary load with an existing path

        if current_path:
            start_path = self._chop_path(start_path, current_path)

        if not start_path:
            return None

        # look at the first token and try to locate within the Query
        # what entity we are referring towards.
        token = start_path[0]

        if isinstance(token, util.string_types):
            entity = self._find_entity_basestring(
                entities, token, raiseerr)
        elif isinstance(token, PropComparator):
            prop = token.property
            entity = self._find_entity_prop_comparator(
                entities,
                prop.key,
                token._parententity,
                raiseerr)
        elif self.is_class_strategy and _is_mapped_class(token):
            entity = inspect(token)
            if entity not in entities:
                entity = None
        else:
            raise sa_exc.ArgumentError(
                "mapper option expects "
                "string key or list of attributes")

        if not entity:
            return

        path_element = entity

        # transfer our entity-less state into a Load() object
        # with a real entity path.  Start with the lead entity
        # we just located, then go through the rest of our path
        # tokens and populate into the Load().
        loader = Load(path_element)
        if context is not None:
            loader.context = context
        else:
            context = loader.context

        loader.strategy = self.strategy
        loader.is_opts_only = self.is_opts_only
        loader.is_class_strategy = self.is_class_strategy

        path = loader.path

        if not loader.is_class_strategy:
            for token in start_path:
                if not loader._generate_path(
                        loader.path, token, None, raiseerr):
                    return

        loader.local_opts.update(self.local_opts)

        if not loader.is_class_strategy and loader.path.has_entity:
            effective_path = loader.path.parent
        else:
            effective_path = loader.path

        # prioritize "first class" options over those
        # that were "links in the chain", e.g. "x" and "y" in
        # someload("x.y.z") versus someload("x") / someload("x.y")

        if effective_path.is_token:
            for path in effective_path.generate_for_superclasses():
                loader._set_for_path(
                    context, path,
                    replace=not self._is_chain_link,
                    merge_opts=self.is_opts_only)
        else:
            loader._set_for_path(
                context, effective_path,
                replace=not self._is_chain_link,
                merge_opts=self.is_opts_only)

        return loader

    def _find_entity_prop_comparator(self, entities, token, mapper, raiseerr):
        if _is_aliased_class(mapper):
            searchfor = mapper
        else:
            searchfor = _class_to_mapper(mapper)
        for ent in entities:
            if orm_util._entity_corresponds_to(ent, searchfor):
                return ent
        else:
            if raiseerr:
                if not list(entities):
                    raise sa_exc.ArgumentError(
                        "Query has only expression-based entities - "
                        "can't find property named '%s'."
                        % (token, )
                    )
                else:
                    raise sa_exc.ArgumentError(
                        "Can't find property '%s' on any entity "
                        "specified in this Query.  Note the full path "
                        "from root (%s) to target entity must be specified."
                        % (token, ",".join(str(x) for
                                           x in entities))
                    )
            else:
                return None

    def _find_entity_basestring(self, entities, token, raiseerr):
        if token.endswith(':' + _WILDCARD_TOKEN):
            if len(list(entities)) != 1:
                if raiseerr:
                    raise sa_exc.ArgumentError(
                        "Wildcard loader can only be used with exactly "
                        "one entity.  Use Load(ent) to specify "
                        "specific entities.")
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
                    "can't find property named '%s'."
                    % (token, )
                )
            else:
                return None


class loader_option(object):
    def __init__(self):
        pass

    def __call__(self, fn):
        self.name = name = fn.__name__
        self.fn = fn
        if hasattr(Load, name):
            raise TypeError("Load class already has a %s method." % (name))
        setattr(Load, name, fn)

        return self

    def _add_unbound_fn(self, fn):
        self._unbound_fn = fn
        fn_doc = self.fn.__doc__
        self.fn.__doc__ = """Produce a new :class:`.Load` object with the
:func:`.orm.%(name)s` option applied.

See :func:`.orm.%(name)s` for usage examples.

""" % {"name": self.name}

        fn.__doc__ = fn_doc
        return self

    def _add_unbound_all_fn(self, fn):
        self._unbound_all_fn = fn
        fn.__doc__ = """Produce a standalone "all" option for :func:`.orm.%(name)s`.

.. deprecated:: 0.9.0

    The "_all()" style is replaced by method chaining, e.g.::

        session.query(MyClass).options(
            %(name)s("someattribute").%(name)s("anotherattribute")
        )

""" % {"name": self.name}
        return self


@loader_option()
def contains_eager(loadopt, attr, alias=None):
    r"""Indicate that the given attribute should be eagerly loaded from
    columns stated manually in the query.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    The option is used in conjunction with an explicit join that loads
    the desired rows, i.e.::

        sess.query(Order).\
                join(Order.user).\
                options(contains_eager(Order.user))

    The above query would join from the ``Order`` entity to its related
    ``User`` entity, and the returned ``Order`` objects would have the
    ``Order.user`` attribute pre-populated.

    :func:`contains_eager` also accepts an `alias` argument, which is the
    string name of an alias, an :func:`~sqlalchemy.sql.expression.alias`
    construct, or an :func:`~sqlalchemy.orm.aliased` construct. Use this when
    the eagerly-loaded rows are to come from an aliased table::

        user_alias = aliased(User)
        sess.query(Order).\
                join((user_alias, Order.user)).\
                options(contains_eager(Order.user, alias=user_alias))

    .. seealso::

        :ref:`loading_toplevel`

        :ref:`contains_eager`

    """
    if alias is not None:
        if not isinstance(alias, str):
            info = inspect(alias)
            alias = info.selectable

    cloned = loadopt.set_relationship_strategy(
        attr,
        {"lazy": "joined"},
        propagate_to_loaders=False
    )
    cloned.local_opts['eager_from_alias'] = alias
    return cloned


@contains_eager._add_unbound_fn
def contains_eager(*keys, **kw):
    return _UnboundLoad()._from_keys(
        _UnboundLoad.contains_eager, keys, True, kw)


@loader_option()
def load_only(loadopt, *attrs):
    """Indicate that for a particular entity, only the given list
    of column-based attribute names should be loaded; all others will be
    deferred.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    Example - given a class ``User``, load only the ``name`` and ``fullname``
    attributes::

        session.query(User).options(load_only("name", "fullname"))

    Example - given a relationship ``User.addresses -> Address``, specify
    subquery loading for the ``User.addresses`` collection, but on each
    ``Address`` object load only the ``email_address`` attribute::

        session.query(User).options(
                subqueryload("addresses").load_only("email_address")
        )

    For a :class:`.Query` that has multiple entities, the lead entity can be
    specifically referred to using the :class:`.Load` constructor::

        session.query(User, Address).join(User.addresses).options(
                    Load(User).load_only("name", "fullname"),
                    Load(Address).load_only("email_addres")
                )


    .. versionadded:: 0.9.0

    """
    cloned = loadopt.set_column_strategy(
        attrs,
        {"deferred": False, "instrument": True}
    )
    cloned.set_column_strategy("*",
                               {"deferred": True, "instrument": True},
                               {"undefer_pks": True})
    return cloned


@load_only._add_unbound_fn
def load_only(*attrs):
    return _UnboundLoad().load_only(*attrs)


@loader_option()
def joinedload(loadopt, attr, innerjoin=None):
    """Indicate that the given attribute should be loaded using joined
    eager loading.

    This function is part of the :class:`.Load` interface and supports
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

    :param innerjoin: if ``True``, indicates that the joined eager load should
     use an inner join instead of the default of left outer join::

        query(Order).options(joinedload(Order.user, innerjoin=True))

     In order to chain multiple eager joins together where some may be
     OUTER and others INNER, right-nested joins are used to link them::

        query(A).options(
            joinedload(A.bs, innerjoin=False).
                joinedload(B.cs, innerjoin=True)
        )

     The above query, linking A.bs via "outer" join and B.cs via "inner" join
     would render the joins as "a LEFT OUTER JOIN (b JOIN c)".   When using
     older versions of SQLite (< 3.7.16), this form of JOIN is translated to
     use full subqueries as this syntax is otherwise not directly supported.

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
        from a many-to-many association table, e.g. a table configured
        as :paramref:`.relationship.secondary`, to the target table; for
        correctness of results, these joins are always INNER and are
        therefore right-nested if linked to an OUTER join.

     .. versionchanged:: 1.0.0 ``innerjoin=True`` now implies
        ``innerjoin="nested"``, whereas in 0.9 it implied
        ``innerjoin="unnested"``.  In order to achieve the pre-1.0 "unnested"
        inner join behavior, use the value ``innerjoin="unnested"``.
        See :ref:`migration_3008`.

    .. note::

        The joins produced by :func:`.orm.joinedload` are **anonymously
        aliased**.  The criteria by which the join proceeds cannot be
        modified, nor can the :class:`.Query` refer to these joins in any way,
        including ordering.  See :ref:`zen_of_eager_loading` for further
        detail.

        To produce a specific SQL JOIN which is explicitly available, use
        :meth:`.Query.join`.   To combine explicit JOINs with eager loading
        of collections, use :func:`.orm.contains_eager`; see
        :ref:`contains_eager`.

    .. seealso::

        :ref:`loading_toplevel`

        :ref:`joined_eager_loading`

    """
    loader = loadopt.set_relationship_strategy(attr, {"lazy": "joined"})
    if innerjoin is not None:
        loader.local_opts['innerjoin'] = innerjoin
    return loader


@joinedload._add_unbound_fn
def joinedload(*keys, **kw):
    return _UnboundLoad._from_keys(
        _UnboundLoad.joinedload, keys, False, kw)


@joinedload._add_unbound_all_fn
def joinedload_all(*keys, **kw):
    return _UnboundLoad._from_keys(
        _UnboundLoad.joinedload, keys, True, kw)


@loader_option()
def subqueryload(loadopt, attr):
    """Indicate that the given attribute should be loaded using
    subquery eager loading.

    This function is part of the :class:`.Load` interface and supports
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
    return loadopt.set_relationship_strategy(attr, {"lazy": "subquery"})


@subqueryload._add_unbound_fn
def subqueryload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.subqueryload, keys, False, {})


@subqueryload._add_unbound_all_fn
def subqueryload_all(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.subqueryload, keys, True, {})


@loader_option()
def selectinload(loadopt, attr):
    """Indicate that the given attribute should be loaded using
    SELECT IN eager loading.

    This function is part of the :class:`.Load` interface and supports
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
    return loadopt.set_relationship_strategy(attr, {"lazy": "selectin"})


@selectinload._add_unbound_fn
def selectinload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.selectinload, keys, False, {})


@selectinload._add_unbound_all_fn
def selectinload_all(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.selectinload, keys, True, {})


@loader_option()
def lazyload(loadopt, attr):
    """Indicate that the given attribute should be loaded using "lazy"
    loading.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    .. seealso::

        :ref:`loading_toplevel`

        :ref:`lazy_loading`

    """
    return loadopt.set_relationship_strategy(attr, {"lazy": "select"})


@lazyload._add_unbound_fn
def lazyload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.lazyload, keys, False, {})


@lazyload._add_unbound_all_fn
def lazyload_all(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.lazyload, keys, True, {})


@loader_option()
def immediateload(loadopt, attr):
    """Indicate that the given attribute should be loaded using
    an immediate load with a per-attribute SELECT statement.

    The :func:`.immediateload` option is superseded in general
    by the :func:`.selectinload` option, which performs the same task
    more efficiently by emitting a SELECT for all loaded objects.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    .. seealso::

        :ref:`loading_toplevel`

        :ref:`selectin_eager_loading`

    """
    loader = loadopt.set_relationship_strategy(attr, {"lazy": "immediate"})
    return loader


@immediateload._add_unbound_fn
def immediateload(*keys):
    return _UnboundLoad._from_keys(
        _UnboundLoad.immediateload, keys, False, {})


@loader_option()
def noload(loadopt, attr):
    """Indicate that the given relationship attribute should remain unloaded.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    :func:`.orm.noload` applies to :func:`.relationship` attributes; for
    column-based attributes, see :func:`.orm.defer`.

    .. seealso::

        :ref:`loading_toplevel`

    """

    return loadopt.set_relationship_strategy(attr, {"lazy": "noload"})


@noload._add_unbound_fn
def noload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.noload, keys, False, {})


@loader_option()
def raiseload(loadopt, attr, sql_only=False):
    """Indicate that the given relationship attribute should disallow lazy loads.

    A relationship attribute configured with :func:`.orm.raiseload` will
    raise an :exc:`~sqlalchemy.exc.InvalidRequestError` upon access.   The
    typical way this is useful is when an application is attempting to ensure
    that all relationship attributes that are accessed in a particular context
    would have been already loaded via eager loading.  Instead of having
    to read through SQL logs to ensure lazy loads aren't occurring, this
    strategy will cause them to raise immediately.

    :param sql_only: if True, raise only if the lazy load would emit SQL,
     but not if it is only checking the identity map, or determining that
     the related value should just be None due to missing keys.  When False,
     the strategy will raise for all varieties of lazyload.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    :func:`.orm.raiseload` applies to :func:`.relationship` attributes only.

    .. versionadded:: 1.1

    .. seealso::

        :ref:`loading_toplevel`

        :ref:`prevent_lazy_with_raiseload`

    """

    return loadopt.set_relationship_strategy(
        attr, {"lazy": "raise_on_sql" if sql_only else "raise"})


@raiseload._add_unbound_fn
def raiseload(*keys, **kw):
    return _UnboundLoad._from_keys(_UnboundLoad.raiseload, keys, False, kw)


@loader_option()
def defaultload(loadopt, attr):
    """Indicate an attribute should load using its default loader style.

    This method is used to link to other loader options further into
    a chain of attributes without altering the loader style of the links
    along the chain.  For example, to set joined eager loading for an
    element of an element::

        session.query(MyClass).options(
            defaultload(MyClass.someattribute).
            joinedload(MyOtherClass.someotherattribute)
        )

    :func:`.defaultload` is also useful for setting column-level options
    on a related class, namely that of :func:`.defer` and :func:`.undefer`::

        session.query(MyClass).options(
            defaultload(MyClass.someattribute).
            defer("some_column").
            undefer("some_other_column")
        )

    .. seealso::

        :ref:`relationship_loader_options`

        :ref:`deferred_loading_w_multiple`

    """
    return loadopt.set_relationship_strategy(
        attr,
        None
    )


@defaultload._add_unbound_fn
def defaultload(*keys):
    return _UnboundLoad._from_keys(_UnboundLoad.defaultload, keys, False, {})


@loader_option()
def defer(loadopt, key):
    r"""Indicate that the given column-oriented attribute should be deferred, e.g.
    not loaded until accessed.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    e.g.::

        from sqlalchemy.orm import defer

        session.query(MyClass).options(
                            defer("attribute_one"),
                            defer("attribute_two"))

        session.query(MyClass).options(
                            defer(MyClass.attribute_one),
                            defer(MyClass.attribute_two))

    To specify a deferred load of an attribute on a related class,
    the path can be specified one token at a time, specifying the loading
    style for each link along the chain.  To leave the loading style
    for a link unchanged, use :func:`.orm.defaultload`::

        session.query(MyClass).options(defaultload("someattr").defer("some_column"))

    A :class:`.Load` object that is present on a certain path can have
    :meth:`.Load.defer` called multiple times, each will operate on the same
    parent entity::


        session.query(MyClass).options(
                        defaultload("someattr").
                            defer("some_column").
                            defer("some_other_column").
                            defer("another_column")
            )

    :param key: Attribute to be deferred.

    :param \*addl_attrs: Deprecated; this option supports the old 0.8 style
     of specifying a path as a series of attributes, which is now superseded
     by the method-chained style.

    .. seealso::

        :ref:`deferred`

        :func:`.orm.undefer`

    """
    return loadopt.set_column_strategy(
        (key, ),
        {"deferred": True, "instrument": True}
    )


@defer._add_unbound_fn
def defer(key, *addl_attrs):
    return _UnboundLoad._from_keys(
        _UnboundLoad.defer, (key, ) + addl_attrs, False, {})


@loader_option()
def undefer(loadopt, key):
    r"""Indicate that the given column-oriented attribute should be undeferred,
    e.g. specified within the SELECT statement of the entity as a whole.

    The column being undeferred is typically set up on the mapping as a
    :func:`.deferred` attribute.

    This function is part of the :class:`.Load` interface and supports
    both method-chained and standalone operation.

    Examples::

        # undefer two columns
        session.query(MyClass).options(undefer("col1"), undefer("col2"))

        # undefer all columns specific to a single class using Load + *
        session.query(MyClass, MyOtherClass).options(
            Load(MyClass).undefer("*"))

    :param key: Attribute to be undeferred.

    :param \*addl_attrs: Deprecated; this option supports the old 0.8 style
     of specifying a path as a series of attributes, which is now superseded
     by the method-chained style.

    .. seealso::

        :ref:`deferred`

        :func:`.orm.defer`

        :func:`.orm.undefer_group`

    """
    return loadopt.set_column_strategy(
        (key, ),
        {"deferred": False, "instrument": True}
    )


@undefer._add_unbound_fn
def undefer(key, *addl_attrs):
    return _UnboundLoad._from_keys(
        _UnboundLoad.undefer, (key, ) + addl_attrs, False, {})


@loader_option()
def undefer_group(loadopt, name):
    """Indicate that columns within the given deferred group name should be
    undeferred.

    The columns being undeferred are set up on the mapping as
    :func:`.deferred` attributes and include a "group" name.

    E.g::

        session.query(MyClass).options(undefer_group("large_attrs"))

    To undefer a group of attributes on a related entity, the path can be
    spelled out using relationship loader options, such as
    :func:`.orm.defaultload`::

        session.query(MyClass).options(
            defaultload("someattr").undefer_group("large_attrs"))

    .. versionchanged:: 0.9.0 :func:`.orm.undefer_group` is now specific to a
       particiular entity load path.

    .. seealso::

        :ref:`deferred`

        :func:`.orm.defer`

        :func:`.orm.undefer`

    """
    return loadopt.set_column_strategy(
        "*",
        None,
        {"undefer_group_%s" % name: True},
        opts_only=True
    )


@undefer_group._add_unbound_fn
def undefer_group(name):
    return _UnboundLoad().undefer_group(name)


from ..sql import expression as sql_expr
from .util import _orm_full_deannotate


@loader_option()
def with_expression(loadopt, key, expression):
    r"""Apply an ad-hoc SQL expression to a "deferred expression" attribute.

    This option is used in conjunction with the :func:`.orm.query_expression`
    mapper-level construct that indicates an attribute which should be the
    target of an ad-hoc SQL expression.

    E.g.::


        sess.query(SomeClass).options(
            with_expression(SomeClass.x_y_expr, SomeClass.x + SomeClass.y)
        )

    .. versionadded:: 1.2

    :param key: Attribute to be undeferred.

    :param expr: SQL expression to be applied to the attribute.

    .. seealso::

        :ref:`mapper_query_expression`

    """

    expression = sql_expr._labeled(
        _orm_full_deannotate(expression))

    return loadopt.set_column_strategy(
        (key, ),
        {"query_expression": True},
        opts={"expression": expression}
    )


@with_expression._add_unbound_fn
def with_expression(key, expression):
    return _UnboundLoad._from_keys(
        _UnboundLoad.with_expression, (key, ),
        False, {"expression": expression})


@loader_option()
def selectin_polymorphic(loadopt, classes):
    """Indicate an eager load should take place for all attributes
    specific to a subclass.

    This uses an additional SELECT with IN against all matched primary
    key values, and is the per-query analogue to the ``"selectin"``
    setting on the :paramref:`.mapper.polymorphic_load` parameter.

    .. versionadded:: 1.2

    .. seealso::

        :ref:`inheritance_polymorphic_load`

    """
    loadopt.set_class_strategy(
        {"selectinload_polymorphic": True},
        opts={"entities": tuple(sorted((inspect(cls) for cls in classes), key=id))}
    )
    return loadopt


@selectin_polymorphic._add_unbound_fn
def selectin_polymorphic(base_cls, classes):
    ul = _UnboundLoad()
    ul.is_class_strategy = True
    ul.path = (inspect(base_cls), )
    ul.selectin_polymorphic(
        classes
    )
    return ul
