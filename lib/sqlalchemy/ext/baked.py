# sqlalchemy/ext/baked.py
# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""Baked query extension.

Provides a creational pattern for the :class:`.query.Query` object which
allows the fully constructed object, Core select statement, and string
compiled result to be fully cached.


"""

from ..orm.query import Query
from ..orm import strategies, attributes, properties, \
    strategy_options, util as orm_util, interfaces
from .. import log as sqla_log
from ..sql import util as sql_util, func, literal_column
from ..orm import exc as orm_exc
from .. import exc as sa_exc
from .. import util

import copy
import logging

log = logging.getLogger(__name__)


class Bakery(object):
    """Callable which returns a :class:`.BakedQuery`.

    This object is returned by the class method
    :meth:`.BakedQuery.bakery`.  It exists as an object
    so that the "cache" can be easily inspected.

    .. versionadded:: 1.2


    """
    __slots__ = 'cls', 'cache'

    def __init__(self, cls_, cache):
        self.cls = cls_
        self.cache = cache

    def __call__(self, initial_fn, *args):
        return self.cls(self.cache, initial_fn, args)


class BakedQuery(object):
    """A builder object for :class:`.query.Query` objects."""

    __slots__ = 'steps', '_bakery', '_cache_key', '_spoiled'

    def __init__(self, bakery, initial_fn, args=()):
        self._cache_key = ()
        self._update_cache_key(initial_fn, args)
        self.steps = [initial_fn]
        self._spoiled = False
        self._bakery = bakery

    @classmethod
    def bakery(cls, size=200, _size_alert=None):
        """Construct a new bakery.

        :return: an instance of :class:`.Bakery`

        """

        return Bakery(cls, util.LRUCache(size, size_alert=_size_alert))

    def _clone(self):
        b1 = BakedQuery.__new__(BakedQuery)
        b1._cache_key = self._cache_key
        b1.steps = list(self.steps)
        b1._bakery = self._bakery
        b1._spoiled = self._spoiled
        return b1

    def _update_cache_key(self, fn, args=()):
        self._cache_key += (fn.__code__,) + args

    def __iadd__(self, other):
        if isinstance(other, tuple):
            self.add_criteria(*other)
        else:
            self.add_criteria(other)
        return self

    def __add__(self, other):
        if isinstance(other, tuple):
            return self.with_criteria(*other)
        else:
            return self.with_criteria(other)

    def add_criteria(self, fn, *args):
        """Add a criteria function to this :class:`.BakedQuery`.

        This is equivalent to using the ``+=`` operator to
        modify a :class:`.BakedQuery` in-place.

        """
        self._update_cache_key(fn, args)
        self.steps.append(fn)
        return self

    def with_criteria(self, fn, *args):
        """Add a criteria function to a :class:`.BakedQuery` cloned from this one.

        This is equivalent to using the ``+`` operator to
        produce a new :class:`.BakedQuery` with modifications.

        """
        return self._clone().add_criteria(fn, *args)

    def for_session(self, session):
        """Return a :class:`.Result` object for this :class:`.BakedQuery`.

        This is equivalent to calling the :class:`.BakedQuery` as a
        Python callable, e.g. ``result = my_baked_query(session)``.

        """
        return Result(self, session)

    def __call__(self, session):
        return self.for_session(session)

    def spoil(self, full=False):
        """Cancel any query caching that will occur on this BakedQuery object.

        The BakedQuery can continue to be used normally, however additional
        creational functions will not be cached; they will be called
        on every invocation.

        This is to support the case where a particular step in constructing
        a baked query disqualifies the query from being cacheable, such
        as a variant that relies upon some uncacheable value.

        :param full: if False, only functions added to this
         :class:`.BakedQuery` object subsequent to the spoil step will be
         non-cached; the state of the :class:`.BakedQuery` up until
         this point will be pulled from the cache.   If True, then the
         entire :class:`.Query` object is built from scratch each
         time, with all creational functions being called on each
         invocation.

        """
        if not full and not self._spoiled:
            _spoil_point = self._clone()
            _spoil_point._cache_key += ('_query_only', )
            self.steps = [_spoil_point._retrieve_baked_query]
        self._spoiled = True
        return self

    def _add_lazyload_options(self, options, effective_path, cache_path=None):
        """Used by per-state lazy loaders to add options to the
        "lazy load" query from a parent query.

        Creates a cache key based on given load path and query options;
        if a repeatable cache key cannot be generated, the query is
        "spoiled" so that it won't use caching.

        """

        key = ()

        if not cache_path:
            cache_path = effective_path

        if cache_path.path[0].is_aliased_class:
            # paths that are against an AliasedClass are unsafe to cache
            # with since the AliasedClass is an ad-hoc object.
            self.spoil()
        else:
            for opt in options:
                cache_key = opt._generate_cache_key(cache_path)
                if cache_key is False:
                    self.spoil()
                elif cache_key is not None:
                    key += cache_key

        self.add_criteria(
            lambda q: q._with_current_path(effective_path).
            _conditional_options(*options),
            cache_path.path, key
        )

    def _retrieve_baked_query(self, session):
        query = self._bakery.get(self._cache_key, None)
        if query is None:
            query = self._as_query(session)
            self._bakery[self._cache_key] = query.with_session(None)
        return query.with_session(session)

    def _bake(self, session):
        query = self._as_query(session)

        context = query._compile_context()
        self._bake_subquery_loaders(session, context)
        context.session = None
        context.query = query = context.query.with_session(None)
        query._execution_options = query._execution_options.union(
            {"compiled_cache": self._bakery}
        )
        # we'll be holding onto the query for some of its state,
        # so delete some compilation-use-only attributes that can take up
        # space
        for attr in (
                '_correlate', '_from_obj', '_mapper_adapter_map',
                '_joinpath', '_joinpoint'):
            query.__dict__.pop(attr, None)
        self._bakery[self._cache_key] = context
        return context

    def _as_query(self, session):
        query = self.steps[0](session)

        for step in self.steps[1:]:
            query = step(query)
        return query

    def _bake_subquery_loaders(self, session, context):
        """convert subquery eager loaders in the cache into baked queries.

        For subquery eager loading to work, all we need here is that the
        Query point to the correct session when it is run.  However, since
        we are "baking" anyway, we may as well also turn the query into
        a "baked" query so that we save on performance too.

        """
        context.attributes['baked_queries'] = baked_queries = []
        for k, v in list(context.attributes.items()):
            if isinstance(v, Query):
                if 'subquery' in k:
                    bk = BakedQuery(self._bakery, lambda *args: v)
                    bk._cache_key = self._cache_key + k
                    bk._bake(session)
                    baked_queries.append((k, bk._cache_key, v))
                del context.attributes[k]

    def _unbake_subquery_loaders(
            self, session, context, params, post_criteria):
        """Retrieve subquery eager loaders stored by _bake_subquery_loaders
        and turn them back into Result objects that will iterate just
        like a Query object.

        """
        for k, cache_key, query in context.attributes["baked_queries"]:
            bk = BakedQuery(self._bakery,
                            lambda sess, q=query: q.with_session(sess))
            bk._cache_key = cache_key
            q = bk.for_session(session)
            for fn in post_criteria:
                q = fn(q)
            context.attributes[k] = q.params(**params)


class Result(object):
    """Invokes a :class:`.BakedQuery` against a :class:`.Session`.

    The :class:`.Result` object is where the actual :class:`.query.Query`
    object gets created, or retrieved from the cache,
    against a target :class:`.Session`, and is then invoked for results.

    """
    __slots__ = 'bq', 'session', '_params', '_post_criteria'

    def __init__(self, bq, session):
        self.bq = bq
        self.session = session
        self._params = {}
        self._post_criteria = []

    def params(self, *args, **kw):
        """Specify parameters to be replaced into the string SQL statement."""

        if len(args) == 1:
            kw.update(args[0])
        elif len(args) > 0:
            raise sa_exc.ArgumentError(
                "params() takes zero or one positional argument, "
                "which is a dictionary.")
        self._params.update(kw)
        return self

    def _using_post_criteria(self, fns):
        if fns:
            self._post_criteria.extend(fns)
        return self

    def with_post_criteria(self, fn):
        """Add a criteria function that will be applied post-cache.

        This adds a function that will be run against the
        :class:`.Query` object after it is retrieved from the
        cache.    Functions here can be used to alter the query in ways
        that **do not affect the SQL output**, such as execution options
        and shard identifiers (when using a shard-enabled query object)

        .. warning::  :meth:`.Result.with_post_criteria` functions are applied
           to the :class:`.Query` object **after** the query's SQL statement
           object has been retrieved from the cache.   Any operations here
           which intend to modify the SQL should ensure that
           :meth:`.BakedQuery.spoil` was called first.

        .. versionadded:: 1.2


        """
        return self._using_post_criteria([fn])

    def _as_query(self):
        q = self.bq._as_query(self.session).params(self._params)
        for fn in self._post_criteria:
            q = fn(q)
        return q

    def __str__(self):
        return str(self._as_query())

    def __iter__(self):
        bq = self.bq
        if not self.session.enable_baked_queries or bq._spoiled:
            return iter(self._as_query())

        baked_context = bq._bakery.get(bq._cache_key, None)
        if baked_context is None:
            baked_context = bq._bake(self.session)

        context = copy.copy(baked_context)
        context.session = self.session
        context.attributes = context.attributes.copy()

        bq._unbake_subquery_loaders(
            self.session, context, self._params, self._post_criteria)

        context.statement.use_labels = True
        if context.autoflush and not context.populate_existing:
            self.session._autoflush()
        q = context.query.params(self._params).with_session(self.session)
        for fn in self._post_criteria:
            q = fn(q)

        return q._execute_and_instances(context)

    def count(self):
        """return the 'count'.

        Equivalent to :meth:`.Query.count`.

        Note this uses a subquery to ensure an accurate count regardless
        of the structure of the original statement.

        .. versionadded:: 1.1.6

        """

        col = func.count(literal_column('*'))
        bq = self.bq.with_criteria(lambda q: q.from_self(col))
        return bq.for_session(self.session).params(self._params).scalar()

    def scalar(self):
        """Return the first element of the first result or None
        if no rows present.  If multiple rows are returned,
        raises MultipleResultsFound.

        Equivalent to :meth:`.Query.scalar`.

        .. versionadded:: 1.1.6

        """
        try:
            ret = self.one()
            if not isinstance(ret, tuple):
                return ret
            return ret[0]
        except orm_exc.NoResultFound:
            return None

    def first(self):
        """Return the first row.

        Equivalent to :meth:`.Query.first`.

        """
        bq = self.bq.with_criteria(lambda q: q.slice(0, 1))
        ret = list(
            bq.for_session(self.session).params(self._params).
            _using_post_criteria(self._post_criteria))
        if len(ret) > 0:
            return ret[0]
        else:
            return None

    def one(self):
        """Return exactly one result or raise an exception.

        Equivalent to :meth:`.Query.one`.

        """
        try:
            ret = self.one_or_none()
        except orm_exc.MultipleResultsFound:
            raise orm_exc.MultipleResultsFound(
                "Multiple rows were found for one()")
        else:
            if ret is None:
                raise orm_exc.NoResultFound("No row was found for one()")
            return ret

    def one_or_none(self):
        """Return one or zero results, or raise an exception for multiple
        rows.

        Equivalent to :meth:`.Query.one_or_none`.

        .. versionadded:: 1.0.9

        """
        ret = list(self)

        l = len(ret)
        if l == 1:
            return ret[0]
        elif l == 0:
            return None
        else:
            raise orm_exc.MultipleResultsFound(
                "Multiple rows were found for one_or_none()")

    def all(self):
        """Return all rows.

        Equivalent to :meth:`.Query.all`.

        """
        return list(self)

    def get(self, ident):
        """Retrieve an object based on identity.

        Equivalent to :meth:`.Query.get`.

        """

        query = self.bq.steps[0](self.session)
        return query._get_impl(ident, self._load_on_pk_identity)

    def _load_on_pk_identity(self, query, primary_key_identity):
        """Load the given primary key identity from the database."""

        mapper = query._mapper_zero()

        _get_clause, _get_params = mapper._get_clause

        def setup(query):
            _lcl_get_clause = _get_clause
            q = query._clone()
            q._get_condition()
            q._order_by = None

            # None present in ident - turn those comparisons
            # into "IS NULL"
            if None in primary_key_identity:
                nones = set([
                    _get_params[col].key for col, value in
                    zip(mapper.primary_key, primary_key_identity)
                    if value is None
                ])
                _lcl_get_clause = sql_util.adapt_criterion_to_null(
                    _lcl_get_clause, nones)

            _lcl_get_clause = q._adapt_clause(_lcl_get_clause, True, False)
            q._criterion = _lcl_get_clause
            for fn in self._post_criteria:
                q = fn(q)
            return q

        # cache the query against a key that includes
        # which positions in the primary key are NULL
        # (remember, we can map to an OUTER JOIN)
        bq = self.bq

        # add the clause we got from mapper._get_clause to the cache
        # key so that if a race causes multiple calls to _get_clause,
        # we've cached on ours
        bq = bq._clone()
        bq._cache_key += (_get_clause, )

        bq = bq.with_criteria(
            setup, tuple(elem is None for elem in primary_key_identity))

        params = dict([
            (_get_params[primary_key].key, id_val)
            for id_val, primary_key
            in zip(primary_key_identity, mapper.primary_key)
        ])

        result = list(bq.for_session(self.session).params(**params))
        l = len(result)
        if l > 1:
            raise orm_exc.MultipleResultsFound()
        elif l:
            return result[0]
        else:
            return None


@util.deprecated(
    "1.2", "Baked lazy loading is now the default implementation.")
def bake_lazy_loaders():
    """Enable the use of baked queries for all lazyloaders systemwide.

    The "baked" implementation of lazy loading is now the sole implementation
    for the base lazy loader; this method has no effect except for a warning.

    """
    pass


@util.deprecated(
    "1.2", "Baked lazy loading is now the default implementation.")
def unbake_lazy_loaders():
    """Disable the use of baked queries for all lazyloaders systemwide.

    This method now raises NotImplmentedError() as the "baked" implementation
    is the only lazy load implementation.  The
    :paramref:`.relationship.bake_queries` flag may be used to disable
    the caching of queries on a per-relationship basis.

    """
    raise NotImplementedError(
        "Baked lazy loading is now the default implementation")


@strategy_options.loader_option()
def baked_lazyload(loadopt, attr):
    """Indicate that the given attribute should be loaded using "lazy"
    loading with a "baked" query used in the load.

    """
    return loadopt.set_relationship_strategy(attr, {"lazy": "baked_select"})


@baked_lazyload._add_unbound_fn
@util.deprecated(
    "1.2", "Baked lazy loading is now the default "
    "implementation for lazy loading.")
def baked_lazyload(*keys):
    return strategy_options._UnboundLoad._from_keys(
        strategy_options._UnboundLoad.baked_lazyload, keys, False, {})


@baked_lazyload._add_unbound_all_fn
@util.deprecated(
    "1.2", "Baked lazy loading is now the default "
    "implementation for lazy loading.")
def baked_lazyload_all(*keys):
    return strategy_options._UnboundLoad._from_keys(
        strategy_options._UnboundLoad.baked_lazyload, keys, True, {})

baked_lazyload = baked_lazyload._unbound_fn
baked_lazyload_all = baked_lazyload_all._unbound_all_fn

bakery = BakedQuery.bakery
