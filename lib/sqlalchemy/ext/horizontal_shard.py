# ext/horizontal_shard.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Horizontal sharding support.

Defines a rudimental 'horizontal sharding' system which allows a Session to
distribute queries and persistence operations across multiple databases.

For a usage example, see the :ref:`examples_sharding` example included in
the source distribution.

"""

from .. import inspect
from .. import util
from ..orm.query import Query
from ..orm.session import Session


__all__ = ["ShardedSession", "ShardedQuery"]


class ShardedQuery(Query):
    def __init__(self, *args, **kwargs):
        super(ShardedQuery, self).__init__(*args, **kwargs)
        self.id_chooser = self.session.id_chooser
        self.query_chooser = self.session.query_chooser
        self._shard_id = None

    def set_shard(self, shard_id):
        """return a new query, limited to a single shard ID.

        all subsequent operations with the returned query will
        be against the single shard regardless of other state.
        """

        q = self._clone()
        q._shard_id = shard_id
        return q

    def _execute_and_instances(self, context):
        def iter_for_shard(shard_id):
            context.attributes["shard_id"] = context.identity_token = shard_id
            result = self._connection_from_session(
                mapper=self._bind_mapper(), shard_id=shard_id
            ).execute(context.statement, self._params)
            return self.instances(result, context)

        if context.identity_token is not None:
            return iter_for_shard(context.identity_token)
        elif self._shard_id is not None:
            return iter_for_shard(self._shard_id)
        else:
            partial = []
            for shard_id in self.query_chooser(self):
                partial.extend(iter_for_shard(shard_id))

            # if some kind of in memory 'sorting'
            # were done, this is where it would happen
            return iter(partial)

    def _execute_crud(self, stmt, mapper):
        def exec_for_shard(shard_id):
            conn = self._connection_from_session(
                mapper=mapper,
                shard_id=shard_id,
                clause=stmt,
                close_with_result=True,
            )
            result = conn.execute(stmt, self._params)
            return result

        if self._shard_id is not None:
            return exec_for_shard(self._shard_id)
        else:
            rowcount = 0
            results = []
            for shard_id in self.query_chooser(self):
                result = exec_for_shard(shard_id)
                rowcount += result.rowcount
                results.append(result)

            return ShardedResult(results, rowcount)

    def _identity_lookup(
        self,
        mapper,
        primary_key_identity,
        identity_token=None,
        lazy_loaded_from=None,
        **kw
    ):
        """override the default Query._identity_lookup method so that we
        search for a given non-token primary key identity across all
        possible identity tokens (e.g. shard ids).

        """

        if identity_token is not None:
            return super(ShardedQuery, self)._identity_lookup(
                mapper,
                primary_key_identity,
                identity_token=identity_token,
                **kw
            )
        else:
            q = self.session.query(mapper)
            if lazy_loaded_from:
                q = q._set_lazyload_from(lazy_loaded_from)
            for shard_id in self.id_chooser(q, primary_key_identity):
                obj = super(ShardedQuery, self)._identity_lookup(
                    mapper, primary_key_identity, identity_token=shard_id, **kw
                )
                if obj is not None:
                    return obj

            return None

    def _get_impl(self, primary_key_identity, db_load_fn, identity_token=None):
        """Override the default Query._get_impl() method so that we emit
        a query to the DB for each possible identity token, if we don't
        have one already.

        """

        def _db_load_fn(query, primary_key_identity):
            # load from the database.  The original db_load_fn will
            # use the given Query object to load from the DB, so our
            # shard_id is what will indicate the DB that we query from.
            if self._shard_id is not None:
                return db_load_fn(self, primary_key_identity)
            else:
                ident = util.to_list(primary_key_identity)
                # build a ShardedQuery for each shard identifier and
                # try to load from the DB
                for shard_id in self.id_chooser(self, ident):
                    q = self.set_shard(shard_id)
                    o = db_load_fn(q, ident)
                    if o is not None:
                        return o
                else:
                    return None

        if identity_token is None and self._shard_id is not None:
            identity_token = self._shard_id

        return super(ShardedQuery, self)._get_impl(
            primary_key_identity, _db_load_fn, identity_token=identity_token
        )


class ShardedResult(object):
    """A value object that represents multiple :class:`.ResultProxy` objects.

    This is used by the :meth:`.ShardedQuery._execute_crud` hook to return
    an object that takes the place of the single :class:`.ResultProxy`.

    Attribute include ``result_proxies``, which is a sequence of the
    actual :class:`.ResultProxy` objects, as well as ``aggregate_rowcount``
    or ``rowcount``, which is the sum of all the individual rowcount values.

    .. versionadded::  1.3
    """

    __slots__ = ("result_proxies", "aggregate_rowcount")

    def __init__(self, result_proxies, aggregate_rowcount):
        self.result_proxies = result_proxies
        self.aggregate_rowcount = aggregate_rowcount

    @property
    def rowcount(self):
        return self.aggregate_rowcount


class ShardedSession(Session):
    def __init__(
        self,
        shard_chooser,
        id_chooser,
        query_chooser,
        shards=None,
        query_cls=ShardedQuery,
        **kwargs
    ):
        """Construct a ShardedSession.

        :param shard_chooser: A callable which, passed a Mapper, a mapped
          instance, and possibly a SQL clause, returns a shard ID.  This id
          may be based off of the attributes present within the object, or on
          some round-robin scheme. If the scheme is based on a selection, it
          should set whatever state on the instance to mark it in the future as
          participating in that shard.

        :param id_chooser: A callable, passed a query and a tuple of identity
          values, which should return a list of shard ids where the ID might
          reside.  The databases will be queried in the order of this listing.

        :param query_chooser: For a given Query, returns the list of shard_ids
          where the query should be issued.  Results from all shards returned
          will be combined together into a single listing.

        :param shards: A dictionary of string shard names
          to :class:`~sqlalchemy.engine.Engine` objects.

        """
        super(ShardedSession, self).__init__(query_cls=query_cls, **kwargs)
        self.shard_chooser = shard_chooser
        self.id_chooser = id_chooser
        self.query_chooser = query_chooser
        self.__binds = {}
        self.connection_callable = self.connection
        if shards is not None:
            for k in shards:
                self.bind_shard(k, shards[k])

    def _choose_shard_and_assign(self, mapper, instance, **kw):
        if instance is not None:
            state = inspect(instance)
            if state.key:
                token = state.key[2]
                assert token is not None
                return token
            elif state.identity_token:
                return state.identity_token

        shard_id = self.shard_chooser(mapper, instance, **kw)
        if instance is not None:
            state.identity_token = shard_id
        return shard_id

    def connection(self, mapper=None, instance=None, shard_id=None, **kwargs):
        if shard_id is None:
            shard_id = self._choose_shard_and_assign(mapper, instance)

        if self.transaction is not None:
            return self.transaction.connection(mapper, shard_id=shard_id)
        else:
            return self.get_bind(
                mapper, shard_id=shard_id, instance=instance
            )._contextual_connect(**kwargs)

    def get_bind(
        self, mapper, shard_id=None, instance=None, clause=None, **kw
    ):
        if shard_id is None:
            shard_id = self._choose_shard_and_assign(
                mapper, instance, clause=clause
            )
        return self.__binds[shard_id]

    def bind_shard(self, shard_id, bind):
        self.__binds[shard_id] = bind
