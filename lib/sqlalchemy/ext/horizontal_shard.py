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

from .. import event
from .. import exc
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
        self.execute_chooser = self.session.execute_chooser
        self._shard_id = None

    def set_shard(self, shard_id):
        """return a new query, limited to a single shard ID.

        all subsequent operations with the returned query will
        be against the single shard regardless of other state.

        The shard_id can be passed for a 2.0 style execution to the
        bind_arguments dictionary of :meth:`.Session.execute`::

            results = session.execute(
                stmt,
                bind_arguments={"shard_id": "my_shard"}
            )

        """
        return self.execution_options(_sa_shard_id=shard_id)

    def _execute_crud(self, stmt, mapper):
        def exec_for_shard(shard_id):
            conn = self.session.connection(
                mapper=mapper,
                shard_id=shard_id,
                clause=stmt,
                close_with_result=True,
            )
            result = conn._execute_20(
                stmt, self.load_options._params, self._execution_options
            )
            return result

        if self._shard_id is not None:
            return exec_for_shard(self._shard_id)
        else:
            rowcount = 0
            results = []
            # TODO: this will have to be the new object
            for shard_id in self.execute_chooser(self):
                result = exec_for_shard(shard_id)
                rowcount += result.rowcount
                results.append(result)

            return ShardedResult(results, rowcount)


class ShardedResult(object):
    """A value object that represents multiple :class:`_engine.CursorResult`
    objects.

    This is used by the :meth:`.ShardedQuery._execute_crud` hook to return
    an object that takes the place of the single :class:`_engine.CursorResult`.

    Attribute include ``result_proxies``, which is a sequence of the
    actual :class:`_engine.CursorResult` objects,
    as well as ``aggregate_rowcount``
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
        execute_chooser=None,
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

        :param execute_chooser: For a given :class:`.ORMExecuteState`,
          returns the list of shard_ids
          where the query should be issued.  Results from all shards returned
          will be combined together into a single listing.

          .. versionchanged:: 1.4  The ``execute_chooser`` paramter
             supersedes the ``query_chooser`` parameter.

        :param shards: A dictionary of string shard names
          to :class:`~sqlalchemy.engine.Engine` objects.

        """
        query_chooser = kwargs.pop("query_chooser", None)
        super(ShardedSession, self).__init__(query_cls=query_cls, **kwargs)

        event.listen(
            self, "do_orm_execute", execute_and_instances, retval=True
        )
        self.shard_chooser = shard_chooser
        self.id_chooser = id_chooser

        if query_chooser:
            util.warn_deprecated(
                "The ``query_choser`` parameter is deprecated; "
                "please use ``execute_chooser``.",
                "1.4",
            )
            if execute_chooser:
                raise exc.ArgumentError(
                    "Can't pass query_chooser and execute_chooser "
                    "at the same time."
                )

            def execute_chooser(orm_context):
                return query_chooser(orm_context.statement)

            self.execute_chooser = execute_chooser
        else:
            self.execute_chooser = execute_chooser
        self.query_chooser = query_chooser
        self.__binds = {}
        if shards is not None:
            for k in shards:
                self.bind_shard(k, shards[k])

    def _identity_lookup(
        self,
        mapper,
        primary_key_identity,
        identity_token=None,
        lazy_loaded_from=None,
        **kw
    ):
        """override the default :meth:`.Session._identity_lookup` method so
        that we search for a given non-token primary key identity across all
        possible identity tokens (e.g. shard ids).

        .. versionchanged:: 1.4  Moved :meth:`.Session._identity_lookup` from
           the :class:`_query.Query` object to the :class:`.Session`.

        """

        if identity_token is not None:
            return super(ShardedSession, self)._identity_lookup(
                mapper,
                primary_key_identity,
                identity_token=identity_token,
                **kw
            )
        else:
            q = self.query(mapper)
            if lazy_loaded_from:
                q = q._set_lazyload_from(lazy_loaded_from)
            for shard_id in self.id_chooser(q, primary_key_identity):
                obj = super(ShardedSession, self)._identity_lookup(
                    mapper,
                    primary_key_identity,
                    identity_token=shard_id,
                    lazy_loaded_from=lazy_loaded_from,
                    **kw
                )
                if obj is not None:
                    return obj

            return None

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

    def connection_callable(
        self, mapper=None, instance=None, shard_id=None, **kwargs
    ):
        """Provide a :class:`_engine.Connection` to use in the unit of work
        flush process.

        """

        if shard_id is None:
            shard_id = self._choose_shard_and_assign(mapper, instance)

        if self.transaction is not None:
            return self.transaction.connection(mapper, shard_id=shard_id)
        else:
            return self.get_bind(
                mapper, shard_id=shard_id, instance=instance
            ).connect(**kwargs)

    def get_bind(
        self, mapper=None, shard_id=None, instance=None, clause=None, **kw
    ):
        if shard_id is None:
            shard_id = self._choose_shard_and_assign(
                mapper, instance, clause=clause
            )
        return self.__binds[shard_id]

    def bind_shard(self, shard_id, bind):
        self.__binds[shard_id] = bind


def execute_and_instances(orm_context):
    if orm_context.bind_arguments.get("_horizontal_shard", False):
        return None

    params = orm_context.parameters

    load_options = orm_context.load_options
    session = orm_context.session
    # orm_query = orm_context.orm_query

    if params is None:
        params = load_options._params

    def iter_for_shard(shard_id, load_options):
        execution_options = dict(orm_context.local_execution_options)

        bind_arguments = dict(orm_context.bind_arguments)
        bind_arguments["_horizontal_shard"] = True
        bind_arguments["shard_id"] = shard_id

        load_options += {"_refresh_identity_token": shard_id}
        execution_options["_sa_orm_load_options"] = load_options

        return session.execute(
            orm_context.statement,
            orm_context.parameters,
            execution_options,
            bind_arguments,
        )

    if load_options._refresh_identity_token is not None:
        shard_id = load_options._refresh_identity_token
    elif "_sa_shard_id" in orm_context.merged_execution_options:
        shard_id = orm_context.merged_execution_options["_sa_shard_id"]
    elif "shard_id" in orm_context.bind_arguments:
        shard_id = orm_context.bind_arguments["shard_id"]
    else:
        shard_id = None

    if shard_id is not None:
        return iter_for_shard(shard_id, load_options)
    else:
        partial = []
        for shard_id in session.execute_chooser(orm_context):
            result_ = iter_for_shard(shard_id, load_options)
            partial.append(result_)

        return partial[0].merge(*partial[1:])
