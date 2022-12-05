# ext/horizontal_shard.py
# Copyright (C) 2005-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

"""Horizontal sharding support.

Defines a rudimental 'horizontal sharding' system which allows a Session to
distribute queries and persistence operations across multiple databases.

For a usage example, see the :ref:`examples_sharding` example included in
the source distribution.

"""
from __future__ import annotations

from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import Union

from .. import event
from .. import exc
from .. import inspect
from .. import util
from ..engine.base import Connection
from ..engine.base import Engine
from ..engine.base import OptionEngine
from ..engine.result import IteratorResult
from ..engine.result import Result
from ..orm import LoaderCallableStatus
from ..orm import PassiveFlag
from ..orm.bulk_persistence import BulkUDCompileState
from ..orm.context import QueryContext
from ..orm.mapper import Mapper
from ..orm.query import Query
from ..orm.session import _SessionBind
from ..orm.session import _SessionBindKey
from ..orm.session import ORMExecuteState
from ..orm.session import Session
from ..orm.state import InstanceState
from ..sql._typing import _TP
from ..sql.base import _MetaOptions
from ..sql.elements import ClauseElement


__all__ = ["ShardedSession", "ShardedQuery"]

_T = TypeVar("_T", bound=Any)

SelfShardedQuery = TypeVar("SelfShardedQuery", bound="ShardedQuery[Any]")


class ShardedQuery(Query[_T]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.id_chooser = self.session.id_chooser
        self.query_chooser = self.session.query_chooser
        self.execute_chooser = self.session.execute_chooser
        self._shard_id = None

    def set_shard(self: SelfShardedQuery, shard_id: str) -> SelfShardedQuery:
        """Return a new query, limited to a single shard ID.

        All subsequent operations with the returned query will
        be against the single shard regardless of other state.

        The shard_id can be passed for a 2.0 style execution to the
        bind_arguments dictionary of :meth:`.Session.execute`::

            results = session.execute(
                stmt,
                bind_arguments={"shard_id": "my_shard"}
            )

        """
        return self.execution_options(_sa_shard_id=shard_id)


class ShardedSession(Session):
    def __init__(
        self,
        shard_chooser: Callable[
            [Mapper[_T], Any, Optional[ClauseElement]], Any
        ],
        id_chooser: Callable[[Query[_T], Iterable[_T]], Iterable[Any]],
        execute_chooser: Optional[
            Callable[[ORMExecuteState], Iterable[Any]]
        ] = None,
        shards: Optional[Dict[str, Any]] = None,
        query_cls: Type[Query[_T]] = ShardedQuery,
        **kwargs: Any,
    ) -> None:
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

          .. versionchanged:: 1.4  The ``execute_chooser`` parameter
             supersedes the ``query_chooser`` parameter.

        :param shards: A dictionary of string shard names
          to :class:`~sqlalchemy.engine.Engine` objects.

        """
        query_chooser = kwargs.pop("query_chooser", None)
        super().__init__(query_cls=query_cls, **kwargs)

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

            def execute_chooser(orm_context: ORMExecuteState) -> Any:
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
        mapper: Mapper[_T],
        primary_key_identity: Union[Any, Tuple[Any, ...]],
        identity_token: Optional[Any] = None,
        passive: PassiveFlag = PassiveFlag.PASSIVE_OFF,
        lazy_loaded_from: Optional[InstanceState[Any]] = None,
        **kw: Any,
    ) -> Union[Optional[_T], LoaderCallableStatus]:
        """override the default :meth:`.Session._identity_lookup` method so
        that we search for a given non-token primary key identity across all
        possible identity tokens (e.g. shard ids).

        .. versionchanged:: 1.4  Moved :meth:`.Session._identity_lookup` from
           the :class:`_query.Query` object to the :class:`.Session`.

        """

        if identity_token is not None:
            return super()._identity_lookup(
                mapper,
                primary_key_identity,
                identity_token=identity_token,
                **kw,
            )
        else:
            q = self.query(mapper)
            if lazy_loaded_from:
                q = q._set_lazyload_from(lazy_loaded_from)
            for shard_id in self.id_chooser(q, primary_key_identity):
                obj = super()._identity_lookup(
                    mapper,
                    primary_key_identity,
                    identity_token=shard_id,
                    lazy_loaded_from=lazy_loaded_from,
                    **kw,
                )
                if obj is not None:
                    return obj

            return None

    def _choose_shard_and_assign(
        self,
        mapper: Mapper[_T],
        instance: Any,
        **kw: Any,
    ) -> Any:
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
        self,
        mapper: Optional[Mapper[_T]] = None,
        instance: Optional[Any] = None,
        shard_id: Optional[Any] = None,
        **kwargs: Any,
    ) -> Connection:
        """Provide a :class:`_engine.Connection` to use in the unit of work
        flush process.

        """

        if shard_id is None:
            shard_id = self._choose_shard_and_assign(mapper, instance)

        if self.in_transaction():
            return self.get_transaction().connection(mapper, shard_id=shard_id)  # type: ignore [union-attr] # noqa: E501
        else:
            return self.get_bind(
                mapper, shard_id=shard_id, instance=instance
            ).connect(**kwargs)

    def get_bind(
        self,
        mapper: Optional[Mapper[_T]] = None,
        shard_id: Optional[_SessionBindKey] = None,
        instance: Optional[Any] = None,
        clause: Optional[ClauseElement] = None,
        **kw: Any,
    ) -> _SessionBind:
        if shard_id is None:
            shard_id = self._choose_shard_and_assign(
                mapper, instance, clause=clause
            )
        return self.__binds[shard_id]

    def bind_shard(
        self, shard_id: str, bind: Union[Engine, OptionEngine]
    ) -> None:
        self.__binds[shard_id] = bind


def execute_and_instances(
    orm_context: ORMExecuteState,
) -> Union[Result[_T], IteratorResult[_TP]]:
    if orm_context.is_select:
        load_options = active_options = orm_context.load_options
        update_options = None

    elif orm_context.is_update or orm_context.is_delete:
        load_options = None
        update_options = active_options = orm_context.update_delete_options
    else:
        load_options = update_options = active_options = None

    session = orm_context.session

    def iter_for_shard(
        shard_id: str,
        load_options: Union[
            None, QueryContext.default_load_options, _MetaOptions
        ],
        update_options: Optional[BulkUDCompileState.default_update_options],
    ) -> Union[Result[_T], IteratorResult[_TP]]:
        execution_options = dict(orm_context.local_execution_options)

        bind_arguments = dict(orm_context.bind_arguments)
        bind_arguments["shard_id"] = shard_id

        if orm_context.is_select:
            load_options += {"_refresh_identity_token": shard_id}  # type: ignore [operator] # noqa: E501
            execution_options["_sa_orm_load_options"] = load_options
        elif orm_context.is_update or orm_context.is_delete:
            update_options += {"_refresh_identity_token": shard_id}  # type: ignore [operator] # noqa: E501
            execution_options["_sa_orm_update_options"] = update_options

        return orm_context.invoke_statement(
            bind_arguments=bind_arguments, execution_options=execution_options
        )

    if active_options and active_options._refresh_identity_token is not None:
        shard_id = active_options._refresh_identity_token
    elif "_sa_shard_id" in orm_context.execution_options:
        shard_id = orm_context.execution_options["_sa_shard_id"]
    elif "shard_id" in orm_context.bind_arguments:
        shard_id = orm_context.bind_arguments["shard_id"]
    else:
        shard_id = None

    if shard_id is not None:
        return iter_for_shard(shard_id, load_options, update_options)
    else:
        partial = []
        for shard_id in session.execute_chooser(orm_context):
            result_ = iter_for_shard(shard_id, load_options, update_options)
            partial.append(result_)
        return partial[0].merge(*partial[1:])
