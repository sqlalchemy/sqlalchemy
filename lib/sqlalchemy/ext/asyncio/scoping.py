# ext/asyncio/scoping.py
# Copyright (C) 2005-2021 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .session import AsyncSession
from ...orm.scoping import ScopedSessionMixin
from ...util import create_proxy_methods
from ...util import ScopedRegistry


@create_proxy_methods(
    AsyncSession,
    ":class:`_asyncio.AsyncSession`",
    ":class:`_asyncio.scoping.async_scoped_session`",
    classmethods=["close_all", "object_session", "identity_key"],
    methods=[
        "__contains__",
        "__iter__",
        "add",
        "add_all",
        "begin",
        "begin_nested",
        "close",
        "commit",
        "connection",
        "delete",
        "execute",
        "expire",
        "expire_all",
        "expunge",
        "expunge_all",
        "flush",
        "get",
        "get_bind",
        "is_modified",
        "merge",
        "refresh",
        "rollback",
        "scalar",
    ],
    attributes=[
        "bind",
        "dirty",
        "deleted",
        "new",
        "identity_map",
        "is_active",
        "autoflush",
        "no_autoflush",
        "info",
    ],
)
class async_scoped_session(ScopedSessionMixin):
    """Provides scoped management of :class:`.AsyncSession` objects."""

    def __init__(self, session_factory, scopefunc):
        """Construct a new :class:`.scoped_session`.

        :param session_factory: a factory to create new :class:`.Session`
         instances. This is usually, but not necessarily, an instance
         of :class:`.sessionmaker`.
        :param scopefunc: function which defines
         the current scope. Different from scoped_session's __init__ signature,
         which has a default 'thread-local' scope, the scopefunc must be passed
         in and it should return a hashable token with the same requirement as
         in scoped_session.

        """
        self.session_factory = session_factory
        self.registry = ScopedRegistry(session_factory, scopefunc)

    @property
    def _proxied(self):
        return self.registry()

    async def remove(self):
        """Dispose of the current :class:`.AsyncSession`, if present.

        Different from scoped_session's remove method, this method would use
        await to wait for the close method of AsyncSession.

        """

        if self.registry.has():
            await self.registry().close()
        self.registry.clear()
