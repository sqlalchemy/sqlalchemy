# ext/asyncio/events.py
# Copyright (C) 2020-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from .engine import AsyncConnectable
from .session import AsyncSession
from ...engine import events as engine_event
from ...orm import events as orm_event


class AsyncConnectionEvents(engine_event.ConnectionEvents):
    _target_class_doc = "SomeEngine"
    _dispatch_target = AsyncConnectable

    @classmethod
    def _listen(cls, event_key, retval=False):
        raise NotImplementedError(
            "asynchronous events are not implemented at this time.  Apply "
            "synchronous listeners to the AsyncEngine.sync_engine or "
            "AsyncConnection.sync_connection attributes."
        )


class AsyncSessionEvents(orm_event.SessionEvents):
    _target_class_doc = "SomeSession"
    _dispatch_target = AsyncSession

    @classmethod
    def _listen(cls, event_key, retval=False):
        raise NotImplementedError(
            "asynchronous events are not implemented at this time.  Apply "
            "synchronous listeners to the AsyncSession.sync_session."
        )
