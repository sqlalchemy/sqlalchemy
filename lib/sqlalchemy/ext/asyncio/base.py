# ext/asyncio/base.py
# Copyright (C) 2020-2022 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php

from __future__ import annotations

import abc
import functools
from typing import Any
from typing import ClassVar
from typing import Dict
from typing import Generic
from typing import NoReturn
from typing import Optional
from typing import overload
from typing import Type
from typing import TypeVar
import weakref

from . import exc as async_exc
from ... import util
from ...util.typing import Literal

_T = TypeVar("_T", bound=Any)


_PT = TypeVar("_PT", bound=Any)


SelfReversibleProxy = TypeVar(
    "SelfReversibleProxy", bound="ReversibleProxy[Any]"
)


class ReversibleProxy(Generic[_PT]):
    _proxy_objects: ClassVar[
        Dict[weakref.ref[Any], weakref.ref[ReversibleProxy[Any]]]
    ] = {}
    __slots__ = ("__weakref__",)

    @overload
    def _assign_proxied(self, target: _PT) -> _PT:
        ...

    @overload
    def _assign_proxied(self, target: None) -> None:
        ...

    def _assign_proxied(self, target: Optional[_PT]) -> Optional[_PT]:
        if target is not None:
            target_ref: weakref.ref[_PT] = weakref.ref(
                target, ReversibleProxy._target_gced
            )
            proxy_ref = weakref.ref(
                self,
                functools.partial(  # type: ignore
                    ReversibleProxy._target_gced, target_ref
                ),
            )
            ReversibleProxy._proxy_objects[target_ref] = proxy_ref

        return target

    @classmethod
    def _target_gced(
        cls: Type[SelfReversibleProxy],
        ref: weakref.ref[_PT],
        proxy_ref: Optional[weakref.ref[SelfReversibleProxy]] = None,
    ) -> None:
        cls._proxy_objects.pop(ref, None)

    @classmethod
    def _regenerate_proxy_for_target(
        cls: Type[SelfReversibleProxy], target: _PT
    ) -> SelfReversibleProxy:
        raise NotImplementedError()

    @overload
    @classmethod
    def _retrieve_proxy_for_target(
        cls: Type[SelfReversibleProxy],
        target: _PT,
        regenerate: Literal[True] = ...,
    ) -> SelfReversibleProxy:
        ...

    @overload
    @classmethod
    def _retrieve_proxy_for_target(
        cls: Type[SelfReversibleProxy], target: _PT, regenerate: bool = True
    ) -> Optional[SelfReversibleProxy]:
        ...

    @classmethod
    def _retrieve_proxy_for_target(
        cls: Type[SelfReversibleProxy], target: _PT, regenerate: bool = True
    ) -> Optional[SelfReversibleProxy]:
        try:
            proxy_ref = cls._proxy_objects[weakref.ref(target)]
        except KeyError:
            pass
        else:
            proxy = proxy_ref()
            if proxy is not None:
                return proxy  # type: ignore

        if regenerate:
            return cls._regenerate_proxy_for_target(target)
        else:
            return None


SelfStartableContext = TypeVar(
    "SelfStartableContext", bound="StartableContext"
)


class StartableContext(abc.ABC):
    __slots__ = ()

    @abc.abstractmethod
    async def start(
        self: SelfStartableContext, is_ctxmanager: bool = False
    ) -> Any:
        raise NotImplementedError()

    def __await__(self) -> Any:
        return self.start().__await__()

    async def __aenter__(self: SelfStartableContext) -> Any:
        return await self.start(is_ctxmanager=True)

    @abc.abstractmethod
    async def __aexit__(self, type_: Any, value: Any, traceback: Any) -> None:
        pass

    def _raise_for_not_started(self) -> NoReturn:
        raise async_exc.AsyncContextNotStarted(
            "%s context has not been started and object has not been awaited."
            % (self.__class__.__name__)
        )


class ProxyComparable(ReversibleProxy[_PT]):
    __slots__ = ()

    @util.ro_non_memoized_property
    def _proxied(self) -> _PT:
        raise NotImplementedError()

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, self.__class__)
            and self._proxied == other._proxied
        )

    def __ne__(self, other: Any) -> bool:
        return (
            not isinstance(other, self.__class__)
            or self._proxied != other._proxied
        )
