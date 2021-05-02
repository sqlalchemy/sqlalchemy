import abc

from . import exc as async_exc


class StartableContext(abc.ABC):
    @abc.abstractmethod
    async def start(self, is_ctxmanager=False) -> "StartableContext":
        pass

    def __await__(self):
        return self.start().__await__()

    async def __aenter__(self):
        return await self.start(is_ctxmanager=True)

    @abc.abstractmethod
    async def __aexit__(self, type_, value, traceback):
        pass

    def _raise_for_not_started(self):
        raise async_exc.AsyncContextNotStarted(
            "%s context has not been started and object has not been awaited."
            % (self.__class__.__name__)
        )


class ProxyComparable:
    def __hash__(self):
        return id(self)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self._proxied == other._proxied
        )

    def __ne__(self, other):
        return (
            not isinstance(other, self.__class__)
            or self._proxied != other._proxied
        )
