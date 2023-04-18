from __future__ import annotations

import operator
import typing
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import Union

if typing.TYPE_CHECKING:
    from .result import _KeyMapRecType
    from .result import _KeyType
    from .result import _ProcessorsType
    from .result import _RawRowType
    from .result import _TupleGetterType
    from .result import ResultMetaData

MD_INDEX = 0  # integer index in cursor.description

_MISSING_SENTINEL = object()


class BaseRow:
    __slots__ = ("_parent", "_data", "_keymap_by_str")

    _parent: ResultMetaData
    _keymap_by_str: Dict[_KeyType, _KeyMapRecType]
    _data: _RawRowType

    def __init__(
        self,
        parent: ResultMetaData,
        processors: Optional[_ProcessorsType],
        data: _RawRowType,
    ):
        """Row objects are constructed by CursorResult objects."""
        object.__setattr__(self, "_parent", parent)

        object.__setattr__(self, "_keymap_by_str", parent._keymap_by_str)

        if processors:
            object.__setattr__(
                self,
                "_data",
                tuple(
                    [
                        proc(value) if proc else value
                        for proc, value in zip(processors, data)
                    ]
                ),
            )
        else:
            object.__setattr__(self, "_data", tuple(data))

    def __reduce__(self) -> Tuple[Callable[..., BaseRow], Tuple[Any, ...]]:
        return (
            rowproxy_reconstructor,
            (self.__class__, self.__getstate__()),
        )

    def __getstate__(self) -> Dict[str, Any]:
        return {
            "_parent": self._parent,
            "_data": self._data,
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        parent = state["_parent"]
        object.__setattr__(self, "_parent", parent)
        object.__setattr__(self, "_data", state["_data"])
        object.__setattr__(self, "_keymap_by_str", parent._keymap_by_str)

    def _values_impl(self) -> List[Any]:
        return list(self)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __hash__(self) -> int:
        return hash(self._data)

    def _get_by_int_impl(self, key: Union[int, slice]) -> Any:
        return self._data[key]

    if not typing.TYPE_CHECKING:
        __getitem__ = _get_by_int_impl

    def _get_by_key_impl_mapping(self, key):
        cached_index = self._keymap_by_str.get(key, _MISSING_SENTINEL)
        if cached_index is not _MISSING_SENTINEL and cached_index is not None:
            return self._data[cached_index]
        if cached_index is _MISSING_SENTINEL:
            self._parent._key_fallback(key, KeyError(key))
        self._parent._raise_for_ambiguous_column_name(
            self._parent._keymap[key]
        )

    def __getattr__(self, name: str) -> Any:
        cached_index = self._keymap_by_str.get(name, _MISSING_SENTINEL)
        if cached_index is not _MISSING_SENTINEL and cached_index is not None:
            return self._data[cached_index]
        if cached_index is _MISSING_SENTINEL:
            try:
                self._parent._key_fallback(name, KeyError(name))
            except KeyError as e:
                raise AttributeError(e.args[0]) from e
        self._parent._raise_for_ambiguous_column_name(
            self._parent._keymap[name]
        )


# This reconstructor is necessary so that pickles with the Cy extension or
# without use the same Binary format.
def rowproxy_reconstructor(
    cls: Type[BaseRow], state: Dict[str, Any]
) -> BaseRow:
    obj = cls.__new__(cls)
    obj.__setstate__(state)
    return obj


def tuplegetter(*indexes: int) -> _TupleGetterType:
    it = operator.itemgetter(*indexes)

    if len(indexes) > 1:
        return it
    else:
        return lambda row: (it(row),)
