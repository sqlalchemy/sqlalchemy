from __future__ import annotations

import enum
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
    from .result import _KeyMapType
    from .result import _KeyType
    from .result import _ProcessorsType
    from .result import _RawRowType
    from .result import _TupleGetterType
    from .result import ResultMetaData

MD_INDEX = 0  # integer index in cursor.description


class _KeyStyle(enum.Enum):
    KEY_INTEGER_ONLY = 0
    """__getitem__ only allows integer values and slices, raises TypeError
    otherwise"""

    KEY_OBJECTS_ONLY = 1
    """__getitem__ only allows string/object values, raises TypeError
    otherwise"""


KEY_INTEGER_ONLY, KEY_OBJECTS_ONLY = list(_KeyStyle)


class BaseRow:
    __slots__ = ("_parent", "_data", "_keymap", "_key_style")

    _parent: ResultMetaData
    _data: _RawRowType
    _keymap: _KeyMapType
    _key_style: _KeyStyle

    def __init__(
        self,
        parent: ResultMetaData,
        processors: Optional[_ProcessorsType],
        keymap: _KeyMapType,
        key_style: _KeyStyle,
        data: _RawRowType,
    ):
        """Row objects are constructed by CursorResult objects."""
        object.__setattr__(self, "_parent", parent)

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

        object.__setattr__(self, "_keymap", keymap)

        object.__setattr__(self, "_key_style", key_style)

    def __reduce__(self) -> Tuple[Callable[..., BaseRow], Tuple[Any, ...]]:
        return (
            rowproxy_reconstructor,
            (self.__class__, self.__getstate__()),
        )

    def __getstate__(self) -> Dict[str, Any]:
        return {
            "_parent": self._parent,
            "_data": self._data,
            "_key_style": self._key_style,
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        parent = state["_parent"]
        object.__setattr__(self, "_parent", parent)
        object.__setattr__(self, "_data", state["_data"])
        object.__setattr__(self, "_keymap", parent._keymap)
        object.__setattr__(self, "_key_style", state["_key_style"])

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

    def _get_by_key_impl_mapping(self, key: _KeyType) -> Any:
        try:
            rec = self._keymap[key]
        except KeyError as ke:
            rec = self._parent._key_fallback(key, ke)

        mdindex = rec[MD_INDEX]
        if mdindex is None:
            self._parent._raise_for_ambiguous_column_name(rec)
        elif self._key_style == KEY_OBJECTS_ONLY and isinstance(key, int):
            raise KeyError(key)

        return self._data[mdindex]

    def __getattr__(self, name: str) -> Any:
        try:
            return self._get_by_key_impl_mapping(name)
        except KeyError as e:
            raise AttributeError(e.args[0]) from e


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
