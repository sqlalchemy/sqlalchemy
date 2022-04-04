from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Union


if TYPE_CHECKING:
    from .mapper import Mapper
    from .util import AliasedInsp

_EntityType = Union[Mapper, AliasedInsp]
