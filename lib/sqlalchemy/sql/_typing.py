from __future__ import annotations

from typing import Type
from typing import Union

from . import roles
from ..inspection import Inspectable

_ColumnsClauseElement = Union[
    roles.ColumnsClauseRole, Type, Inspectable[roles.HasClauseElement]
]
_FromClauseElement = Union[
    roles.FromClauseRole, Type, Inspectable[roles.HasFromClauseElement]
]
