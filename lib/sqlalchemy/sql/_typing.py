from __future__ import annotations

from typing import Any
from typing import Mapping
from typing import Sequence
from typing import Type
from typing import Union

from . import roles
from ..inspection import Inspectable
from ..util import immutabledict

_SingleExecuteParams = Mapping[str, Any]
_MultiExecuteParams = Sequence[_SingleExecuteParams]
_ExecuteParams = Union[_SingleExecuteParams, _MultiExecuteParams]
_ExecuteOptions = Mapping[str, Any]
_ImmutableExecuteOptions = immutabledict[str, Any]
_ColumnsClauseElement = Union[
    roles.ColumnsClauseRole, Type, Inspectable[roles.HasClauseElement]
]
_FromClauseElement = Union[
    roles.FromClauseRole, Type, Inspectable[roles.HasFromClauseElement]
]
