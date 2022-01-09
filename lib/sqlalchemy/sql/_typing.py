from typing import Any
from typing import Mapping
from typing import Sequence
from typing import Union

_SingleExecuteParams = Mapping[str, Any]
_MultiExecuteParams = Sequence[_SingleExecuteParams]
_ExecuteParams = Union[_SingleExecuteParams, _MultiExecuteParams]
_ExecuteOptions = Mapping[str, Any]
