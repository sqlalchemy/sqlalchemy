from __future__ import annotations

from typing import Any
from typing import Iterable
from typing import Mapping
from typing import Optional
from typing import Union

from ..sql._typing import _DDLColumnArgument
from ..sql.elements import DQLDMLClauseElement
from ..sql.schema import ColumnCollectionConstraint
from ..sql.schema import Index


_OnConflictConstraintT = Union[str, ColumnCollectionConstraint, Index, None]
_OnConflictIndexElementsT = Optional[Iterable[_DDLColumnArgument]]
_OnConflictIndexWhereT = Optional[DQLDMLClauseElement]
_OnConflictSetT = Optional[Mapping[Any, Any]]
_OnConflictWhereT = Union[DQLDMLClauseElement, str, None]
