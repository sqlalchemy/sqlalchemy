from typing import Type, Any

from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.type_api import TypeEngine

col: ColumnElement
type_: Type[TypeEngine[Any]]

col.cast(type_)
