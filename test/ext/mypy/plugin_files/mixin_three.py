from typing import Callable

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import deferred
from sqlalchemy.orm import Mapped
from sqlalchemy.orm.decl_api import declarative_mixin
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.orm.interfaces import MapperProperty


def some_other_decorator(fn: Callable[..., None]) -> Callable[..., None]:
    return fn


@declarative_mixin
class HasAMixin:
    x: Mapped[int] = Column(Integer)

    y = Column(String)

    @declared_attr
    def data(cls) -> Column[String]:
        return Column(String)

    @declared_attr
    def data2(cls) -> MapperProperty[str]:
        return deferred(Column(String))

    @some_other_decorator
    def q(cls) -> None:
        return None
