from typing import Generic
from typing import Type
from typing import TypeVar

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import as_declarative


def test_class_getitem():
    T = TypeVar("T", bound="CommonBase")  # noqa

    class CommonBase(Generic[T]):
        @classmethod
        def boring(cls: Type[T]) -> Type[T]:
            return cls

    @as_declarative()
    class Base(CommonBase[T]):
        pass

    class Tab(Base["Tab"]):
        a = Column(Integer, primary_key=True)
