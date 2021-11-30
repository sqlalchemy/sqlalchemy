from typing import TypeVar, Generic, Type
from sqlalchemy import Column, Integer
from sqlalchemy.orm import as_declarative


def test_class_getitem():
    T = TypeVar("T", bound="CommonBase")

    class CommonBase(Generic[T]):
        @classmethod
        def boring(cls: Type[T]) -> Type[T]:
            return cls

    @as_declarative()
    class Base(CommonBase[T]):
        pass

    class Tab(Base["Tab"]):
        a = Column(Integer, primary_key=True)
