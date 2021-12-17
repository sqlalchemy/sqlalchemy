from typing import Generic
from typing import Type
from typing import TypeVar

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import as_declarative
from sqlalchemy.testing import fixtures


class DeclarativeBaseTest(fixtures.TestBase):
    def test_class_getitem(self):
        T = TypeVar("T", bound="CommonBase")  # noqa

        class CommonBase(Generic[T]):
            @classmethod
            def boring(cls: Type[T]) -> Type[T]:
                return cls

        @as_declarative()
        class Base(CommonBase[T]):
            foo = 1

        class Tab(Base["Tab"]):
            __tablename__ = "foo"
            a = Column(Integer, primary_key=True)

        assert Tab.foo == 1
        assert Tab.__table__ is not None
        assert Tab.boring() == Tab
