from typing import Generic
from typing import Type
from typing import TypeVar

from sqlalchemy import Column
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy.orm import as_declarative
from sqlalchemy.testing import eq_
from sqlalchemy.testing import fixtures
from sqlalchemy.testing import is_
from sqlalchemy.testing.assertions import expect_raises


class DeclarativeBaseTest(fixtures.TestBase):
    __requires__ = ("python37",)

    def test_class_getitem(self):
        T = TypeVar("T", bound="CommonBase")  # noqa

        class CommonBase(Generic[T]):
            @classmethod
            def boring(cls: Type[T]) -> Type[T]:
                return cls

            @classmethod
            def more_boring(cls: Type[T]) -> int:
                return 27

        @as_declarative()
        class Base(CommonBase[T]):
            foo = 1

        class Tab(Base["Tab"]):
            __tablename__ = "foo"
            a = Column(Integer, primary_key=True)

        eq_(Tab.foo, 1)
        is_(Tab.__table__, inspect(Tab).local_table)
        eq_(Tab.boring(), Tab)
        eq_(Tab.more_boring(), 27)

        with expect_raises(AttributeError):
            Tab.non_existent
