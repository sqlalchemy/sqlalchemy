import enum
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy.orm import declarative_base


class MyEnum(enum.Enum):
    one = 1
    two = 2
    three = 3


Base = declarative_base()

one, two, three = "one", "two", "three"


class TestEnum(Base):
    __tablename__ = "test_enum"

    e1: str = Column(Enum("one", "two", "three"))

    e2: MyEnum = Column(Enum(MyEnum))

    e3 = Column(Enum(one, two, three))

    e4 = Column(Enum(MyEnum))


t1 = TestEnum(e1="two", e2=MyEnum.three, e3="one", e4=MyEnum.one)

x: str = t1.e1

y: MyEnum = t1.e2

z: Optional[str] = t1.e3

z2: Optional[MyEnum] = t1.e4
