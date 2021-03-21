from typing import Optional

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class TestBoolean(Base):
    __tablename__ = "test_boolean"

    flag = Column(Boolean)

    bflag: bool = Column(Boolean(create_constraint=True))


expr = TestBoolean.flag.is_(True)

t1 = TestBoolean(flag=True)

x: Optional[bool] = t1.flag

y: bool = t1.bflag
