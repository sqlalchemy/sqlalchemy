from __future__ import annotations

from typing import Any
from typing import Dict

from sqlalchemy import Column
from sqlalchemy import insert
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    data: Mapped[str]


# test #9376
d1: dict[str, Any] = {}
stmt1 = insert(User).values(d1)


d2: Dict[str, Any] = {}
stmt2 = insert(User).values(d2)


d3: Dict[Column[str], Any] = {}
stmt3 = insert(User).values(d3)
