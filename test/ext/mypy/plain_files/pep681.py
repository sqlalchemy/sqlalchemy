from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass


class Base(MappedAsDataclass, DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    data: Mapped[str]
    x: Mapped[Optional[int]] = mapped_column(default=None)
    y: Mapped[Optional[int]] = mapped_column(kw_only=True)


a1 = A(data="some data", y=5)

# EXPECTED_TYPE: str
reveal_type(a1.data)

# EXPECTED_RE_TYPE: .*Union\[builtins.int, None\]
reveal_type(a1.y)

a1.data = "some other data"
