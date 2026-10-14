from typing import Optional

from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import Unmapped
from sqlalchemy.orm import unmapped_column


class Base(DeclarativeBase):
    pass


class DCBase(MappedAsDataclass, DeclarativeBase):
    pass


class Plain(Base):
    __tablename__ = "plain"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str]

    x: Unmapped[Optional[str]] = unmapped_column()
    y: Unmapped[str] = unmapped_column(String(30), server_default="y")
    z: Unmapped[int]


class DC(DCBase):
    __tablename__ = "dc"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    data: Mapped[str]

    # unmapped_column() is a field specifier that's always init=False
    x: Unmapped[Optional[str]] = unmapped_column(default="x")
    y: Unmapped[int] = unmapped_column()

    q: Mapped[Optional[str]] = mapped_column(default=None)


p1 = Plain(data="d")

dc1 = DC(data="d")
dc2 = DC("d", None)

# EXPECTED_MYPY: Unexpected keyword argument "x" for "DC"
DC(data="d", x="x")

# Unmapped is not a descriptor, so there's no __set__() that would accept
# the plain value; the attribute is typed as the Unmapped container itself

# EXPECTED_MYPY_RE: Incompatible types in assignment \(expression has type "str", variable has type "Unmapped\[.*\]"\)  # noqa: E501
dc1.x = "some x"

# EXPECTED_MYPY_RE: Incompatible types in assignment \(expression has type "str", variable has type "Unmapped\[.*\]"\)  # noqa: E501
p1.x = "some x"

# EXPECTED_MYPY_RE: Argument "init" to "unmapped_column" has incompatible .*
unmapped_column(init=True)
