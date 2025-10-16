from __future__ import annotations

from typing import assert_type
from typing import Optional

from sqlalchemy.orm import column_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import query_expression


class Base(DeclarativeBase):
    pass


class TestInitialSupport(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    data: Mapped[str]
    x: Mapped[Optional[int]] = mapped_column(default=None)
    y: Mapped[Optional[int]] = mapped_column(kw_only=True)


tis = TestInitialSupport(data="some data", y=5)

assert_type(tis.data, str)

assert_type(tis.y, int | None)

tis.data = "some other data"


class TestTicket9628(MappedAsDataclass, Base):
    __tablename__ = "ticket_9628"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    data: Mapped[str] = mapped_column()

    d2: Mapped[str] = column_property(data + "Asdf")
    d3: Mapped[str] = query_expression(data + "Asdf")


# d2 and d3 are not required, as these have init=False.  We omit
# them from dataclass transforms entirely as these are never intended
# to be writeable fields in a 2.0 declarative mapping
t9628 = TestTicket9628(data="asf")
