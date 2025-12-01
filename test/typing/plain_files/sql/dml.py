from __future__ import annotations

from typing import Any
from typing import Dict

from sqlalchemy import Column
from sqlalchemy import insert
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import update
from sqlalchemy import values
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


user_table = Table(
    "user",
    MetaData(),
    Column("id", Integer, primary_key=True),
    Column("data", String),
)


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

stmt4 = insert(User).from_select(
    [User.id, "name", User.__table__.c.data],
    select(User.id, User.name, User.data),
)


# test #10353
stmt5 = update(User).values({User.id: 123, User.data: "value"})

stmt6 = user_table.update().values(
    {user_table.c.d: 123, user_table.c.data: "value"}
)


update_values = values(
    User.id,
    User.name,
    name="update_values",
).data([(1, "Alice"), (2, "Bob")])

query = (
    update(User)
    .values(
        {
            User.name: update_values.c.name,
        }
    )
    .where(User.id == update_values.c.id)
)
