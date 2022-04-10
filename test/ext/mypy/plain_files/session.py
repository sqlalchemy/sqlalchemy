from __future__ import annotations

from typing import List

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    addresses: Mapped[List[Address]] = relationship(back_populates="user")


class Address(Base):
    __tablename__ = "address"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id = mapped_column(ForeignKey("user.id"))
    email: Mapped[str]

    user: Mapped[User] = relationship(back_populates="addresses")


e = create_engine("sqlite://")
Base.metadata.create_all(e)

with Session(e) as sess:
    u1 = User(name="u1")
    sess.add(u1)
    sess.add_all([Address(user=u1, email="e1"), Address(user=u1, email="e2")])
    sess.commit()

with Session(e) as sess:
    users: List[User] = sess.scalars(
        select(User), execution_options={"stream_results": False}
    ).all()
