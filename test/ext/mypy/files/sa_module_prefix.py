from typing import List
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import orm as saorm


Base = saorm.declarative_base()


class B(Base):
    __tablename__ = "b"
    id = sa.Column(sa.Integer, primary_key=True)
    a_id: int = sa.Column(sa.ForeignKey("a.id"))
    data = sa.Column(sa.String)

    a: Optional["A"] = saorm.relationship("A", back_populates="bs")


class A(Base):
    __tablename__ = "a"

    id = sa.Column(sa.Integer, primary_key=True)
    data = sa.Column(sa.String)
    bs = saorm.relationship(B, uselist=True, back_populates="a")


a1 = A(bs=[B(data="b"), B(data="b")])

x: List[B] = a1.bs


b1 = B(a=A())
