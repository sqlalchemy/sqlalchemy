"""An advanced association proxy example which
illustrates nesting of association proxies to produce multi-level Python
collections, in this case a dictionary with string keys and sets of integers
as values, which conceal the underlying mapped classes.

This is a three table model which represents a parent table referencing a
dictionary of string keys and sets as values, where each set stores a
collection of integers. The association proxy extension is used to hide the
details of this persistence. The dictionary also generates new collections
upon access of a non-existent key, in the same manner as Python's
"collections.defaultdict" object.

"""
from __future__ import annotations

import operator
from typing import Optional
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import KeyFuncDict


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(Integer, primary_key=True)


class GenDefaultCollection(KeyFuncDict):
    def __missing__(self, key) -> B:
        self[key] = b = B(key)
        return b


class A(Base):
    __tablename__ = "a"
    associations = relationship(
        "B",
        collection_class=lambda: GenDefaultCollection(
            operator.attrgetter("key")
        ),
    )

    collections = association_proxy("associations", "values")
    """Bridge the association from 'associations' over to the 'values'
    association proxy of B.
    """


class B(Base):
    __tablename__ = "b"
    a_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("a.id"), nullable=False
    )
    elements = relationship("C", collection_class=set)
    key: Mapped[str] = mapped_column(String)

    values = association_proxy("elements", "value")
    """Bridge the association from 'elements' over to the
    'value' element of C."""

    def __init__(self, key: str, values: Optional[int] = None):
        self.key = key
        if values:
            self.values = values


class C(Base):
    __tablename__ = "c"
    b_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("b.id"), nullable=False
    )
    value: Mapped[int] = mapped_column(Integer)

    def __init__(self, value: int):
        self.value = value


if __name__ == "__main__":
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    session = Session(engine)

    # only "A" is referenced explicitly.  Using "collections",
    # we deal with a dict of key/sets of integers directly.

    session.add_all([A(collections={"1": {1, 2, 3}})])
    session.commit()

    a1 = session.query(A).first()
    if TYPE_CHECKING:
        assert a1 is not None
    print(a1.collections["1"])
    a1.collections["1"].add(4)
    session.commit()

    a1.collections["2"].update([7, 8, 9])
    session.commit()

    print(a1.collections["2"])
