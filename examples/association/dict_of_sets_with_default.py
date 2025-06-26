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
from typing import Mapping

from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.collections import KeyFuncDict


class Base(DeclarativeBase):
    id: Mapped[int] = mapped_column(primary_key=True)


class GenDefaultCollection(KeyFuncDict[str, "B"]):
    def __missing__(self, key: str) -> B:
        self[key] = b = B(key)
        return b


class A(Base):
    __tablename__ = "a"
    associations: Mapped[Mapping[str, B]] = relationship(
        "B",
        collection_class=lambda: GenDefaultCollection(
            operator.attrgetter("key")
        ),
    )

    collections: AssociationProxy[dict[str, set[int]]] = association_proxy(
        "associations", "values"
    )
    """Bridge the association from 'associations' over to the 'values'
    association proxy of B.
    """


class B(Base):
    __tablename__ = "b"
    a_id: Mapped[int] = mapped_column(ForeignKey("a.id"))
    elements: Mapped[set[C]] = relationship("C", collection_class=set)
    key: Mapped[str]

    values: AssociationProxy[set[int]] = association_proxy("elements", "value")
    """Bridge the association from 'elements' over to the
    'value' element of C."""

    def __init__(self, key: str, values: set[int] | None = None) -> None:
        self.key = key
        if values:
            self.values = values


class C(Base):
    __tablename__ = "c"
    b_id: Mapped[int] = mapped_column(ForeignKey("b.id"))
    value: Mapped[int]

    def __init__(self, value: int) -> None:
        self.value = value


if __name__ == "__main__":
    engine = create_engine("sqlite://", echo=True)
    Base.metadata.create_all(engine)
    session = Session(engine)

    # only "A" is referenced explicitly.  Using "collections",
    # we deal with a dict of key/sets of integers directly.

    session.add_all([A(collections={"1": {1, 2, 3}})])
    session.commit()

    a1 = session.scalars(select(A)).one()
    print(a1.collections["1"])
    a1.collections["1"].add(4)
    session.commit()

    a1.collections["2"].update([7, 8, 9])
    session.commit()

    print(a1.collections["2"])
