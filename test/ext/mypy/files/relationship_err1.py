from typing import List

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class B(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)

    # EXPECTED: Expected Python collection type for collection_class parameter # noqa
    as_: List["A"] = relationship("A", collection_class=None)

    # EXPECTED: Can't infer type from ORM mapped expression assigned to attribute 'another_as_'; # noqa
    another_as_ = relationship("A", uselist=True)


class A(Base):
    __tablename__ = "a"

    id = Column(Integer, primary_key=True)
    b_id: int = Column(ForeignKey("b.id"))

    # EXPECTED: Sending uselist=False and collection_class at the same time does not make sense # noqa
    b: B = relationship(B, uselist=False, collection_class=set)
