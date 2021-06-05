from typing import List

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

mapper_registry: registry = registry()


@mapper_registry.mapped
class B:
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey("a.id"))
    ordering = Column(Integer)


@mapper_registry.mapped
class C:
    __tablename__ = "c"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey("a.id"))
    ordering = Column(Integer)


@mapper_registry.mapped
class A:
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)

    bs = relationship(B, collection_class=ordering_list("ordering"))

    bs_w_list: List[B] = relationship(
        B, collection_class=ordering_list("ordering")
    )

    # EXPECTED: Left hand assignment 'cs: "List[B]"' not compatible with ORM mapped expression of type "Mapped[List[C]]"  # noqa
    cs: List[B] = relationship(C, uselist=True)

    # EXPECTED: Left hand assignment 'cs_2: "B"' not compatible with ORM mapped expression of type "Mapped[List[C]]"  # noqa
    cs_2: B = relationship(C, uselist=True)


b1 = B(ordering=10)

# in this case, the plugin infers OrderingList as the type.  not great
a1 = A()
a1.bs.append(b1)

# so we want to support being able to override it at least
a2 = A(bs_w_list=[b1])
