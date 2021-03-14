from typing import Callable

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import deferred
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship
from sqlalchemy.orm.decl_api import declared_attr
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.sql.schema import ForeignKey


reg: registry = registry()


@reg.mapped
class C:
    __tablename__ = "c"
    id = Column(Integer, primary_key=True)


def some_other_decorator(fn: Callable[..., None]) -> Callable[..., None]:
    return fn


class HasAMixin:
    @declared_attr
    def a(cls) -> Mapped["A"]:
        return relationship("A", back_populates="bs")

    # EXPECTED: Can't infer type from @declared_attr on function 'a2';
    @declared_attr
    def a2(cls):
        return relationship("A", back_populates="bs")

    @declared_attr
    def a3(cls) -> relationship["A"]:
        return relationship("A", back_populates="bs")

    @declared_attr
    def c1(cls) -> relationship[C]:
        return relationship(C, back_populates="bs")

    @declared_attr
    def c2(cls) -> Mapped[C]:
        return relationship(C, back_populates="bs")

    @declared_attr
    def data(cls) -> Column[String]:
        return Column(String)

    @declared_attr
    def data2(cls) -> MapperProperty[str]:
        return deferred(Column(String))

    @some_other_decorator
    def q(cls) -> None:
        return None


@reg.mapped
class B(HasAMixin):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    a_id: int = Column(ForeignKey("a.id"))
    c_id: int = Column(ForeignKey("c.id"))


@reg.mapped
class A:
    __tablename__ = "a"

    id = Column(Integer, primary_key=True)

    @declared_attr
    def data(cls) -> Column[String]:
        return Column(String)

    # EXPECTED: Can't infer type from @declared_attr on function 'data2';
    @declared_attr
    def data2(cls):
        return Column(String)

    bs = relationship(B, uselist=True, back_populates="a")


a1 = A(id=1, data="d1", data2="d2")


b1 = B(a=A(), a2=A(), c1=C(), c2=C(), data="d1", data2="d2")

# descriptor access as Mapped[<type>]
B.a.any()
B.a2.any()
B.c1.any()
B.c2.any()

# sanity check against another fn that isn't mapped
# EXPECTED_MYPY: "Callable[..., None]" has no attribute "any"
B.q.any()

B.data.in_(["a", "b"])
B.data2.in_(["a", "b"])
