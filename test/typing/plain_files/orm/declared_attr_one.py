from datetime import datetime
import typing
from typing import Any
from typing import assert_type
from typing import Unpack

from sqlalchemy import DateTime
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import Select
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedClassProtocol
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.sql.schema import PrimaryKeyConstraint


class Base(DeclarativeBase):
    pass


class Employee(Base):
    __tablename__ = "employee"
    id = mapped_column(Integer, primary_key=True)
    name = mapped_column(String(50))
    type = mapped_column(String(20))

    __mapper_args__ = {
        "polymorphic_on": type,
        "polymorphic_identity": "employee",
    }

    __table_args__ = (
        Index("my_index", name, type.desc()),
        UniqueConstraint(name),
        PrimaryKeyConstraint(id),
        {"prefix": []},
    )


class Engineer(Employee):
    __mapper_args__ = {
        "polymorphic_identity": "engineer",
    }

    @declared_attr
    def start_date(cls) -> Mapped[datetime]:
        "Start date column, if not present already."

        assert Employee.__table__ is not None
        return getattr(
            Employee.__table__.c,
            "start date",
            mapped_column("start date", DateTime),
        )


class Manager(Employee):
    __mapper_args__ = {
        "polymorphic_identity": "manager",
    }

    @declared_attr
    def start_date(cls) -> Mapped[datetime]:
        "Start date column, if not present already."

        assert Employee.__table__ is not None
        return getattr(
            Employee.__table__.c,
            "start date",
            mapped_column("start date", DateTime),
        )


def do_something_with_mapped_class(
    cls_: MappedClassProtocol[Employee],
) -> None:
    assert_type(cls_.__table__.select(), Select[Unpack[tuple[Any, ...]]])

    assert_type(cls_.__mapper__, Mapper[Employee])

    assert_type(cls_(), Employee)


do_something_with_mapped_class(Manager)
do_something_with_mapped_class(Engineer)


if typing.TYPE_CHECKING:
    assert_type(Engineer.start_date, InstrumentedAttribute[datetime])

    assert_type(Manager.start_date, InstrumentedAttribute[datetime])
