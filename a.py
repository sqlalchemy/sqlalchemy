from typing import TypeVar

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase

T = TypeVar("T")

session = sa.orm.Session(sa.create_engine('...'))


class Base(DeclarativeBase):
    ...


class Table(Base):
    __tablename__ = "site_thread"

    id: Mapped[str] = mapped_column(sa.String(), primary_key=True)


_: Table = session.query(Table).where(Table.id == 1).one()
