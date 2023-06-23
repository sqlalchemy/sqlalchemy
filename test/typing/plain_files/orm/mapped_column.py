from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class X(Base):
    __tablename__ = "x"

    # these are fine - pk, column is not null, have the attribute be
    # non-optional, fine
    id: Mapped[int] = mapped_column(primary_key=True)
    int_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # but this is also "fine" because the developer may wish to have the object
    # in a pending state with None for the id for some period of time.
    # "primary_key=True" will still be interpreted correctly in DDL
    err_int_id: Mapped[Optional[int]] = mapped_column(
        Integer, primary_key=True
    )

    # also fine, X(err_int_id_name) is None when you first make the
    # object
    err_int_id_name: Mapped[Optional[int]] = mapped_column(
        "err_int_id_name", Integer, primary_key=True
    )

    id_name: Mapped[int] = mapped_column("id_name", primary_key=True)
    int_id_name: Mapped[int] = mapped_column(
        "int_id_name", Integer, primary_key=True
    )

    a: Mapped[str] = mapped_column()
    b: Mapped[Optional[str]] = mapped_column()

    # this can't be detected because we don't know the type
    c: Mapped[str] = mapped_column(nullable=True)
    d: Mapped[str] = mapped_column(nullable=False)

    e: Mapped[Optional[str]] = mapped_column(ForeignKey(c), nullable=True)

    f1 = mapped_column(Integer)
    f: Mapped[Optional[str]] = mapped_column(ForeignKey(f1), nullable=False)

    g: Mapped[str] = mapped_column(String)
    h: Mapped[Optional[str]] = mapped_column(String)

    # this probably is wrong.  however at the moment it seems better to
    # decouple the right hand arguments from declaring things about the
    # left side since it mostly doesn't work in any case.
    i: Mapped[str] = mapped_column(String, nullable=True)

    j: Mapped[str] = mapped_column(String, nullable=False)

    k: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    l: Mapped[Optional[str]] = mapped_column(String, nullable=False)

    a_name: Mapped[str] = mapped_column("a_name")
    b_name: Mapped[Optional[str]] = mapped_column("b_name")

    c_name: Mapped[str] = mapped_column("c_name", nullable=True)
    d_name: Mapped[str] = mapped_column("d_name", nullable=False)

    e_name: Mapped[Optional[str]] = mapped_column("e_name", nullable=True)

    f_name: Mapped[Optional[str]] = mapped_column("f_name", nullable=False)

    g_name: Mapped[str] = mapped_column("g_name", String)
    h_name: Mapped[Optional[str]] = mapped_column("h_name", String)

    i_name: Mapped[str] = mapped_column("i_name", String, nullable=True)

    j_name: Mapped[str] = mapped_column("j_name", String, nullable=False)

    k_name: Mapped[Optional[str]] = mapped_column(
        "k_name", String, nullable=True
    )

    l_name: Mapped[Optional[str]] = mapped_column(
        "l_name",
        String,
        nullable=False,
    )

    __table_args__ = (UniqueConstraint(a, b, name="uq1"), Index("ix1", c, d))
