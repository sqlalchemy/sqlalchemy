import typing
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
    c: Mapped[Optional[str]] = mapped_column(nullable=True)
    d: Mapped[str] = mapped_column(nullable=False)

    e: Mapped[Optional[str]] = mapped_column(ForeignKey(c), nullable=True)

    f1 = mapped_column(Integer)
    f: Mapped[Optional[str]] = mapped_column(ForeignKey(f1), nullable=False)

    g: Mapped[str] = mapped_column(String)
    h: Mapped[Optional[str]] = mapped_column(String)

    # this probably is wrong.  however at the moment it seems better to
    # decouple the right hand arguments from declaring things about the
    # left side since it mostly doesn't work in any case.
    j: Mapped[str] = mapped_column(String, nullable=False)

    k: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    l: Mapped[Optional[str]] = mapped_column(String, nullable=False)

    a_name: Mapped[str] = mapped_column("a_name")
    b_name: Mapped[Optional[str]] = mapped_column("b_name")

    d_name: Mapped[str] = mapped_column("d_name", nullable=False)

    e_name: Mapped[Optional[str]] = mapped_column("e_name", nullable=True)

    f_name: Mapped[Optional[str]] = mapped_column("f_name", nullable=False)

    g_name: Mapped[str] = mapped_column("g_name", String)
    h_name: Mapped[Optional[str]] = mapped_column("h_name", String)

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


if typing.TYPE_CHECKING:
    # EXPECTED_RE_TYPE: sqlalchemy.orm.properties.MappedColumn\[builtins.int\]
    reveal_type(mapped_column(Integer))

    # EXPECTED_RE_TYPE: sqlalchemy.orm.properties.MappedColumn\[Union\[builtins.int, None\]\]
    reveal_type(mapped_column(Integer, nullable=True))

    # EXPECTED_RE_TYPE: sqlalchemy.orm.properties.MappedColumn\[builtins.int\]
    reveal_type(mapped_column(Integer, default=7))

    # EXPECTED_MYPY_RE: Argument 1 to "mapped_column" has incompatible type.*
    a_err: Mapped[str] = mapped_column(Integer)

    # EXPECTED_MYPY_RE: Argument 2 to "mapped_column" has incompatible type.*
    a_err_name: Mapped[str] = mapped_column("a", Integer)

    # EXPECTED_MYPY_RE: Argument "default" to "mapped_column" has incompatible type "None".*
    b_err: Mapped[int] = mapped_column(Integer, default=None)

    # EXPECTED_MYPY_RE: Argument "default" to "mapped_column" has incompatible type "None".*
    b_err_name: Mapped[int] = mapped_column("b", Integer, default=None)

    # EXPECTED_MYPY_RE: Argument "default" to "mapped_column" has incompatible type "None".*
    c_err: Mapped[int] = mapped_column(default=None)

    # EXPECTED_MYPY_RE: Argument "default" to "mapped_column" has incompatible type "None".*
    c_err_name: Mapped[int] = mapped_column("c", default=None)

    # EXPECTED_MYPY_RE: Incompatible types in assignment.*
    d_err: Mapped[int] = mapped_column(Integer, nullable=True)

    # EXPECTED_MYPY_RE: Incompatible types in assignment.*
    d_err_name: Mapped[int] = mapped_column("d", Integer, nullable=True)

    # EXPECTED_MYPY_RE: Incompatible types in assignment.*
    e_err: Mapped[int] = mapped_column(nullable=True)

    # EXPECTED_MYPY_RE: Incompatible types in assignment.*
    e_err_name: Mapped[int] = mapped_column("e", nullable=True)

    # EXPECTED_MYPY_RE: Argument "default" to "mapped_column" has incompatible type.*
    f_err: Mapped[int] = mapped_column(default="a")

    # EXPECTED_MYPY_RE: Argument "default" to "mapped_column" has incompatible type.*
    f_err_name: Mapped[int] = mapped_column("f", default="a")

    # All of these are fine
    x1: Mapped[str] = mapped_column(String, default="a", nullable=False)
    x2: Mapped[str] = mapped_column(String, default="a")
    x3: Mapped[str] = mapped_column(default="a", nullable=False)
    x4: Mapped[str] = mapped_column(String, nullable=False)
    x5: Mapped[str] = mapped_column(String)
    x6: Mapped[str] = mapped_column(default="a")
    x7: Mapped[str] = mapped_column(nullable=False)
    x8: Mapped[str] = mapped_column()

    y1: Mapped[Optional[int]] = mapped_column(Integer, default=None, nullable=True)
    y2: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    y3: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    y4: Mapped[Optional[int]] = mapped_column(default=None, nullable=True)
    y5: Mapped[Optional[int]] = mapped_column(default=None)
    y6: Mapped[Optional[int]] = mapped_column(Integer)
    y7: Mapped[Optional[int]] = mapped_column(nullable=True)
    y8: Mapped[Optional[int]] = mapped_column()

    z1: Mapped[int] = mapped_column(Integer, default=7, nullable=False)
    z2: Mapped[int] = mapped_column(Integer, default=7)
    z3: Mapped[int] = mapped_column(default=7, nullable=False)
    z4: Mapped[int] = mapped_column(Integer, nullable=False)
    z5: Mapped[int] = mapped_column(Integer)
    z6: Mapped[int] = mapped_column(default=7)
    z7: Mapped[int] = mapped_column(nullable=False)
    z8: Mapped[int] = mapped_column()

