from typing import Optional

from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class X(Base):
    __tablename__ = "x"

    id: Mapped[int] = mapped_column(primary_key=True)
    int_id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # EXPECTED_MYPY: No overload variant of "mapped_column" matches argument types
    err_int_id: Mapped[Optional[int]] = mapped_column(
        Integer, primary_key=True
    )

    id_name: Mapped[int] = mapped_column("id_name", primary_key=True)
    int_id_name: Mapped[int] = mapped_column(
        "int_id_name", Integer, primary_key=True
    )

    # EXPECTED_MYPY: No overload variant of "mapped_column" matches argument types
    err_int_id_name: Mapped[Optional[int]] = mapped_column(
        "err_int_id_name", Integer, primary_key=True
    )

    # note we arent getting into primary_key=True / nullable=True here.
    # leaving that as undefined for now

    a: Mapped[str] = mapped_column()
    b: Mapped[Optional[str]] = mapped_column()

    # can't detect error because no SQL type is present
    c: Mapped[str] = mapped_column(nullable=True)
    d: Mapped[str] = mapped_column(nullable=False)

    e: Mapped[Optional[str]] = mapped_column(nullable=True)

    # can't detect error because no SQL type is present
    f: Mapped[Optional[str]] = mapped_column(nullable=False)

    g: Mapped[str] = mapped_column(String)
    h: Mapped[Optional[str]] = mapped_column(String)

    # EXPECTED_MYPY: No overload variant of "mapped_column" matches argument types
    i: Mapped[str] = mapped_column(String, nullable=True)

    j: Mapped[str] = mapped_column(String, nullable=False)

    k: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    # EXPECTED_MYPY_RE: Argument \d to "mapped_column" has incompatible type
    l: Mapped[Optional[str]] = mapped_column(String, nullable=False)

    a_name: Mapped[str] = mapped_column("a_name")
    b_name: Mapped[Optional[str]] = mapped_column("b_name")

    # can't detect error because no SQL type is present
    c_name: Mapped[str] = mapped_column("c_name", nullable=True)
    d_name: Mapped[str] = mapped_column("d_name", nullable=False)

    e_name: Mapped[Optional[str]] = mapped_column("e_name", nullable=True)

    # can't detect error because no SQL type is present
    f_name: Mapped[Optional[str]] = mapped_column("f_name", nullable=False)

    g_name: Mapped[str] = mapped_column("g_name", String)
    h_name: Mapped[Optional[str]] = mapped_column("h_name", String)

    # EXPECTED_MYPY: No overload variant of "mapped_column" matches argument types
    i_name: Mapped[str] = mapped_column("i_name", String, nullable=True)

    j_name: Mapped[str] = mapped_column("j_name", String, nullable=False)

    k_name: Mapped[Optional[str]] = mapped_column(
        "k_name", String, nullable=True
    )

    l_name: Mapped[Optional[str]] = mapped_column(
        "l_name",
        # EXPECTED_MYPY_RE: Argument \d to "mapped_column" has incompatible type
        String,
        nullable=False,
    )
