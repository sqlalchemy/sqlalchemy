from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import Integer
from sqlalchemy import lambda_stmt
from sqlalchemy import MetaData
from sqlalchemy import Result
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str]


user_table = Table(
    "user_table", MetaData(), Column("id", Integer), Column("email", String)
)


s1 = select(user_table).where(lambda: user_table.c.id == 5)

s2 = select(User).where(lambda: User.id == 5)

s3 = lambda_stmt(lambda: select(user_table).where(user_table.c.id == 5))

s4 = lambda_stmt(lambda: select(User).where(User.id == 5))

s5 = lambda_stmt(lambda: select(user_table)) + (
    lambda s: s.where(user_table.c.id == 5)
)

s6 = lambda_stmt(lambda: select(User)) + (lambda s: s.where(User.id == 5))


if TYPE_CHECKING:
    # EXPECTED_TYPE: StatementLambdaElement
    reveal_type(s5)

    # EXPECTED_TYPE: StatementLambdaElement
    reveal_type(s6)


e = create_engine("sqlite://")

with e.connect() as conn:
    result = conn.execute(s6)

    if TYPE_CHECKING:
        # EXPECTED_TYPE: CursorResult[Unpack[.*tuple[Any, ...]]]
        reveal_type(result)

    # we can type these like this
    my_result: Result[User] = conn.execute(s6)

    if TYPE_CHECKING:
        # pyright and mypy disagree on the specific type here,
        # mypy sees Result as we said, pyright seems to upgrade it to
        # CursorResult
        # EXPECTED_RE_TYPE: .*(?:Cursor)?Result\[.*User\]
        reveal_type(my_result)
