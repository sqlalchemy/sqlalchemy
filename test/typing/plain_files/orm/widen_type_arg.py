"""ORM constructs that receive a SQL expression typed from a SQL type
argument may be assigned to a wider declared type.  The ORM constructors
themselves don't infer their type from a SQL type argument; the declared
type reaches the inner Core construct, which accepts the widening.
#13654

"""

from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import type_coerce
from sqlalchemy.orm import column_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import deferred
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedSQLExpression
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import with_expression


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"

    id: Mapped[int] = mapped_column(primary_key=True)
    data: Mapped[str] = mapped_column(String)

    c1: Mapped[int | None] = column_property(cast(data, Integer))
    c2: Mapped[int | None] = column_property(type_coerce(data, Integer))
    c3: Mapped[float] = column_property(cast(data, Integer))
    c4: Mapped[int | None] = column_property(Column(Integer))
    c5: Mapped[int | None] = column_property(column("q", Integer))
    c6: MappedSQLExpression[int | None] = column_property(cast(data, Integer))

    d1: Mapped[int | None] = deferred(cast(data, Integer))
    d2: Mapped[int | None] = deferred(Column(Integer))

    q1: Mapped[int | None] = query_expression(cast(data, Integer))


def test_with_expression() -> None:
    with_expression(A.q1, cast(A.data, Integer))
