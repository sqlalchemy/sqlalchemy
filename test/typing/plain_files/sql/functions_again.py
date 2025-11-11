from typing import Any
from typing import assert_type

from sqlalchemy import column
from sqlalchemy import func
from sqlalchemy import Function
from sqlalchemy import Integer
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.expression import FunctionFilter
from sqlalchemy.sql.expression import Over
from sqlalchemy.sql.expression import WithinGroup
from sqlalchemy.sql.functions import coalesce
from sqlalchemy.sql.functions import max as functions_max
from sqlalchemy.sql.selectable import TableValuedAlias


class Base(DeclarativeBase):
    pass


class Foo(Base):
    __tablename__ = "foo"

    id: Mapped[int] = mapped_column(primary_key=True)
    a: Mapped[int]
    b: Mapped[int]
    c: Mapped[str]
    _d: Mapped[int | None] = mapped_column("d")

    @hybrid_property
    def d(self) -> int | None:
        return self._d


assert_type(
    func.row_number().over(order_by=Foo.a, partition_by=Foo.b.desc()),
    Over[Any],
)
func.row_number().over(order_by=[Foo.a.desc(), Foo.b.desc()])
func.row_number().over(partition_by=[Foo.a.desc(), Foo.b.desc()])
func.row_number().over(order_by="a", partition_by=("a", "b"))
func.row_number().over(partition_by="a", order_by=("a", "b"))


assert_type(func.row_number().filter(), Function[Any])
assert_type(func.row_number().filter(Foo.a > 0), FunctionFilter[Any])
assert_type(
    func.row_number().within_group(Foo.a).filter(Foo.b < 0),
    FunctionFilter[Any],
)
assert_type(func.row_number().within_group(Foo.a), WithinGroup[Any])
assert_type(
    func.row_number().filter(Foo.a > 0).within_group(Foo.a), WithinGroup[Any]
)
assert_type(func.row_number().filter(Foo.a > 0).over(), Over[Any])
assert_type(func.row_number().within_group(Foo.a).over(), Over[Any])

# test #10801
assert_type(func.max(Foo.b), functions_max[int])


stmt1 = select(Foo.a, func.min(Foo.b)).group_by(Foo.a)
assert_type(stmt1, Select[int, int])

# test #10818
assert_type(func.coalesce(Foo.c, "a", "b"), coalesce[str])
assert_type(func.coalesce("a", "b"), coalesce[str])
assert_type(func.coalesce(column("x", Integer), 3), coalesce[int])

assert_type(func.coalesce(Foo._d, 100), coalesce[int])

stmt2 = select(Foo.a, func.coalesce(Foo.c, "a", "b")).group_by(Foo.a)
assert_type(stmt2, Select[int, str])


assert_type(func.json_each().table_valued("key", "value"), TableValuedAlias)
assert_type(func.json_each().table_valued(Foo.a, Foo.b), TableValuedAlias)
