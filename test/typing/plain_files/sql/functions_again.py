from sqlalchemy import column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Foo(Base):
    __tablename__ = "foo"

    id: Mapped[int] = mapped_column(primary_key=True)
    a: Mapped[int]
    b: Mapped[int]
    c: Mapped[str]


# EXPECTED_TYPE: Over[Any]
reveal_type(func.row_number().over(order_by=Foo.a, partition_by=Foo.b.desc()))
func.row_number().over(order_by=[Foo.a.desc(), Foo.b.desc()])
func.row_number().over(partition_by=[Foo.a.desc(), Foo.b.desc()])
func.row_number().over(order_by="a", partition_by=("a", "b"))
func.row_number().over(partition_by="a", order_by=("a", "b"))


# EXPECTED_TYPE: Function[Any]
reveal_type(func.row_number().filter())
# EXPECTED_TYPE: FunctionFilter[Any]
reveal_type(func.row_number().filter(Foo.a > 0))
# EXPECTED_TYPE: FunctionFilter[Any]
reveal_type(func.row_number().within_group(Foo.a).filter(Foo.b < 0))
# EXPECTED_TYPE: WithinGroup[Any]
reveal_type(func.row_number().within_group(Foo.a))
# EXPECTED_TYPE: WithinGroup[Any]
reveal_type(func.row_number().filter(Foo.a > 0).within_group(Foo.a))
# EXPECTED_TYPE: Over[Any]
reveal_type(func.row_number().filter(Foo.a > 0).over())
# EXPECTED_TYPE: Over[Any]
reveal_type(func.row_number().within_group(Foo.a).over())

# test #10801
# EXPECTED_TYPE: max[int]
reveal_type(func.max(Foo.b))


stmt1 = select(Foo.a, func.min(Foo.b)).group_by(Foo.a)
# EXPECTED_TYPE: Select[int, int]
reveal_type(stmt1)

# test #10818
# EXPECTED_TYPE: coalesce[str]
reveal_type(func.coalesce(Foo.c, "a", "b"))
# EXPECTED_TYPE: coalesce[str]
reveal_type(func.coalesce("a", "b"))
# EXPECTED_TYPE: coalesce[int]
reveal_type(func.coalesce(column("x", Integer), 3))


stmt2 = select(Foo.a, func.coalesce(Foo.c, "a", "b")).group_by(Foo.a)
# EXPECTED_TYPE: Select[int, str]
reveal_type(stmt2)


# EXPECTED_TYPE: TableValuedAlias
reveal_type(func.json_each().table_valued("key", "value"))
# EXPECTED_TYPE: TableValuedAlias
reveal_type(func.json_each().table_valued(Foo.a, Foo.b))
