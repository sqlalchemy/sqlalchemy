from datetime import date
from datetime import datetime
from typing import Any
from typing import assert_type
from typing import Dict
from typing import Sequence
from uuid import UUID as _py_uuid

from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import DATERANGE
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import INT8MULTIRANGE
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.dialects.postgresql import TSTZMULTIRANGE
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

# test #6402

c1 = Column(UUID())

assert_type(c1, Column[_py_uuid])

c2 = Column(UUID(as_uuid=False))

assert_type(c2, Column[str])


class Base(DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = "test_table_json"

    id = mapped_column(Integer, primary_key=True)
    data: Mapped[Dict[str, Any]] = mapped_column(JSONB)

    ident: Mapped[_py_uuid] = mapped_column(UUID())

    ident_str: Mapped[str] = mapped_column(UUID(as_uuid=False))

    __table_args__ = (ExcludeConstraint((Column("ident_str"), "=")),)


elem = func.jsonb_array_elements(Test.data, type_=JSONB).column_valued("elem")

stmt = select(Test).where(
    or_(
        cast("example code", ARRAY(Text)).contained_by(
            array([select(elem["code"].astext).scalar_subquery()])
        ),
        cast("stefan", ARRAY(Text)).contained_by(
            array([select(elem["code"]["new_value"].astext).scalar_subquery()])
        ),
    )
)
print(stmt)


t1 = Test()

assert_type(t1.data, dict[str, Any])

assert_type(t1.ident, _py_uuid)

unique = UniqueConstraint(name="my_constraint")
insert(Test).on_conflict_do_nothing(
    "foo", [Test.id], Test.id > 0
).on_conflict_do_update(
    unique, ["foo"], Test.id > 0, {"id": 42, Test.ident: 99}, Test.id == 22
).excluded.foo.desc()

s1 = insert(Test)
s1.on_conflict_do_update(set_=s1.excluded)


assert_type(Column(INT4RANGE()), Column[Range[int]])
assert_type(Column("foo", DATERANGE()), Column[Range[date]])
assert_type(Column(INT8MULTIRANGE()), Column[Sequence[Range[int]]])
assert_type(Column("foo", TSTZMULTIRANGE()), Column[Sequence[Range[datetime]]])


range_col_stmt = select(Column(INT4RANGE()), Column(INT8MULTIRANGE()))

assert_type(range_col_stmt, Select[Range[int], Sequence[Range[int]]])

array_from_ints = array(range(2))

assert_type(array_from_ints, array[int])

array_of_strings = array([], type_=Text)

assert_type(array_of_strings, array[str])

array_of_ints = array([0], type_=Integer)

assert_type(array_of_ints, array[int])

# EXPECTED_MYPY_RE: Cannot infer .* of "array"
array([0], type_=Text)

assert_type(ARRAY(Text), ARRAY[str])

assert_type(Column(type_=ARRAY(Integer)), Column[Sequence[int]])

stmt_array_agg = select(func.array_agg(Column("num", type_=Integer)))

assert_type(stmt_array_agg, Select[Sequence[int]])

assert_type(select(func.array_agg(Test.ident_str)), Select[Sequence[str]])

stmt_array_agg_order_by_1 = select(
    func.array_agg(
        aggregate_order_by(
            Column("title", type_=Text),
            Column("date", type_=DATERANGE).desc(),
            Column("id", type_=Integer),
        ),
    )
)

assert_type(stmt_array_agg_order_by_1, Select[Sequence[str]])

stmt_array_agg_order_by_2 = select(
    func.array_agg(
        aggregate_order_by(Test.ident_str, Test.id.desc(), Test.ident),
    )
)

assert_type(stmt_array_agg_order_by_2, Select[Sequence[str]])
