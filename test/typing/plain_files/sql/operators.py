import datetime as dt
from decimal import Decimal
from typing import Any
from typing import assert_type
from typing import List

from sqlalchemy import ARRAY
from sqlalchemy import BigInteger
from sqlalchemy import column
from sqlalchemy import ColumnElement
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import Grouping
from sqlalchemy.sql.expression import BinaryExpression


class Base(DeclarativeBase):
    pass


class A(Base):
    __tablename__ = "a"
    id: Mapped[int]
    string: Mapped[str]
    arr: Mapped[List[int]] = mapped_column(ARRAY(Integer))


lt1: "ColumnElement[bool]" = A.id > A.id
lt2: "ColumnElement[bool]" = A.id > 1
lt3: "ColumnElement[bool]" = 1 < A.id

le1: "ColumnElement[bool]" = A.id >= A.id
le2: "ColumnElement[bool]" = A.id >= 1
le3: "ColumnElement[bool]" = 1 <= A.id

eq1: "ColumnElement[bool]" = A.id == A.id
eq2: "ColumnElement[bool]" = A.id == 1
# eq3: "ColumnElement[bool]" = 1 == A.id

ne1: "ColumnElement[bool]" = A.id != A.id
ne2: "ColumnElement[bool]" = A.id != 1
# ne3: "ColumnElement[bool]" = 1 != A.id

gt1: "ColumnElement[bool]" = A.id < A.id
gt2: "ColumnElement[bool]" = A.id < 1
gt3: "ColumnElement[bool]" = 1 > A.id

ge1: "ColumnElement[bool]" = A.id <= A.id
ge2: "ColumnElement[bool]" = A.id <= 1
ge3: "ColumnElement[bool]" = 1 >= A.id


# TODO "in" doesn't seem to pick up the typing of __contains__?
# EXPECTED_MYPY: Incompatible types in assignment (expression has type "bool", variable has type "ColumnElement[bool]") # noqa: E501
contains1: "ColumnElement[bool]" = A.id in A.arr
# EXPECTED_MYPY: Incompatible types in assignment (expression has type "bool", variable has type "ColumnElement[bool]") # noqa: E501
contains2: "ColumnElement[bool]" = A.id in A.string

lshift1: "ColumnElement[int]" = A.id << A.id
lshift2: "ColumnElement[int]" = A.id << 1
lshift3: "ColumnElement[Any]" = A.string << 1

rshift1: "ColumnElement[int]" = A.id >> A.id
rshift2: "ColumnElement[int]" = A.id >> 1
rshift3: "ColumnElement[Any]" = A.string >> 1

concat1: "ColumnElement[str]" = A.string.concat(A.string)
concat2: "ColumnElement[str]" = A.string.concat(1)
concat3: "ColumnElement[str]" = A.string.concat("a")

like1: "ColumnElement[bool]" = A.string.like("test")
like2: "ColumnElement[bool]" = A.string.like("test", escape="/")
ilike1: "ColumnElement[bool]" = A.string.ilike("test")
ilike2: "ColumnElement[bool]" = A.string.ilike("test", escape="/")

in_: "ColumnElement[bool]" = A.id.in_([1, 2])
not_in: "ColumnElement[bool]" = A.id.not_in([1, 2])

not_like1: "ColumnElement[bool]" = A.string.not_like("test")
not_like2: "ColumnElement[bool]" = A.string.not_like("test", escape="/")
not_ilike1: "ColumnElement[bool]" = A.string.not_ilike("test")
not_ilike2: "ColumnElement[bool]" = A.string.not_ilike("test", escape="/")

is_: "ColumnElement[bool]" = A.string.is_("test")
is_not: "ColumnElement[bool]" = A.string.is_not("test")

startswith: "ColumnElement[bool]" = A.string.startswith("test")
endswith: "ColumnElement[bool]" = A.string.endswith("test")
contains: "ColumnElement[bool]" = A.string.contains("test")
match: "ColumnElement[bool]" = A.string.match("test")
regexp_match: "ColumnElement[bool]" = A.string.regexp_match("test")

regexp_replace: "ColumnElement[str]" = A.string.regexp_replace(
    "pattern", "replacement"
)
between: "ColumnElement[bool]" = A.string.between("a", "b")

adds: "ColumnElement[str]" = A.string + A.string
add1: "ColumnElement[int]" = A.id + A.id
add2: "ColumnElement[int]" = A.id + 1
add3: "ColumnElement[int]" = 1 + A.id
add_date: "ColumnElement[dt.date]" = func.current_date() + dt.timedelta(days=1)
add_datetime: "ColumnElement[dt.datetime]" = (
    func.current_timestamp() + dt.timedelta(seconds=1)
)

sub1: "ColumnElement[int]" = A.id - A.id
sub2: "ColumnElement[int]" = A.id - 1
sub3: "ColumnElement[int]" = 1 - A.id

mul1: "ColumnElement[int]" = A.id * A.id
mul2: "ColumnElement[int]" = A.id * 1
mul3: "ColumnElement[int]" = 1 * A.id

div1: "ColumnElement[float|Decimal]" = A.id / A.id
div2: "ColumnElement[float|Decimal]" = A.id / 1
div3: "ColumnElement[float|Decimal]" = 1 / A.id

mod1: "ColumnElement[int]" = A.id % A.id
mod2: "ColumnElement[int]" = A.id % 1
mod3: "ColumnElement[int]" = 1 % A.id

# unary

neg: "ColumnElement[int]" = -A.id

desc: "ColumnElement[int]" = A.id.desc()
asc: "ColumnElement[int]" = A.id.asc()
any_: "ColumnElement[bool]" = A.id.any_()
all_: "ColumnElement[bool]" = A.id.all_()
nulls_first: "ColumnElement[int]" = A.id.nulls_first()
nulls_last: "ColumnElement[int]" = A.id.nulls_last()
collate: "ColumnElement[str]" = A.string.collate("somelang")
distinct: "ColumnElement[int]" = A.id.distinct()


# custom ops
col = column("flags", Integer)
op_a: "ColumnElement[Any]" = col.op("&")(1)
op_b: "ColumnElement[int]" = col.op("&", return_type=Integer)(1)
op_c: "ColumnElement[str]" = col.op("&", return_type=String)("1")
op_d: "ColumnElement[int]" = col.op("&", return_type=BigInteger)("1")
op_e: "ColumnElement[bool]" = col.bool_op("&")("1")


op_a1 = col.op("&")(1)
assert_type(op_a1, BinaryExpression[Any])


# op functions
t1 = operators.eq(A.id, 1)
select().where(t1)

assert_type(col.op("->>")("field"), BinaryExpression[Any])
assert_type(
    col.op("->>")("field").self_group(), BinaryExpression[Any] | Grouping[Any]
)
