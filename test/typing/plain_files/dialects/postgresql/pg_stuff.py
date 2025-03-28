from typing import Any
from typing import Dict
from uuid import UUID as _py_uuid

from sqlalchemy import cast
from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import DATERANGE
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import INT8MULTIRANGE
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import TSTZMULTIRANGE
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

# test #6402

c1 = Column(UUID())

# EXPECTED_TYPE: Column[UUID]
reveal_type(c1)

c2 = Column(UUID(as_uuid=False))

# EXPECTED_TYPE: Column[str]
reveal_type(c2)


class Base(DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = "test_table_json"

    id = mapped_column(Integer, primary_key=True)
    data: Mapped[Dict[str, Any]] = mapped_column(JSONB)

    ident: Mapped[_py_uuid] = mapped_column(UUID())

    ident_str: Mapped[str] = mapped_column(UUID(as_uuid=False))


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

# EXPECTED_RE_TYPE: .*dict\[.*str, Any\]
reveal_type(t1.data)

# EXPECTED_TYPE: UUID
reveal_type(t1.ident)

unique = UniqueConstraint(name="my_constraint")
insert(Test).on_conflict_do_nothing(
    "foo", [Test.id], Test.id > 0
).on_conflict_do_update(
    unique, ["foo"], Test.id > 0, {"id": 42, Test.ident: 99}, Test.id == 22
).excluded.foo.desc()

s1 = insert(Test)
s1.on_conflict_do_update(set_=s1.excluded)


# EXPECTED_TYPE: Column[Range[int]]
reveal_type(Column(INT4RANGE()))
# EXPECTED_TYPE: Column[Range[datetime.date]]
reveal_type(Column("foo", DATERANGE()))
# EXPECTED_TYPE: Column[Sequence[Range[int]]]
reveal_type(Column(INT8MULTIRANGE()))
# EXPECTED_TYPE: Column[Sequence[Range[datetime.datetime]]]
reveal_type(Column("foo", TSTZMULTIRANGE()))


range_col_stmt = select(Column(INT4RANGE()), Column(INT8MULTIRANGE()))

# EXPECTED_TYPE: Select[Range[int], Sequence[Range[int]]]
reveal_type(range_col_stmt)

array_from_ints = array(range(2))

# EXPECTED_TYPE: array[int]
reveal_type(array_from_ints)

array_of_strings = array([], type_=Text)

# EXPECTED_TYPE: array[str]
reveal_type(array_of_strings)

array_of_ints = array([0], type_=Integer)

# EXPECTED_TYPE: array[int]
reveal_type(array_of_ints)

# EXPECTED_MYPY: Cannot infer type argument 1 of "array"
array([0], type_=Text)

# EXPECTED_TYPE: ARRAY[str]
reveal_type(ARRAY(Text))

# EXPECTED_TYPE: Column[Sequence[int]]
reveal_type(Column(type_=ARRAY(Integer)))

stmt_array_agg = select(func.array_agg(Column("num", type_=Integer)))

# EXPECTED_TYPE: Select[Sequence[int]]
reveal_type(stmt_array_agg)

# EXPECTED_TYPE: Select[Sequence[str]]
reveal_type(select(func.array_agg(Test.ident_str)))

stmt_array_agg_order_by_1 = select(
    func.array_agg(
        aggregate_order_by(
            Column("title", type_=Text),
            Column("date", type_=DATERANGE).desc(),
            Column("id", type_=Integer),
        ),
    )
)

# EXPECTED_TYPE: Select[Sequence[str]]
reveal_type(stmt_array_agg_order_by_1)

stmt_array_agg_order_by_2 = select(
    func.array_agg(
        aggregate_order_by(Test.ident_str, Test.id.desc(), Test.ident),
    )
)

# EXPECTED_TYPE: Select[Sequence[str]]
reveal_type(stmt_array_agg_order_by_2)
