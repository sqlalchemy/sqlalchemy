from datetime import date
from datetime import datetime
from typing import Any
from typing import assert_type
from typing import Dict
from typing import List
from typing import Sequence
from typing import Tuple
from uuid import UUID as _py_uuid

from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy import TableValuedColumn
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.postgresql import aggregate_order_by
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.dialects.postgresql import DATERANGE
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.dialects.postgresql import HSTORE
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import INT4RANGE
from sqlalchemy.dialects.postgresql import INT8MULTIRANGE
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import Range
from sqlalchemy.dialects.postgresql import TSTZMULTIRANGE
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.sqltypes import _JSON_VALUE


class Base(DeclarativeBase):
    pass


def test_uuid_column_types() -> None:
    """Test UUID column type inference with as_uuid parameter."""
    c1 = Column(UUID())
    assert_type(c1, Column[_py_uuid])

    c2 = Column(UUID(as_uuid=False))
    assert_type(c2, Column[str])


def test_range_column_types() -> None:
    """Test Range and MultiRange column type inference."""
    assert_type(Column(INT4RANGE()), Column[Range[int]])
    assert_type(Column("foo", DATERANGE()), Column[Range[date]])
    assert_type(Column(INT8MULTIRANGE()), Column[Sequence[Range[int]]])
    assert_type(
        Column("foo", TSTZMULTIRANGE()), Column[Sequence[Range[datetime]]]
    )


def test_range_in_select() -> None:
    """Test Range types in SELECT statements."""
    range_col_stmt = select(Column(INT4RANGE()), Column(INT8MULTIRANGE()))
    assert_type(range_col_stmt, Select[Range[int], Sequence[Range[int]]])


def test_array_type_inference() -> None:
    array_from_ints = array(range(2))
    assert_type(array_from_ints, array[int])

    array_of_strings = array([], type_=Text)
    assert_type(array_of_strings, array[str])

    array_of_ints = array([0], type_=Integer)
    assert_type(array_of_ints, array[int])

    # EXPECTED_MYPY_RE: Cannot infer .* of "array"
    array([0], type_=Text)


def test_array_column_types() -> None:
    """Test ARRAY column type inference."""
    assert_type(ARRAY(Text), ARRAY[str])
    assert_type(Column(type_=ARRAY(Integer)), Column[Sequence[int]])


def test_array_agg_functions() -> None:
    """Test array_agg function type inference."""
    stmt_array_agg = select(func.array_agg(Column("num", type_=Integer)))
    assert_type(stmt_array_agg, Select[Sequence[int]])


def test_array_agg_with_aggregate_order_by() -> None:
    """Test array_agg with aggregate_order_by."""

    class Test(Base):
        __tablename__ = "test_array_agg"
        id = mapped_column(Integer, primary_key=True)
        ident_str: Mapped[str] = mapped_column()
        ident: Mapped[_py_uuid] = mapped_column(UUID())

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


def test_json_parameterization() -> None:

    # test default type
    x: JSON = JSON()

    assert_type(x, JSON[_JSON_VALUE])

    # test column values

    s1 = select(Column(JSON()))

    assert_type(s1, Select[_JSON_VALUE])

    c1: Column[list[int]] = Column(JSON())
    s2 = select(c1)

    assert_type(s2, Select[list[int]])


def test_jsonb_parameterization() -> None:

    # test default type
    x: JSONB = JSONB()

    assert_type(x, JSONB[_JSON_VALUE])

    # test column values

    s1 = select(Column(JSONB()))

    assert_type(s1, Select[_JSON_VALUE])

    c1: Column[list[int]] = Column(JSONB())
    s2 = select(c1)

    assert_type(s2, Select[list[int]])


def test_json_column_types() -> None:
    """Test JSON column type inference with type parameters."""

    c_json: Column[Any] = Column(JSON())
    assert_type(c_json, Column[Any])

    c_json_dict = Column(JSON[Dict[str, Any]]())
    assert_type(c_json_dict, Column[Dict[str, Any]])


def test_json_orm_mapping() -> None:
    """Test JSON type in ORM mapped columns."""

    class JSONTest(Base):
        __tablename__ = "test_json"
        id = mapped_column(Integer, primary_key=True)
        json_data: Mapped[Dict[str, Any]] = mapped_column(JSON)
        json_list: Mapped[List[Any]] = mapped_column(JSON)

    json_obj = JSONTest()
    assert_type(json_obj.json_data, Dict[str, Any])
    assert_type(json_obj.json_list, List[Any])


def test_jsonb_column_types() -> None:
    """Test JSONB column type inference with type parameters."""
    c_jsonb: Column[Any] = Column(JSONB())
    assert_type(c_jsonb, Column[Any])

    c_jsonb_dict = Column(JSONB[Dict[str, Any]]())
    assert_type(c_jsonb_dict, Column[Dict[str, Any]])


def test_jsonb_orm_mapping() -> None:
    """Test JSONB type in ORM mapped columns."""

    class JSONBTest(Base):
        __tablename__ = "test_jsonb"
        id = mapped_column(Integer, primary_key=True)
        jsonb_data: Mapped[Dict[str, Any]] = mapped_column(JSONB)

    jsonb_obj = JSONBTest()
    assert_type(jsonb_obj.jsonb_data, Dict[str, Any])


def test_jsonb_func_with_type_param() -> None:
    """Test func with parameterized JSONB types (issue #13131)."""

    class JSONBTest(Base):
        __tablename__ = "test_jsonb_func"
        id = mapped_column(Integer, primary_key=True)
        data: Mapped[Dict[str, Any]] = mapped_column(JSONB)

    json_func_result = func.jsonb_path_query_array(
        JSONBTest.data, "$.items", type_=JSONB[List[Tuple[int, int]]]
    )
    assert_type(json_func_result, Function[List[Tuple[int, int]]])


def test_hstore_column_types() -> None:
    """Test HSTORE column type inference."""
    c_hstore: Column[dict[str, str | None]] = Column(HSTORE())
    assert_type(c_hstore, Column[dict[str, str | None]])


def test_hstore_orm_mapping() -> None:
    """Test HSTORE type in ORM mapped columns."""

    class HSTORETest(Base):
        __tablename__ = "test_hstore"
        id = mapped_column(Integer, primary_key=True)
        hstore_data: Mapped[dict[str, str | None]] = mapped_column(HSTORE)

    hstore_obj = HSTORETest()
    assert_type(hstore_obj.hstore_data, dict[str, str | None])


def test_hstore_func() -> None:

    my_func = func.foobar(type_=HSTORE)

    stmt = select(my_func)
    assert_type(stmt, Select[dict[str, str | None]])


def test_insert_on_conflict() -> None:
    """Test INSERT with ON CONFLICT clauses."""

    class Test(Base):
        __tablename__ = "test_dml"
        id = mapped_column(Integer, primary_key=True)
        data: Mapped[Dict[str, Any]] = mapped_column(JSONB)
        ident: Mapped[_py_uuid] = mapped_column(UUID())
        ident_str: Mapped[str] = mapped_column(UUID(as_uuid=False))
        __table_args__ = (ExcludeConstraint((Column("ident_str"), "=")),)

    unique = UniqueConstraint(name="my_constraint")
    insert(Test).on_conflict_do_nothing(
        "foo", [Test.id], Test.id > 0
    ).on_conflict_do_update(
        unique, ["foo"], Test.id > 0, {"id": 42, Test.ident: 99}, Test.id == 22
    ).excluded.foo.desc()

    s1 = insert(Test)
    s1.on_conflict_do_update(set_=s1.excluded)


def test_complex_jsonb_query() -> None:
    """Test complex query with JSONB array elements."""

    class Test(Base):
        __tablename__ = "test_complex"
        id = mapped_column(Integer, primary_key=True)
        data: Mapped[Dict[str, Any]] = mapped_column(JSONB)
        ident: Mapped[_py_uuid] = mapped_column(UUID())
        ident_str: Mapped[str] = mapped_column(UUID(as_uuid=False))

    elem = func.jsonb_array_elements(
        Test.data, type_=JSONB[list[str]]
    ).column_valued("elem")

    assert_type(elem, TableValuedColumn[list[str]])

    t1 = Test()
    assert_type(t1.data, dict[str, Any])
    assert_type(t1.ident, _py_uuid)
