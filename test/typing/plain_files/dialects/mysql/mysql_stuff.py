from typing import Any
from typing import assert_type
from typing import Dict
from typing import List
from typing import Tuple

from sqlalchemy import Column
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.functions import Function


class Base(DeclarativeBase):
    pass


def test_insert_on_duplicate_key_update() -> None:
    """Test INSERT with ON DUPLICATE KEY UPDATE."""

    class Test(Base):
        __tablename__ = "test_table_json"
        id = mapped_column(Integer, primary_key=True)
        data: Mapped[str] = mapped_column()

    insert(Test).on_duplicate_key_update(
        {"id": 42, Test.data: 99}, [("foo", 44)], data=99, id="foo"
    ).inserted.foo.desc()


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


def test_json_func_with_type_param() -> None:
    """Test func with parameterized JSON types (issue #13131)."""

    class JSONTest(Base):
        __tablename__ = "test_json_func"
        id = mapped_column(Integer, primary_key=True)
        data: Mapped[Dict[str, Any]] = mapped_column(JSON)

    json_func_result = func.json_extract(
        JSONTest.data, "$.items", type_=JSON[List[Tuple[int, int]]]
    )
    assert_type(json_func_result, Function[List[Tuple[int, int]]])
