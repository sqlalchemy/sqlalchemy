from sqlalchemy import Integer
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = "test_table_json"

    id = mapped_column(Integer, primary_key=True)
    data: Mapped[str] = mapped_column()


insert(Test).on_duplicate_key_update(
    {"id": 42, Test.data: 99}, [("foo", 44)], data=99, id="foo"
).inserted.foo.desc()
