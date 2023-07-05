from sqlalchemy import Integer
from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Test(Base):
    __tablename__ = "test_table_json"

    id = mapped_column(Integer, primary_key=True)
    data: Mapped[str] = mapped_column()


unique = UniqueConstraint(name="my_constraint")
insert(Test).on_conflict_do_nothing("foo", Test.id > 0).on_conflict_do_update(
    unique, Test.id > 0, {"id": 42, Test.data: 99}, Test.id == 22
).excluded.foo.desc()
