from sqlalchemy import update
from sqlalchemy import values

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DbBase(DeclarativeBase):
    pass


class MyTable(DbBase):
    __tablename__ = "my_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


update_values = values(
    MyTable.id,
    MyTable.name,
    name="update_values",
).data([(1, "Alice"), (2, "Bob")])

query = (
    update(MyTable)
    .values(
        {
            MyTable.name: update_values.c.name,
        }
    )
    .where(MyTable.id == update_values.c.id)
)
