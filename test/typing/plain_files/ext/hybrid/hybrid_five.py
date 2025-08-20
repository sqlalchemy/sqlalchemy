from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.elements import SQLCoreOperations


class Base(DeclarativeBase):
    pass


class MyModel(Base):
    __tablename__ = "my_model"

    id: Mapped[int] = mapped_column(primary_key=True)

    int_col: Mapped[int | None] = mapped_column()

    @hybrid_property
    def some_col(self) -> int:
        return (self.int_col or 0) + 1

    @some_col.inplace.setter
    def _str_col_setter(self, value: int | SQLCoreOperations[int]) -> None:
        self.int_col = value - 1


m = MyModel(id=42, int_col=1)
m.some_col = 42
m.some_col = select(func.max(MyModel.id)).scalar_subquery()
m.some_col = func.max(MyModel.id)
