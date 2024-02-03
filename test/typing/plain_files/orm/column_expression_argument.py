from sqlalchemy import ColumnElement
from sqlalchemy import ColumnExpressionArgument
from sqlalchemy import true
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped


class Base(DeclarativeBase):
    ...


class HasPrivate(Base):
    public: Mapped[bool]


def where_criteria(cls_: type[HasPrivate]) -> ColumnElement[bool]:
    return cls_.public == true()


column_expression: ColumnExpressionArgument[bool] = where_criteria
column_expression_lambda: ColumnExpressionArgument[bool] = (
    lambda cls_: cls_.public == true()
)
