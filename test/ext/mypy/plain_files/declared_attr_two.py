import typing

from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import declared_attr
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class HasRelatedDataMixin:
    @declared_attr
    def related_data(cls) -> Mapped[str]:
        return mapped_column(Text(), deferred=True)


class User(HasRelatedDataMixin, Base):
    __tablename__ = "user"
    id = mapped_column(Integer, primary_key=True)


u1 = User()


u1.related_data


if typing.TYPE_CHECKING:

    # EXPECTED_TYPE: str
    reveal_type(u1.related_data)

    # EXPECTED_TYPE: InstrumentedAttribute[str]
    reveal_type(User.related_data)
