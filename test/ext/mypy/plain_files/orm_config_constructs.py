from sqlalchemy import String
from sqlalchemy.orm import column_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import deferred
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import query_expression
from sqlalchemy.orm import validates


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "User"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    @validates("name", include_removes=True)
    def validate_name(self, name: str) -> str:
        """test #8577"""
        return name + "hi"

    # test #9536
    _password: Mapped[str] = mapped_column("Password", String)
    password1: Mapped[str] = column_property(
        _password.collate("SQL_Latin1_General_CP1_CS_AS"), deferred=True
    )
    password2: Mapped[str] = deferred(
        _password.collate("SQL_Latin1_General_CP1_CS_AS")
    )
    password3: Mapped[str] = query_expression(
        _password.collate("SQL_Latin1_General_CP1_CS_AS")
    )
