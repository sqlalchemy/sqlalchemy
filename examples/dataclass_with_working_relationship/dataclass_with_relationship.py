from datetime import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
from sqlalchemy.orm.base import NEVER_SET


class Base(MappedAsDataclass, DeclarativeBase):
    __allow_unmapped__ = True

    def __post_init__(self, **kwargs: Any) -> None:
        for _name, infos in sa.inspect(self.__class__).relationships.items():
            if getattr(self, infos.key) == NEVER_SET:
                # we have to trick the instance here otherwise it will refuse
                # to delete the attribute
                setattr(self, infos.key, None)
                # removing the attribute in this case will avoid trashing
                # the foreign_key attribute
                delattr(self, infos.key)


class TimestampsMixin(MappedAsDataclass):
    created_at: Mapped[datetime] = mapped_column(
        default=None, server_default=sa.func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=None, server_default=sa.func.now(), onupdate=sa.func.now()
    )


class Company(Base, TimestampsMixin):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True, default=None
    )
    name: Mapped[str] = mapped_column(nullable=False, default=None)

    users: Mapped[list["User"]] = relationship(
        back_populates="company", default_factory=list
    )


class User(Base, TimestampsMixin):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True, default=None
    )

    name: Mapped[str] = mapped_column(nullable=False, default=None)
    email: Mapped[str] = mapped_column(nullable=False, default=None)

    company_id: Mapped[int] = mapped_column(
        ForeignKey("companies.id"), nullable=False, default=None
    )
    company: Mapped[Company] = relationship(
        back_populates="users", default=NEVER_SET
    )

    applications: Mapped[list["Application"]] = relationship(
        back_populates="owner", default_factory=list
    )


class Application(Base, TimestampsMixin):
    __tablename__ = "applications"

    id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True, default=None
    )
    name: Mapped[str] = mapped_column(nullable=False, default=None)

    owner_id: Mapped[int] = mapped_column(
        ForeignKey("users.id"), nullable=False, default=None
    )
    owner: Mapped[User] = relationship(
        back_populates="applications", default=None
    )


if __name__ == "__main__":
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    session = Session(engine)

    company = Company(name="Company")
    session.add(company)
    session.commit()

    user1 = User(
        name="User", email="user@example.com", company=company
    )  # works
    session.add(user1)
    session.commit()

    user2 = User(
        name="User", email="user2@example.com", company_id=company.id
    )  # works
    session.add(user2)
    session.commit()

    application1 = Application(name="Application", owner=user1)  # works
    session.add(application1)
    session.commit()

    try:
        application2 = Application(
            name="Application2", owner_id=user2.id
        )  # does not work
        session.add(application2)
        session.commit()
    except sa.exc.IntegrityError as e:
        print(e)
