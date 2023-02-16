from __future__ import annotations

from typing import Any

from sqlalchemy import ColumnElement
from sqlalchemy import func
from sqlalchemy.ext.hybrid import Comparator
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class CaseInsensitiveComparator(Comparator[str]):
    def __eq__(self, other: Any) -> ColumnElement[bool]:  # type: ignore[override]  # noqa: E501
        return func.lower(self.__clause_element__()) == func.lower(other)


class SearchWord(Base):
    __tablename__ = "searchword"

    id: Mapped[int] = mapped_column(primary_key=True)
    word: Mapped[str]

    @hybrid_property
    def word_insensitive(self) -> str:
        return self.word.lower()

    @word_insensitive.inplace.comparator
    @classmethod
    def _word_insensitive_comparator(cls) -> CaseInsensitiveComparator:
        return CaseInsensitiveComparator(cls.word)


class FirstNameOnly(Base):
    __tablename__ = "f"

    id: Mapped[int] = mapped_column(primary_key=True)
    first_name: Mapped[str]

    @hybrid_property
    def name(self) -> str:
        return self.first_name

    @name.inplace.setter
    def _name_setter(self, value: str) -> None:
        self.first_name = value


class FirstNameLastName(FirstNameOnly):

    last_name: Mapped[str]

    @FirstNameOnly.name.getter
    def name(self) -> str:
        return self.first_name + " " + self.last_name

    @name.inplace.setter
    def _name_setter(self, value: str) -> None:
        self.first_name, self.last_name = value.split(" ", 1)
