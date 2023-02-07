import typing
from typing import Dict
from typing import Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import attribute_keyed_dict
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


class Item(Base):
    __tablename__ = "item"

    id: Mapped[int] = mapped_column(primary_key=True)

    notes: Mapped[Dict[str, "Note"]] = relationship(
        collection_class=attribute_keyed_dict("keyword"),
        cascade="all, delete-orphan",
    )


class Note(Base):
    __tablename__ = "note"

    id: Mapped[int] = mapped_column(primary_key=True)
    item_id: Mapped[int] = mapped_column(ForeignKey("item.id"))
    keyword: Mapped[str]
    text: Mapped[Optional[str]]

    def __init__(self, keyword: str, text: str):
        self.keyword = keyword
        self.text = text


item = Item()
item.notes["a"] = Note("a", "atext")

if typing.TYPE_CHECKING:
    # EXPECTED_TYPE: dict_items[str, Note]
    reveal_type(item.notes.items())
