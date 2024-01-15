from __future__ import annotations

import re
from typing import Sequence
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey
from sqlalchemy.ext.orderinglist import ordering_list
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship


class Base(DeclarativeBase):
    pass


def text_to_pos(index: int, items: Sequence[Bullet]) -> int:
    match = re.search(r"(\d+)", items[index].text)
    return int(match[1]) if match else index


pos_from_text = ordering_list("position", ordering_func=text_to_pos)


class Slide(Base):
    __tablename__ = "slide"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

    bullets: Mapped[list[Bullet]] = relationship(
        "Bullet", order_by="Bullet.position", collection_class=pos_from_text
    )


class Bullet(Base):
    __tablename__ = "bullet"
    id: Mapped[int] = mapped_column(primary_key=True)
    slide_id: Mapped[int] = mapped_column(ForeignKey("slide.id"))
    position: Mapped[int]
    text: Mapped[str]


slide = Slide()


if TYPE_CHECKING:
    # EXPECTED_RE_TYPE: def \(\) -> sqlalchemy.*.orderinglist.OrderingList\[orderinglist_one.Bullet\]
    reveal_type(pos_from_text)

    # EXPECTED_TYPE: builtins.list[orderinglist_one.Bullet]
    reveal_type(slide.bullets)
