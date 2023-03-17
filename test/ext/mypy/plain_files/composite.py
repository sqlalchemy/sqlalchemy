from typing import Any
from typing import Tuple

from sqlalchemy import select
from sqlalchemy.orm import composite
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase):
    pass


class Point:
    def __init__(self, x: int, y: int):
        self.x = x
        self.y = y

    def __composite_values__(self) -> Tuple[int, int]:
        return self.x, self.y

    def __repr__(self) -> str:
        return "Point(x=%r, y=%r)" % (self.x, self.y)

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, Point)
            and other.x == self.x
            and other.y == self.y
        )

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    @classmethod
    def _generate(cls, x1: int, y1: int) -> "Point":
        return Point(x1, y1)


class Vertex(Base):
    __tablename__ = "vertices"

    id: Mapped[int] = mapped_column(primary_key=True)
    x1: Mapped[int]
    y1: Mapped[int]
    x2: Mapped[int]
    y2: Mapped[int]

    # inferred from right hand side
    start = composite(Point, "x1", "y1")

    # taken from left hand side
    end: Mapped[Point] = composite(Point._generate, "x2", "y2")


v1 = Vertex(start=Point(3, 4), end=Point(5, 6))

stmt = select(Vertex).where(Vertex.start.in_([Point(3, 4)]))

# EXPECTED_TYPE: Select[Tuple[Vertex]]
reveal_type(stmt)

# EXPECTED_TYPE: composite.Point
reveal_type(v1.start)

# EXPECTED_TYPE: composite.Point
reveal_type(v1.end)

# EXPECTED_TYPE: int
reveal_type(v1.end.y)
