from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy.orm import declarative_base, Mapped
from . import enum_col_import1
from .enum_col_import1 import IntEnum, StrEnum

Base = declarative_base()


class TestEnum(Base):
    __tablename__ = "test_enum"

    e1: Mapped[StrEnum] = Column(Enum(StrEnum))
    e2: StrEnum = Column(Enum(StrEnum))

    e3: Mapped[IntEnum] = Column(Enum(IntEnum))
    e4: IntEnum = Column(Enum(IntEnum))

    e5: Mapped[enum_col_import1.StrEnum] = Column(
        Enum(enum_col_import1.StrEnum)
    )
    e6: enum_col_import1.StrEnum = Column(Enum(enum_col_import1.StrEnum))

    e7: Mapped[enum_col_import1.IntEnum] = Column(
        Enum(enum_col_import1.IntEnum)
    )
    e8: enum_col_import1.IntEnum = Column(Enum(enum_col_import1.IntEnum))
