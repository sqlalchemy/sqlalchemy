from typing import assert_type

from sqlalchemy import Integer
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_as_dataclass
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import registry
from sqlalchemy.orm import unmapped_dataclass


@unmapped_dataclass(kw_only=True)
class DataModel:
    pass


@unmapped_dataclass(init=False, kw_only=True)
class RelationshipsModel(DataModel):
    __tablename__ = "relationships"

    entity_id1: Mapped[int] = mapped_column(primary_key=True)
    entity_id2: Mapped[int] = mapped_column(primary_key=True)


some_target_tables_registry = registry()


@mapped_as_dataclass(some_target_tables_registry)
class Relationships(RelationshipsModel):
    im_going_to_be_mapped = True
    level: Mapped[int] = mapped_column(Integer)


# note init=True is implicit on Relationships
# (this is the type checker, not us)
rs = Relationships(entity_id1=1, entity_id2=2, level=1)

assert_type(rs.entity_id1, int)

assert_type(rs.level, int)
