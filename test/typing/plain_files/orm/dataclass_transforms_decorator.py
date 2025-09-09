from sqlalchemy import Integer
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_as_dataclass
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import registry

some_target_tables_registry = registry()


@mapped_as_dataclass(some_target_tables_registry)
class Relationships:
    __tablename__ = "relationships"

    entity_id1: Mapped[int] = mapped_column(primary_key=True)
    entity_id2: Mapped[int] = mapped_column(primary_key=True)
    level: Mapped[int] = mapped_column(Integer)


rs = Relationships(entity_id1=1, entity_id2=2, level=1)


# EXPECTED_TYPE: int
reveal_type(rs.entity_id1)
