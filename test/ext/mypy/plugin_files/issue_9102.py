from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import registry


class BackendMeta:
    __abstract__ = True
    mapped_registry: registry = registry()
    metadata = mapped_registry.metadata


# this decorator is not picked up now, but at least it doesn't crash
@BackendMeta.mapped_registry.mapped
class User:
    __tablename__ = "user"

    # EXPECTED_MYPY: Incompatible types in assignment (expression has type "Column[int]", variable has type "int")
    id: int = Column(Integer(), primary_key=True)
