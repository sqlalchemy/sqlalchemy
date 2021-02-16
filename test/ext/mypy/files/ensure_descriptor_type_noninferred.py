from typing import Optional

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import registry

reg: registry = registry()


@reg.mapped
class User:
    __tablename__ = "user"

    id = Column(Integer(), primary_key=True)
    name: Mapped[Optional[str]] = Column(String)


u1 = User()

# EXPECTED_MYPY: Incompatible types in assignment (expression has type "Optional[str]", variable has type "Optional[int]") # noqa E501
p: Optional[int] = u1.name
