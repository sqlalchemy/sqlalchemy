from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import select
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

mapper_registry: registry = registry()


@mapper_registry.mapped
@dataclass
class User:
    __table__ = Table(
        "user",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("fullname", String(50)),
        Column("nickname", String(12)),
    )
    id: int = field(init=False)
    name: Optional[str] = None
    fullname: Optional[str] = None
    nickname: Optional[str] = None
    addresses: List[Address] = field(default_factory=list)

    if TYPE_CHECKING:
        _mypy_mapped_attrs = [id, name, fullname, nickname, addresses]

    __mapper_args__ = {  # type: ignore
        "properties": {"addresses": relationship("Address")}
    }


@mapper_registry.mapped
@dataclass
class Address:
    __table__ = Table(
        "address",
        mapper_registry.metadata,
        Column("id", Integer, primary_key=True),
        Column("user_id", Integer, ForeignKey("user.id")),
        Column("email_address", String(50)),
    )

    id: int = field(init=False)
    user_id: int = field(init=False)
    email_address: Optional[str] = None

    if TYPE_CHECKING:
        _mypy_mapped_attrs = [id, user_id, email_address]


stmt = select(User.name).where(User.id.in_([1, 2, 3]))
stmt = select(Address).where(Address.email_address.contains(["foo"]))
