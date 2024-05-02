from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from typing import Any
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
    name: str | None = None
    fullname: str | None = None
    nickname: str | None = None
    addresses: list[Address] = field(default_factory=list)

    if TYPE_CHECKING:
        _mypy_mapped_attrs = [id, name, fullname, nickname, addresses]

    __mapper_args__: dict[str, Any] = {
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
    email_address: str | None = None

    if TYPE_CHECKING:
        _mypy_mapped_attrs = [id, user_id, email_address]


stmt1 = select(User.name).where(User.id.in_([1, 2, 3]))
stmt2 = select(Address).where(Address.email_address.contains(["foo"]))
