from typing import TYPE_CHECKING

from . import Base
from .user import HasUser

if TYPE_CHECKING:
    from .user import User  # noqa
    from sqlalchemy import Integer, Column  # noqa
    from sqlalchemy.orm import RelationshipProperty  # noqa


class Address(Base, HasUser):
    pass
