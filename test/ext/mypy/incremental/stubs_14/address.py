from typing import TYPE_CHECKING

from . import Base
from .user import HasUser

if TYPE_CHECKING:
    from sqlalchemy import Column  # noqa
    from sqlalchemy import Integer  # noqa
    from sqlalchemy.orm import RelationshipProperty  # noqa
    from .user import User  # noqa


class Address(Base, HasUser):
    pass
