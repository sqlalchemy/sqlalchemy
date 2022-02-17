from typing import Any
from typing import Dict

from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr


Base = declarative_base()


class Foo(Base):
    @declared_attr
    def __tablename__(cls) -> str:
        return "name"

    @declared_attr
    def __mapper_args__(cls) -> Dict[Any, Any]:
        return {}

    @declared_attr
    def __table_args__(cls) -> Dict[Any, Any]:
        return {}
