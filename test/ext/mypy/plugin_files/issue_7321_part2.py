from typing import Any
from typing import Dict
from typing import Type

from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import declared_attr


Base = declarative_base()


class Foo(Base):
    # no mypy error emitted regarding the
    # Type[Foo] part
    @declared_attr
    def __tablename__(cls: Type["Foo"]) -> str:
        return "name"

    @declared_attr
    def __mapper_args__(cls: Type["Foo"]) -> Dict[Any, Any]:
        return {}

    # this was a workaround that works if there's no plugin present, make
    # sure that doesn't crash anything
    @classmethod
    @declared_attr
    def __table_args__(cls: Type["Foo"]) -> Dict[Any, Any]:
        return {}
