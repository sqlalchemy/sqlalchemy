from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import registry

reg: registry = registry()


@reg.mapped
class User:
    __tablename__ = "user"

    id = Column(Integer(), primary_key=True)
    # EXPECTED: Left hand assignment 'name: "int"' not compatible with ORM mapped expression # noqa E501
    name: int = Column(String())
